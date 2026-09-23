package com.tomshley.boilerplate.jvm.kafka.internal

import org.apache.pekko.Done
import org.apache.pekko.actor.{CoordinatedShutdown, NoSerializationVerificationNeeded}
import org.apache.pekko.actor.typed.{ActorRef, ActorSystem, Behavior, Extension, Scheduler}
import org.apache.pekko.actor.typed.scaladsl.{AskPattern, Behaviors}
import org.apache.pekko.kafka.scaladsl.SendProducer
import org.apache.pekko.pattern.StatusReply
import org.apache.pekko.util.Timeout

import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration.DurationLong
import scala.util.{Failure, Success}

private[kafka] final class ManagedProducer[K, V] private (
    owner: ActorRef[ManagedProducer.Command[K, V]],
    shutdown: CoordinatedShutdown,
    timeout: Timeout,
    scheduler: Scheduler
) extends Extension:
  import AskPattern.*
  import ManagedProducer.*

  private given Timeout = timeout
  private given Scheduler = scheduler
  private given ExecutionContext = ExecutionContext.parasitic

  def acquire(): Future[SendProducer[K, V]] =
    shutdown.shutdownReason() match
      case Some(_) => Future.failed(new IllegalStateException("ActorSystem is shutting down"))
      case None =>
        owner.ask[Acquired[K, V]](Acquire(_)).flatMap { response =>
          if (response.result.isSuccess) response.result.getValue
          else Future.failed(response.result.getError)
        }

  private def close(): Future[Done] =
    owner.ask[Closed](Close(_)).flatMap(_.result)

private[kafka] object ManagedProducer:
  private sealed trait Command[K, V] extends NoSerializationVerificationNeeded
  private final case class Acquire[K, V](replyTo: ActorRef[Acquired[K, V]]) extends Command[K, V]
  private final case class Close[K, V](replyTo: ActorRef[Closed]) extends Command[K, V]
  private final case class Acquired[K, V](result: StatusReply[Future[SendProducer[K, V]]])
      extends NoSerializationVerificationNeeded
  private final case class Closed(result: Future[Done]) extends NoSerializationVerificationNeeded

  def start[K, V](system: ActorSystem[?], construct: () => Future[SendProducer[K, V]]): ManagedProducer[K, V] =
    val duration = system.settings.config.getDuration("tomshley-boilerplate-kafka.producer.ask-timeout").toNanos.nanos
    require(duration.length > 0, "Producer actor ask timeout must be positive")
    val shutdown = CoordinatedShutdown(system)
    val owner = system.systemActorOf(running(construct, None), s"kafka-producer-${UUID.randomUUID()}")
    val managed = new ManagedProducer(owner, shutdown, Timeout(duration), system.scheduler)
    shutdown.addTask(CoordinatedShutdown.PhaseBeforeActorSystemTerminate, s"close-${owner.path.name}")(
      () => managed.close()
    )
    managed

  private def running[K, V](
      construct: () => Future[SendProducer[K, V]],
      current: Option[Future[SendProducer[K, V]]]
  ): Behavior[Command[K, V]] =
    Behaviors.receiveMessage {
      case Acquire(replyTo) =>
        val producer = current.filterNot(_.value.exists(_.isFailure)).getOrElse(construct())
        replyTo ! Acquired(StatusReply.Success(producer))
        running(construct, Some(producer))
      case Close(replyTo) =>
        val closed = current.fold(Future.successful(Done))(_.transformWith {
          case Success(producer) => producer.close()
          case Failure(_) => Future.successful(Done)
        }(using ExecutionContext.parasitic))
        replyTo ! Closed(closed)
        closing(closed)
    }

  private def closing[K, V](closed: Future[Done]): Behavior[Command[K, V]] =
    Behaviors.receiveMessage {
      case Acquire(replyTo) =>
        replyTo ! Acquired(StatusReply.Error("Kafka producer is shutting down"))
        Behaviors.same
      case Close(replyTo) =>
        replyTo ! Closed(closed)
        Behaviors.same
    }
