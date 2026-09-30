package com.tomshley.boilerplate.jvm.managedmain

import org.apache.pekko.Done
import org.apache.pekko.actor.CoordinatedShutdown
import org.apache.pekko.actor.typed.ActorSystem

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

final case class StartupTask[A](
    name: String,
    acquire: ActorSystem[?] => Future[A],
    release: (ActorSystem[?], A) => Future[Done]
) {
  require(name.trim.nonEmpty, "Startup task name must not be empty")

  private[managedmain] def prepare(system: ActorSystem[?]): () => Future[Done] = {
    given ExecutionContext = ExecutionContext.parasitic
    val shutdown = CoordinatedShutdown(system)
    lazy val acquired: Future[A] = shutdown.shutdownReason() match {
      case Some(_) => Future.failed(new IllegalStateException("ActorSystem is shutting down"))
      case None => Future.fromTry(Try(acquire(system))).flatten
    }
    shutdown.addTask(CoordinatedShutdown.PhaseBeforeActorSystemTerminate, s"managed-$name") { () =>
      acquired.transformWith {
        case Success(resource) => Future.fromTry(Try(release(system, resource))).flatten
        case Failure(_) => Future.successful(Done)
      }
    }
    () => acquired.map(_ => Done)
  }
}
