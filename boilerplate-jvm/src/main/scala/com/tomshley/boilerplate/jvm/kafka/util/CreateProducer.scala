package com.tomshley.boilerplate.jvm.kafka.util

import org.apache.pekko.actor.CoordinatedShutdown
import org.apache.pekko.actor.typed.ActorSystem
import org.apache.pekko.kafka.ProducerSettings
import org.apache.pekko.kafka.scaladsl.SendProducer

import scala.annotation.tailrec
import scala.concurrent.Promise

/** Singleton SendProducer lifecycle trait.
 *
 * Manages a single `SendProducer` instance per concrete object, created
 * lazily on first `init` call. The producer is registered for graceful
 * shutdown via `CoordinatedShutdown`.
 */
protected[util] trait CreateProducer[K, V]:
  private val producerInstance: Promise[SendProducer[K, V]] = Promise()

  private def producerInstanceMaybe = producerInstance.future.value.flatMap(_.toOption)

  @tailrec
  final def init(system: ActorSystem[?]): SendProducer[K, V] =
    producerInstanceMaybe match
      case Some(value) => value
      case None =>
        producerInstance.trySuccess(createProducer(system))
        init(system)

  def producerSettings(system: ActorSystem[?]): ProducerSettings[K, V]

  private def createProducer(system: ActorSystem[?]): SendProducer[K, V] =
    val settings = producerSettings(system)
    val sendProducer = SendProducer(settings)(system)
    val taskName = s"close-sendProducer-${getClass.getSimpleName}-${System.identityHashCode(this)}"
    CoordinatedShutdown(system).addTask(CoordinatedShutdown.PhaseBeforeActorSystemTerminate, taskName) {
      () => sendProducer.close()
    }
    sendProducer
