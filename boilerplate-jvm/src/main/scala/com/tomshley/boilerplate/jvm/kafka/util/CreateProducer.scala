package com.tomshley.boilerplate.jvm.kafka.util

import org.apache.pekko.actor.CoordinatedShutdown
import org.apache.pekko.actor.typed.ActorSystem
import org.apache.pekko.kafka.ProducerSettings
import org.apache.pekko.kafka.scaladsl.SendProducer

import scala.annotation.tailrec
import scala.concurrent.Promise

protected[util] trait CreateProducer[K, V] {
  private val producerInstance: Promise[SendProducer[K, V]] = Promise()

  private def producerInstanceMaybe = producerInstance.future.value.flatMap(_.toOption)

  @tailrec
  final def init(system: ActorSystem[?]): SendProducer[K, V] = {
    producerInstanceMaybe match
      case Some(value) => value
      case None =>
        producerInstance.trySuccess{
          createProducer(system)
        }
        init(system)
  }

  def producerSettings(system: ActorSystem[?]): ProducerSettings[K, V]

  private def createProducer(system: ActorSystem[?]): SendProducer[K, V] = {
    given ps: ProducerSettings[K, V] = producerSettings(system)
    val sendProducer = SendProducer(ps)(system)
    CoordinatedShutdown(system).addTask(CoordinatedShutdown.PhaseBeforeActorSystemTerminate, s"close-sendProducer-${getClass.getSimpleName}-${System.identityHashCode(this)}") { () => sendProducer.close() }
    sendProducer
  }
}
