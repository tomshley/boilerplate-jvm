package com.tomshley.boilerplate.jvm.kafka.util

import org.apache.kafka.clients.producer.MockProducer
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.pekko.actor.NoSerializationVerificationNeeded
import org.apache.pekko.actor.typed.{ActorRef, ActorSystem}
import org.apache.pekko.kafka.ProducerSettings

private[jvm] object KafkaTestSupport {

  enum ProducerEvent extends NoSerializationVerificationNeeded:
    case Flushed(producer: RecordingProducer)
    case Closed(producer: RecordingProducer)

  final class TestProducerFactory(settingsFor: ActorSystem[?] => ProducerSettings[String, String])
      extends CreateProducer[String, String] {
    override def producerSettings(system: ActorSystem[?]): ProducerSettings[String, String] =
      settingsFor(system)
  }

  final class RecordingProducer(
      events: ActorRef[ProducerEvent],
      failClose: Option[Throwable] = None
  ) extends MockProducer[String, String](true, new StringSerializer(), new StringSerializer()) {

    def closeUnderlying(): Unit = super.close(java.time.Duration.ZERO)

    override def flush(): Unit = {
      events ! ProducerEvent.Flushed(this)
      super.flush()
    }

    override def close(timeout: java.time.Duration): Unit = {
      events ! ProducerEvent.Closed(this)
      failClose match {
        case Some(cause) => throw cause
        case None        => super.close(timeout)
      }
    }
  }

  def producerSettings(
      system: ActorSystem[?],
      factory: ProducerSettings[String, String] => org.apache.kafka.clients.producer.Producer[String, String]
  ): ProducerSettings[String, String] =
    ProducerSettings(system, new StringSerializer(), new StringSerializer())
      .withBootstrapServers("127.0.0.1:1")
      .withProducerFactory(factory)
}
