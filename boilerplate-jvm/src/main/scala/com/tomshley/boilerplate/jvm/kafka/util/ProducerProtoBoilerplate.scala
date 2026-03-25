package com.tomshley.boilerplate.jvm.kafka.util

import org.apache.kafka.common.serialization.{ByteArraySerializer, StringSerializer}
import org.apache.pekko.actor.typed.ActorSystem
import org.apache.pekko.kafka.ProducerSettings

/** Protobuf producer settings factory.
 *
 * Produces ProducerSettings[String, Array[Byte]] with raw byte
 * serialization. The application packs messages via
 * KafkaKeyProtoMessageEnvelope.messageBytes before sending.
 */
object ProducerProtoBoilerplate extends CreateProducer[String, Array[Byte]]:
  override def producerSettings(system: ActorSystem[?]): ProducerSettings[String, Array[Byte]] =
    ProducerSettings(system, new StringSerializer, new ByteArraySerializer)