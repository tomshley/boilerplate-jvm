package com.tomshley.boilerplate.jvm.kafka.util

import org.apache.kafka.common.serialization.{ByteArrayDeserializer, StringDeserializer}
import org.apache.pekko.actor.typed.ActorSystem
import org.apache.pekko.kafka.ConsumerSettings

/** Protobuf consumer settings factory.
 *
 * Produces ConsumerSettings[String, Array[Byte]] with raw byte
 * deserialization. The application wires these settings into Pekko Kafka
 * source operators to create the reactive stream. Consumed records are
 * wrapped via KafkaKeyProtoConsumerEnvelope.from(), which parses the
 * bytes as a ScalaPB Any on first access (lazy val packed).
 */
object ConsumerProtoBoilerplate extends CreateConsumer[String, Array[Byte]]:
  override def consumerSettings(system: ActorSystem[?]): ConsumerSettings[String, Array[Byte]] =
    ConsumerSettings(system, new StringDeserializer, new ByteArrayDeserializer)
