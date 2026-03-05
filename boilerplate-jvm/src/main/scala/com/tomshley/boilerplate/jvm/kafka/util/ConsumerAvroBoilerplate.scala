package com.tomshley.boilerplate.jvm.kafka.util

import io.confluent.kafka.serializers.*
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.common.serialization.{Deserializer, StringDeserializer}
import org.apache.pekko.actor.typed.ActorSystem
import org.apache.pekko.kafka.ConsumerSettings

import scala.jdk.CollectionConverters.*

/** Avro consumer settings factory.
 *
 * Produces ConsumerSettings[String, GenericRecord] configured with
 * a Schema Registry-backed deserializer. The application wires these
 * settings into Pekko Kafka source operators (e.g. Consumer.committableSource)
 * to create the actual reactive stream. Deserialized records are wrapped
 * via KafkaKeyAvroConsumerEnvelope.from().
 */
object ConsumerAvroBoilerplate extends CreateConsumer[String, GenericRecord]:
  override def consumerSettings(system: ActorSystem[?]): ConsumerSettings[String, GenericRecord] =
    consumerSettings(system, system.settings.config.getString("schema-registry.url"))

  def consumerSettings(system: ActorSystem[?], schemaRegistryUrl: String): ConsumerSettings[String, GenericRecord] =
    val kafkaAvroSerDeConfig = Map[String, Any](
      AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG -> schemaRegistryUrl,
      KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG -> "false")

    val kafkaAvroDeserializer = new KafkaAvroDeserializer()
    kafkaAvroDeserializer.configure(kafkaAvroSerDeConfig.asJava, false)

    // Typed wrapper — KafkaAvroDeserializer implements Deserializer[Object],
    // so the manual configure() above is the only effective configuration.
    // Note: deserialize() returns null for null/empty data (tombstone records).
    // The null GenericRecord propagates to ConsumerRecord.value(), where
    // KafkaKeyAvroConsumerEnvelope.from() catches it via the tombstone guard.
    val deserializer: Deserializer[GenericRecord] = new Deserializer[GenericRecord]:
      override def deserialize(topic: String, data: Array[Byte]): GenericRecord =
        kafkaAvroDeserializer.deserialize(topic, data).asInstanceOf[GenericRecord]
      override def close(): Unit = kafkaAvroDeserializer.close()

    ConsumerSettings(system, new StringDeserializer, deserializer)
