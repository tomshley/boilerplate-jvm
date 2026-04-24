package com.tomshley.boilerplate.jvm.kafka.util

import io.confluent.kafka.serializers.{KafkaAvroDeserializer, KafkaAvroDeserializerConfig, KafkaAvroSerializer}
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.common.serialization.{Deserializer, Serializer}

import scala.jdk.CollectionConverters.*

/** Confluent Schema Registry serde factory.
 *
 * Creates Kafka Serializer/Deserializer instances configured with
 * Schema Registry credentials. These handle the Confluent wire format
 * ({{{[0x00][4-byte schema-id][avro-binary]}}}), distinct from the avro4s
 * marshalling layer which handles case class ↔ GenericRecord.
 *
 * The serializer inherits `auto.register.schemas` and `use.latest.version`
 * from the provided [[SchemaRegistryConfig]]. Defaults (off / on) enforce
 * the schemas-as-code contract: the registry is the source of truth and
 * producers encode against its latest version. See
 * [[SchemaRegistryConfig]] for rationale and override paths.
 */
object SchemaRegistrySerde:

  def serializer(config: SchemaRegistryConfig): Serializer[GenericRecord] =
    val s = new KafkaAvroSerializer()
    s.configure(config.toSerializerConfig.asJava, false)
    s.asInstanceOf[Serializer[GenericRecord]]

  def deserializer(config: SchemaRegistryConfig): Deserializer[GenericRecord] =
    val d = new KafkaAvroDeserializer()
    // toClientConfig (URL + auth only): KafkaAvroDeserializer does not
    // consume the serializer-side `auto.register.schemas` / `use.latest.version`
    // gate flags; passing them here would be dead weight and a forward-compat
    // liability if future Confluent versions begin rejecting unknown keys.
    d.configure(
      (config.toClientConfig + (KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG -> "false")).asJava,
      false
    )
    // KafkaAvroDeserializer returns Object; typed wrapper avoids asInstanceOf at every call site
    new Deserializer[GenericRecord]:
      override def deserialize(topic: String, data: Array[Byte]): GenericRecord =
        d.deserialize(topic, data).asInstanceOf[GenericRecord]
      override def close(): Unit = d.close()
