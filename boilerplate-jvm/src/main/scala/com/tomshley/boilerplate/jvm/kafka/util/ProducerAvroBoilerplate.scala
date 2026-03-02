package com.tomshley.boilerplate.jvm.kafka.util

import io.confluent.kafka.serializers.*
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.common.serialization.{Serializer, StringSerializer}
import org.apache.pekko.actor.typed.ActorSystem
import org.apache.pekko.kafka.ProducerSettings

import scala.jdk.CollectionConverters.*

object ProducerAvroBoilerplate extends CreateProducer[String, GenericRecord] {
  override def producerSettings(system: ActorSystem[?]): ProducerSettings[String, GenericRecord] = {
    val schemaRegistryUrl = system.settings.config.getString("schema-registry.url")

    val kafkaAvroSerDeConfig = Map[String, Any](
      AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG -> schemaRegistryUrl)

    val kafkaAvroSerializer = new KafkaAvroSerializer()
    kafkaAvroSerializer.configure(kafkaAvroSerDeConfig.asJava, false)

    // Typed wrapper — Kafka does not call configure() on pre-instantiated serializers,
    // so the manual configure() above is the only effective configuration.
    val serializer: Serializer[GenericRecord] = new Serializer[GenericRecord] {
      override def serialize(topic: String, data: GenericRecord): Array[Byte] =
        kafkaAvroSerializer.serialize(topic, data)
      override def close(): Unit = kafkaAvroSerializer.close()
    }

    ProducerSettings(system, new StringSerializer, serializer)
  }
}
