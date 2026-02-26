package com.tomshley.boilerplate.jvm.kafka.util

import io.confluent.kafka.serializers.*
import org.apache.avro.specific.SpecificRecord
import org.apache.kafka.common.serialization.{Serializer, StringSerializer}
import org.apache.pekko.actor.typed.ActorSystem
import org.apache.pekko.kafka.ProducerSettings

import scala.jdk.CollectionConverters.*

object ProducerAvroBoilerplate extends CreateProducer[String, SpecificRecord] {
  override def producerSettings(system: ActorSystem[?]): ProducerSettings[String, SpecificRecord] = {
    val schemaRegistryUrl = system.settings.config.getString("schema-registry.url")

    val kafkaAvroSerDeConfig = Map[String, Any](
      AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG -> schemaRegistryUrl,
      KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG -> true.toString)

    val kafkaAvroSerializer = new KafkaAvroSerializer()
    kafkaAvroSerializer.configure(kafkaAvroSerDeConfig.asJava, false)

    val serializer: Serializer[SpecificRecord] = new Serializer[SpecificRecord] {
      override def configure(configs: java.util.Map[String, ?], isKey: Boolean): Unit =
        kafkaAvroSerializer.configure(configs, isKey)
      override def serialize(topic: String, data: SpecificRecord): Array[Byte] =
        kafkaAvroSerializer.serialize(topic, data)
      override def close(): Unit = kafkaAvroSerializer.close()
    }

    ProducerSettings(system, new StringSerializer, serializer)
  }
}
