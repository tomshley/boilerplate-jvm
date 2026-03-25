package com.tomshley.boilerplate.jvm.kafka.util

import org.apache.avro.generic.GenericRecord
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.pekko.actor.typed.ActorSystem
import org.apache.pekko.kafka.ProducerSettings

/** Avro producer settings with Confluent Schema Registry wire format.
 *
 * {{{
 * MarshallModel[T]  ──  AvroMarshaller.toRecord (avro4s)  ──▶  GenericRecord
 *                                                                    │
 *                          KafkaAvroSerializer (Confluent wire format)│
 *                                                                    ▼
 *                                                                  Kafka
 * }}}
 *
 * avro4s handles case class ↔ GenericRecord (via KafkaKeyAvroMessageEnvelope).
 * This object wires GenericRecord ↔ wire bytes (via SchemaRegistrySerde).
 * The Confluent serializer prepends `[0x00][schema-id]` to each message
 * and registers schemas with the Schema Registry on first encounter.
 */
object ProducerAvroBoilerplate extends CreateProducer[String, GenericRecord]:

  override def producerSettings(system: ActorSystem[?]): ProducerSettings[String, GenericRecord] =
    producerSettings(system, SchemaRegistryConfig.fromConfig(system.settings.config))

  def producerSettings(system: ActorSystem[?], schemaRegistryUrl: String): ProducerSettings[String, GenericRecord] =
    producerSettings(system, SchemaRegistryConfig(schemaRegistryUrl))

  def producerSettings(
      system: ActorSystem[?],
      config: SchemaRegistryConfig
  ): ProducerSettings[String, GenericRecord] =
    ProducerSettings(system, new StringSerializer, SchemaRegistrySerde.serializer(config))
