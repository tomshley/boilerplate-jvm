/*
 * copyright 2017–present tomshley llc
 *
 * licensed under the apache license, version 2.0 (the "license");
 * you may not use this file except in compliance with the license.
 * you may obtain a copy of the license at
 *
 * http://www.apache.org/licenses/license-2.0
 *
 * unless required by applicable law or agreed to in writing, software
 * distributed under the license is distributed on an "as is" basis,
 * without warranties or conditions of any kind, either express or implied.
 * see the license for the specific language governing permissions and
 * limitations under the license.
 *
 * @author thomas schena @sgoggles <https://github.com/sgoggles> | <https://gitlab.com/sgoggles>
 *
 */

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
