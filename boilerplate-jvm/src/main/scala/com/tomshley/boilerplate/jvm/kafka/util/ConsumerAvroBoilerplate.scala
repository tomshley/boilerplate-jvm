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
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.pekko.actor.typed.ActorSystem
import org.apache.pekko.kafka.ConsumerSettings

/** Avro consumer settings with Confluent Schema Registry wire format.
 *
 * {{{
 *                                                                  Kafka
 *                                                                    │
 *                       KafkaAvroDeserializer (strips wire format)   │
 *                                                                    ▼
 * MarshallModel[T]  ◀──  AvroMarshaller.fromRecord (avro4s)  ──  GenericRecord
 *                         (via KafkaKeyAvroConsumerEnvelope.as[T];
 *                          use .asResolving[T] to additionally resolve aliased
 *                          field renames against the reader schema)
 * }}}
 *
 * This object wires wire bytes → GenericRecord (via SchemaRegistrySerde).
 * avro4s handles GenericRecord → case class (via KafkaKeyAvroConsumerEnvelope).
 * The application wires these settings into Pekko Kafka source operators
 * (e.g. Consumer.committableSource) to create the reactive stream.
 */
object ConsumerAvroBoilerplate extends CreateConsumer[String, GenericRecord]:

  override def consumerSettings(system: ActorSystem[?]): ConsumerSettings[String, GenericRecord] =
    consumerSettings(system, SchemaRegistryConfig.fromConfig(system.settings.config))

  def consumerSettings(system: ActorSystem[?], schemaRegistryUrl: String): ConsumerSettings[String, GenericRecord] =
    consumerSettings(system, SchemaRegistryConfig(schemaRegistryUrl))

  def consumerSettings(
      system: ActorSystem[?],
      config: SchemaRegistryConfig
  ): ConsumerSettings[String, GenericRecord] =
    ConsumerSettings(system, new StringDeserializer, SchemaRegistrySerde.deserializer(config))
