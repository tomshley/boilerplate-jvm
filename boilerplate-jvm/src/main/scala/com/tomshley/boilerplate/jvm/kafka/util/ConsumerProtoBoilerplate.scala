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
