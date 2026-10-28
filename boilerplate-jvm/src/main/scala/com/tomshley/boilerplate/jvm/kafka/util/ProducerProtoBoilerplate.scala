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