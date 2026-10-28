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

import com.google.protobuf.any.Any as ScalaPBAny
import org.apache.kafka.common.header.internals.RecordHeaders
import org.apache.pekko.persistence.query.typed.EventEnvelope as PersistenceQueryEventEnvelope
import org.apache.pekko.persistence.typed.PersistenceId
import org.apache.pekko.projection.eventsourced.EventEnvelope as EventSourcedEventEnvelope
import scalapb.GeneratedMessage

@deprecated
val KafkaKeyMessageEnvelope = KafkaKeyProtoMessageEnvelope

final case class KafkaKeyProtoMessageEnvelope(
    serviceName: String,
    key: String,
    pbValue: GeneratedMessage,
    headers: RecordHeaders = new RecordHeaders()
):
  lazy val messageBytes: Array[Byte] = ScalaPBAny.pack(pbValue, serviceName).toByteArray

object KafkaKeyProtoMessageEnvelope:
  def apply(
      serviceName: String,
      eventsourcedEnvelope: EventSourcedEventEnvelope[?],
      pbValue: GeneratedMessage
  ): KafkaKeyProtoMessageEnvelope =
    new KafkaKeyProtoMessageEnvelope(
      serviceName,
      PersistenceId.extractEntityId(eventsourcedEnvelope.persistenceId),
      pbValue
    )

  def apply(
      serviceName: String,
      persistenceQueryEnvelope: PersistenceQueryEventEnvelope[?],
      pbValue: GeneratedMessage
  ): KafkaKeyProtoMessageEnvelope =
    new KafkaKeyProtoMessageEnvelope(
      serviceName,
      PersistenceId.extractEntityId(persistenceQueryEnvelope.persistenceId),
      pbValue
    )