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
import com.tomshley.boilerplate.jvm.kafka.exceptions.KafkaTombstoneException
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.header.internals.RecordHeaders
import scalapb.{GeneratedMessage, GeneratedMessageCompanion}

/** Protobuf consumer envelope for Kafka consumption.
 *
 * Wraps a consumed record's key, defensively-copied bytes, and headers.
 * The bytes are lazily parsed as a ScalaPB Any on first access to `packed`.
 * Use `as[T](companion)` to extract a typed message.
 *
 * Constructed via the `from(ConsumerRecord)` factory, which rejects
 * null keys (IllegalArgumentException) and tombstone records
 * (KafkaTombstoneException).
 *
 * Note: as a case class with an Array[Byte] field, auto-generated
 * equals/hashCode use reference identity for bytes, not content
 * equality. This is acceptable for a transient data carrier — do not
 * use envelopes as Map keys or Set members.
 */
final case class KafkaKeyProtoConsumerEnvelope(key: String, bytes: Array[Byte], headers: RecordHeaders = new RecordHeaders()):
  lazy val packed: ScalaPBAny = ScalaPBAny.parseFrom(bytes)
  def typeUrl: String = packed.typeUrl
  /** Typed extraction — requires the ScalaPB companion object. */
  def as[T <: GeneratedMessage](companion: GeneratedMessageCompanion[T]): T =
    companion.parseFrom(packed.value.newCodedInput())

object KafkaKeyProtoConsumerEnvelope:
  def from(record: ConsumerRecord[String, Array[Byte]]): KafkaKeyProtoConsumerEnvelope =
    require(record.key() != null, s"Null-keyed records are not supported (partition=${record.partition()}, offset=${record.offset()})")
    if (record.value() == null) throw KafkaTombstoneException(record.key(), record.partition(), record.offset())
    KafkaKeyProtoConsumerEnvelope(record.key(), record.value().clone(), new RecordHeaders(record.headers()))
