/*
 * copyright 2023 tomshley llc
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

package com.tomshley.boilerplate.jvm.marshalling

import com.sksamuel.avro4s.{Decoder, Encoder, FromRecord, SchemaFor, ToRecord}
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericDatumReader, GenericDatumWriter, GenericRecord}
import org.apache.avro.io.{DecoderFactory, EncoderFactory}

import models.MarshallModel

import java.io.ByteArrayOutputStream
import scala.concurrent.{ExecutionContext, Future}

/** Avro serialization for [[MarshallModel]] instances via avro4s.
 *
 *  [[com.sksamuel.avro4s.Encoder]], [[com.sksamuel.avro4s.Decoder]], and
 *  [[com.sksamuel.avro4s.SchemaFor]] are auto-derived by Magnolia at each
 *  call site — no explicit givens needed for case classes.
 *
 *  For enum-like fields, use sealed trait + case objects (auto-derived by
 *  avro4s Magnolia as Avro ENUM). Do NOT define explicit givens in the
 *  companion — Magnolia handles derivation automatically:
 *  {{{
 *  sealed trait DeviceStatus
 *  case object ONLINE extends DeviceStatus
 *  case object OFFLINE extends DeviceStatus
 *
 *  final case class DeviceEvent(id: String, status: DeviceStatus)
 *      extends MarshallModel[DeviceEvent]
 *
 *  val record = AvroMarshaller.toRecord(DeviceEvent("d1", ONLINE))
 *  val back   = AvroMarshaller.fromRecord[DeviceEvent](record)
 *  }}}
 */
trait AvroMarshaller {

  /** toRecord
   *
   * @param model T
   * @tparam T T <: MarshallModel[ T ]
   * @return GenericRecord
   */
  final def toRecord[T <: MarshallModel[T]](model: T)(using enc: Encoder[T], sf: SchemaFor[T]): GenericRecord =
    ToRecord[T](sf.schema).to(model)

  /** fromRecord
   *
   * @param record GenericRecord
   * @tparam T T <: MarshallModel[ T ]
   * @return T
   */
  final def fromRecord[T <: MarshallModel[T]](record: GenericRecord)(using dec: Decoder[T], sf: SchemaFor[T]): T =
    FromRecord[T](sf.schema).from(record)

  /** schema
   *
   * @tparam T T <: MarshallModel[ T ]
   * @return Schema
   */
  final def schema[T <: MarshallModel[T]](using sf: SchemaFor[T]): Schema =
    sf.schema

  /** toRecordAsync
   *
   * @param model T
   * @param ec ExecutionContext
   * @tparam T T <: MarshallModel[ T ]
   * @return Future[GenericRecord]
   */
  final def toRecordAsync[T <: MarshallModel[T]](model: T, ec: ExecutionContext)(using Encoder[T], SchemaFor[T]): Future[GenericRecord] = {
    given ExecutionContext = ec
    Future { toRecord(model) }
  }

  /** fromRecordAsync
   *
   * @param record GenericRecord
   * @param ec ExecutionContext
   * @tparam T T <: MarshallModel[ T ]
   * @return Future[T]
   */
  final def fromRecordAsync[T <: MarshallModel[T]](record: GenericRecord, ec: ExecutionContext)(using Decoder[T], SchemaFor[T]): Future[T] = {
    given ExecutionContext = ec
    Future { fromRecord(record) }
  }

  /** Like [[fromRecord]], but first conforms a record materialized under its
   *  *writer* schema onto the avro4s *reader* schema (`schema[T]`) — applied
   *  only when an Avro field **alias** actually resolves a renamed field (see
   *  [[conformToReaderSchema]]). Use this on the read side of a Confluent
   *  Schema Registry pipeline, where the generic `KafkaAvroDeserializer`
   *  hands back writer-schema records and does no reader-schema resolution.
   *
   * @param record GenericRecord (writer schema, e.g. from a generic deserializer)
   * @tparam T T <: MarshallModel[ T ]
   * @return T
   */
  final def fromRecordResolving[T <: MarshallModel[T]](record: GenericRecord)(using Decoder[T], SchemaFor[T]): T =
    fromRecord[T](conformToReaderSchema(record, schema[T]))

  /** fromRecordResolvingAsync
   *
   * @param record GenericRecord
   * @param ec ExecutionContext
   * @tparam T T <: MarshallModel[ T ]
   * @return Future[T]
   */
  final def fromRecordResolvingAsync[T <: MarshallModel[T]](record: GenericRecord, ec: ExecutionContext)(using Decoder[T], SchemaFor[T]): Future[T] = {
    given ExecutionContext = ec
    Future { fromRecordResolving[T](record) }
  }

  /** Resolve a [[GenericRecord]] materialized under its *writer* schema onto
   *  `readerSchema` — but only when an Avro field **alias** actually applies,
   *  i.e. a reader field was renamed and this record (written under the older
   *  name) still carries that name.
   *
   *  Why this is needed (and easy to get wrong):
   *    - Confluent's generic `KafkaAvroDeserializer` materializes records under
   *      the *writer* schema and does no reader-schema resolution.
   *    - avro4s then decodes by *reader* field name and does not consult Avro
   *      aliases on the `GenericRecord` path: `SchemaFieldDecoder` looks up the
   *      reader name in the record's (writer) schema and, when absent, falls
   *      back to the field's Scala default. A renamed field is therefore absent
   *      under its reader name — one without a default decodes `null` and throws
   *      (e.g. NPE on a non-null `String` key, which in Kafka Streams kills the
   *      `StreamThread`); one with a default silently takes the default instead
   *      of the carried-over value.
   *
   *  Scope is deliberately narrow. Ordinary backward-compatible evolution (a
   *  reader field the writer lacks, with no alias) is already handled by avro4s
   *  via the case class's Scala default, so those records skip the round-trip.
   *  Only an aliased rename — at any nesting depth — forces a resolve,
   *  delegated to Avro's own [[GenericDatumReader]] resolver rather than
   *  hand-mapping aliases: it handles nested records, unions, enums and reader
   *  defaults in one pass.
   *
   *  On a genuinely incompatible record the underlying Avro exception
   *  propagates; the caller owns the supervision policy (fail-fast vs DLQ).
   *
   *  Kept deliberately stateless: the alias decision ([[aliasResolutionRequired]],
   *  via `Schema.applyAliases`) and Avro's resolver are recomputed per record
   *  rather than memoized. A cache keyed on writer-schema identity would be
   *  faster on a hot topic replaying many legacy records, but it would add a
   *  shared, concurrent (multiple `StreamThread`s hit it), bounded cache to an
   *  otherwise stateless object — more moving parts to get right and test.
   *  Simple on purpose; add the cache only if profiling shows this path is hot.
   *
   * @param record GenericRecord (carrying its writer schema)
   * @param readerSchema Schema (target reader schema, e.g. `schema[T]`)
   * @return GenericRecord conformed to `readerSchema`, or `record` unchanged
   */
  final def conformToReaderSchema(record: GenericRecord, readerSchema: Schema): GenericRecord = {
    val writerSchema = record.getSchema
    if (aliasResolutionRequired(writerSchema, readerSchema))
      resolveViaAvro(record, writerSchema, readerSchema)
    else record
  }

  /** True iff applying the reader's aliases to the writer schema actually
   *  renames a field somewhere in the tree — the one case avro4s cannot decode
   *  unaided (the renamed field is absent under its reader name, so avro4s
   *  falls back to the Scala default, or fails when there is none). Detection
   *  is delegated to Avro's own `Schema.applyAliases`, which rewrites the
   *  writer schema using the reader's aliases recursively (records, unions,
   *  arrays, maps); resolution is required precisely when that rewrite changes
   *  the schema. When the reader declares no aliases, `applyAliases` returns
   *  the writer schema by reference and the comparison short-circuits. */
  private def aliasResolutionRequired(writerSchema: Schema, readerSchema: Schema): Boolean =
    !Schema.applyAliases(writerSchema, readerSchema).equals(writerSchema)

  private def resolveViaAvro(record: GenericRecord, writerSchema: Schema, readerSchema: Schema): GenericRecord = {
    val buffer = new ByteArrayOutputStream()
    val encoder = EncoderFactory.get().binaryEncoder(buffer, null)
    new GenericDatumWriter[GenericRecord](writerSchema).write(record, encoder)
    encoder.flush()
    val decoder = DecoderFactory.get().binaryDecoder(buffer.toByteArray, null)
    new GenericDatumReader[GenericRecord](writerSchema, readerSchema).read(null.asInstanceOf[GenericRecord], decoder)
  }
}
object AvroMarshaller extends AvroMarshaller
