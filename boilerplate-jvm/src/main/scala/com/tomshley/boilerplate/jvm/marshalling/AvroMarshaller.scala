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
import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.CollectionConverters.*

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
 *
 *  Call-scoped by design: each call re-derives the avro4s encoder/decoder
 *  tree, which keeps this API dependency-free and safe anywhere, but is
 *  measurably expensive when invoked once per record. Per-record paths
 *  (Kafka Streams serdes, consumer loops) should derive once via
 *  [[AvroCodec]] and reuse it — same semantics, derivation paid at wiring
 *  time instead of per call.
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
   *  whenever the materialized writer schema differs from the reader schema (see
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
   *  `readerSchema` whenever the writer and reader schemas differ.
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
   *  The work is delegated to Avro's own [[GenericDatumReader]] resolver rather
   *  than hand-mapping fields: it handles nested records, unions, enums, aliases
   *  and reader defaults in one pass.
   *
   *  On a genuinely incompatible record the underlying Avro exception
   *  propagates; the caller owns the supervision policy (fail-fast vs DLQ).
   *
   *  Kept deliberately stateless: the schema comparison and Avro's resolver are
   *  recomputed per record rather than memoized. A cache keyed on writer-schema
   *  identity would be faster on a hot topic replaying many legacy records, but
   *  it would add a shared, concurrent (multiple `StreamThread`s hit it),
   *  bounded cache to an otherwise stateless object — more moving parts to get
   *  right and test.
   *  Simple on purpose; add the cache only if profiling shows this path is hot.
   *  (Profiled 2026-07: on a replay-heavy Kafka Streams workload the heat was
   *  in per-call avro4s re-derivation — addressed by [[AvroCodec]], which
   *  caches only the compile-time reader derivation — not here. This path runs
   *  only on writer/reader mismatch and stays uncached: writer schemas are
   *  runtime facts.)
   *
   * @param record GenericRecord (carrying its writer schema)
   * @param readerSchema Schema (target reader schema, e.g. `schema[T]`)
   * @return GenericRecord conformed to `readerSchema`, or `record` unchanged
   */
  final def conformToReaderSchema(record: GenericRecord, readerSchema: Schema): GenericRecord = {
    val writerSchema = record.getSchema
    if (!writerSchema.equals(readerSchema))
      resolveViaAvro(record, writerSchema, readerSchema)
    else record
  }

  private def resolveViaAvro(record: GenericRecord, writerSchema: Schema, readerSchema: Schema): GenericRecord = {
    val buffer = new ByteArrayOutputStream()
    val encoder = EncoderFactory.get().binaryEncoder(buffer, null)
    new GenericDatumWriter[GenericRecord](writerSchema).write(record, encoder)
    encoder.flush()
    val decoder = DecoderFactory.get().binaryDecoder(buffer.toByteArray, null)
    new GenericDatumReader[GenericRecord](writerSchema, withValidEnumDefaults(readerSchema))
      .read(null.asInstanceOf[GenericRecord], decoder)
  }

  /** Return `schema` unchanged unless it declares an enum-typed field whose
   *  default is not one of that enum's symbols, in which case a copy is returned
   *  with each such default replaced by a valid symbol.
   *
   *  A reader field that the writer omits is filled from the field's default
   *  during schema resolution. If that default is not a legal enum symbol the
   *  resolver fails: Avro materializes a default symbol the enum does not
   *  contain, and the subsequent ordinal lookup throws (surfacing as a
   *  `NullPointerException`). Some schema generators emit an empty-string
   *  default for an enum field that has a programming-language default — for
   *  example avro4s 5.0.15 does so for a Scala 3 `enum` field — which is not a
   *  member of the enum and triggers exactly this failure. Substituting the
   *  enum's declared default, or its first symbol when none is declared, lets
   *  resolution fill the field instead of crashing; listing an
   *  `UNKNOWN`/`UNSPECIFIED` sentinel first (a common Avro and Protobuf
   *  convention) makes that fallback the natural "unset" value.
   *
   *  The walk covers records, unions, arrays and maps so a field at any depth is
   *  handled, and it preserves names, types, aliases, properties, field order
   *  and every already-valid default. A well-formed schema is returned as-is, so
   *  the common case adds no allocation and flows through the resolver untouched.
   */
  private def withValidEnumDefaults(schema: Schema): Schema =
    if hasOutOfRangeEnumDefault(schema, mutable.Set.empty) then
      rewriteEnumDefaults(schema, mutable.Map.empty)
    else schema

  private def hasOutOfRangeEnumDefault(schema: Schema, visited: mutable.Set[String]): Boolean =
    schema.getType match
      case Schema.Type.RECORD =>
        visited.add(schema.getFullName) && schema.getFields.asScala.exists { field =>
          enumForDefault(field.schema).exists(enumDefaultNeedsRepair(field, _)) ||
            hasOutOfRangeEnumDefault(field.schema, visited)
        }
      case Schema.Type.UNION => schema.getTypes.asScala.exists(hasOutOfRangeEnumDefault(_, visited))
      case Schema.Type.ARRAY => hasOutOfRangeEnumDefault(schema.getElementType, visited)
      case Schema.Type.MAP   => hasOutOfRangeEnumDefault(schema.getValueType, visited)
      case _                 => false

  private def rewriteEnumDefaults(schema: Schema, rebuilt: mutable.Map[String, Schema]): Schema =
    schema.getType match
      case Schema.Type.RECORD =>
        rebuilt.getOrElse(
          schema.getFullName, {
            val record = Schema.createRecord(schema.getName, schema.getDoc, schema.getNamespace, schema.isError)
            schema.getObjectProps.forEach((k, v) => record.addProp(k, v))
            schema.getAliases.forEach(record.addAlias)
            rebuilt.put(schema.getFullName, record)
            record.setFields(schema.getFields.asScala.map(rewriteField(_, rebuilt)).asJava)
            record
          }
        )
      case Schema.Type.UNION => Schema.createUnion(schema.getTypes.asScala.map(rewriteEnumDefaults(_, rebuilt)).asJava)
      case Schema.Type.ARRAY => Schema.createArray(rewriteEnumDefaults(schema.getElementType, rebuilt))
      case Schema.Type.MAP   => Schema.createMap(rewriteEnumDefaults(schema.getValueType, rebuilt))
      case _                 => schema

  private def rewriteField(field: Schema.Field, rebuilt: mutable.Map[String, Schema]): Schema.Field =
    val fieldSchema = rewriteEnumDefaults(field.schema, rebuilt)
    enumForDefault(fieldSchema) match
      case Some(enumSchema) if enumDefaultNeedsRepair(field, enumSchema) =>
        val validDefault = Option(enumSchema.getEnumDefault).getOrElse(enumSchema.getEnumSymbols.get(0))
        val replacement = new Schema.Field(field.name, fieldSchema, field.doc, validDefault, field.order)
        field.getObjectProps.forEach((k, v) => replacement.addProp(k, v))
        field.aliases.forEach(replacement.addAlias)
        replacement
      case _ =>
        new Schema.Field(field, fieldSchema)

  /** True when `field` carries a default that binds to `enumSchema` but is not
   *  one of its symbols. */
  private def enumDefaultNeedsRepair(field: Schema.Field, enumSchema: Schema): Boolean =
    field.hasDefaultValue && !isEnumSymbol(enumSchema, field.defaultVal)

  /** The enum a field default binds to: the field type itself when it is an
   *  enum, or the first member of a union (Avro binds a field default to the
   *  first union branch). `None` for any other shape. */
  private def enumForDefault(schema: Schema): Option[Schema] =
    schema.getType match
      case Schema.Type.ENUM  => Some(schema)
      case Schema.Type.UNION => schema.getTypes.asScala.headOption.filter(_.getType == Schema.Type.ENUM)
      case _                 => None

  private def isEnumSymbol(enumSchema: Schema, default: Any): Boolean =
    default match
      case s: String => enumSchema.getEnumSymbols.contains(s)
      case _         => false
}
object AvroMarshaller extends AvroMarshaller
