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

import com.sksamuel.avro4s.{Avro4sEncodingException, Decoder, Encoder, SchemaFor}
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord

import models.MarshallModel

/** Pre-derived avro4s codec for a [[MarshallModel]] — the per-record-path
 *  complement to [[AvroMarshaller]].
 *
 *  Why this exists: avro4s's call-scoped surface (`ToRecord`/`FromRecord`,
 *  which [[AvroMarshaller.toRecord]]/[[AvroMarshaller.fromRecord]] wrap)
 *  re-derives the full encoder/decoder tree on every call —
 *  `RecordDecoder.decode(schema)` rebuilds one `SchemaFieldDecoder` per field
 *  (a linear scan of the schema's fields plus recursive sub-decoder
 *  derivation) before it looks at the record. That is fine for occasional
 *  marshalling; on per-record paths it dominates. Profiling a replay-heavy
 *  Kafka Streams workload showed the re-derivation — not Avro byte codecs,
 *  not state-store I/O — consuming the majority of `StreamThread` CPU.
 *  avro4s's intended hot-path usage is to call `decode(schema)`/
 *  `encode(schema)` once and reuse the returned function; an `AvroCodec`
 *  captures exactly that: derivation happens once, at construction.
 *
 *  Schema-evolution safety — what may be cached and what must stay dynamic:
 *    - The *reader* derivation (this codec) is a pure function of the
 *      compile-time type: `SchemaFor[T]` is derived by Magnolia from the case
 *      class. No runtime event can invalidate it — a new reader schema means
 *      a new binary, which means a fresh codec. Caching it is therefore
 *      evolution-proof by construction.
 *    - The *writer* schema is a per-record fact (registry-framed records
 *      carry it) and can change mid-stream — producer deploys, replays over
 *      historical versions, interleaved versions on one topic.
 *      [[fromRecordResolving]] consults `record.getSchema` on every record
 *      and delegates mismatches to
 *      [[AvroMarshaller.conformToReaderSchema]], exactly as the uncached
 *      path does, so evolution behavior is byte-for-byte identical.
 *
 *  Thread-safety: an `AvroCodec` is immutable and the captured avro4s
 *  functions close over immutable decoder/encoder trees (avro4s's field
 *  decoders carry only a benign, idempotent fast-path flag — same-valued
 *  writes, safe under races). One instance may be shared freely, e.g. across
 *  Kafka Streams `StreamThread`s.
 *
 *  Usage — derive once at wiring time, reuse per record:
 *  {{{
 *  val codec = AvroCodec[DeviceEvent]
 *  val record = codec.toRecord(DeviceEvent("d1", ONLINE))
 *  val back   = codec.fromRecord(record)
 *  }}}
 */
final class AvroCodec[T <: MarshallModel[T]] private (
    /** The avro4s reader schema for `T` (identical to `AvroMarshaller.schema[T]`). */
    val schema: Schema,
    encodeFn: T => AnyRef,
    decodeFn: Any => T,
):

  /** Encode `model` as a [[GenericRecord]] using the derivation captured at
   *  construction. Equivalent to [[AvroMarshaller.toRecord]] without the
   *  per-call re-derivation.
   */
  def toRecord(model: T): GenericRecord =
    encodeFn(model) match
      case record: GenericRecord => record
      case other =>
        throw new Avro4sEncodingException(
          s"Cannot marshall an instance of $model to a GenericRecord (output was $other of class ${other.getClass})"
        )

  /** Decode a reader-shaped [[GenericRecord]] using the derivation captured
   *  at construction. Equivalent to [[AvroMarshaller.fromRecord]] without the
   *  per-call re-derivation. Callers own null/tombstone handling, as with
   *  [[AvroMarshaller.fromRecord]].
   */
  def fromRecord(record: GenericRecord): T =
    decodeFn(record)

  /** Like [[fromRecord]], but first conforms a record materialized under its
   *  *writer* schema onto this codec's reader schema whenever the two differ —
   *  the registry-framed read-side path. The conform step is deliberately
   *  per-record and uncached (see the decision note on
   *  [[AvroMarshaller.conformToReaderSchema]]): writer schemas are runtime
   *  facts and must never be assumed stable. Only the reader derivation is
   *  reused.
   */
  def fromRecordResolving(record: GenericRecord): T =
    decodeFn(AvroMarshaller.conformToReaderSchema(record, schema))

object AvroCodec:

  /** Derive the codec for `T` once. Encoder, Decoder, and SchemaFor are
   *  auto-derived by Magnolia for case classes, as with [[AvroMarshaller]].
   */
  def apply[T <: MarshallModel[T]](using enc: Encoder[T], dec: Decoder[T], sf: SchemaFor[T]): AvroCodec[T] =
    val schema = sf.schema
    new AvroCodec[T](schema, enc.encode(schema), dec.decode(schema))
