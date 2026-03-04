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
import org.apache.avro.generic.GenericRecord

import models.MarshallModel

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
}
object AvroMarshaller extends AvroMarshaller
