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

package com.tomshley.boilerplate.jvm.marshalling.serializers

import com.sksamuel.avro4s.{Decoder, Encoder, SchemaFor}
import com.tomshley.boilerplate.jvm.utils.TimeUtils
import org.apache.avro.Schema

import java.io.File
import java.nio.file.{Path, Paths}
import java.time.ZonedDateTime

/** Opt-in avro4s given instances for types not auto-derived by Magnolia.
 *
 *  Parallel to [[com.tomshley.boilerplate.jvm.marshalling.serializers.json]].
 *  All types are serialized as Avro STRING using the same string
 *  representation as the JSON serializers.
 *
 *  Usage — import the givens you need:
 *  {{{
 *  import com.tomshley.boilerplate.jvm.marshalling.serializers.avro.given
 *  }}}
 */
package object avro {

  // ── DateTime (Joda — org.joda.time.DateTime via TimeUtils.DateTime) ──

  given SchemaFor[TimeUtils.DateTime] with
    def schema: Schema = Schema.create(Schema.Type.STRING)

  given Encoder[TimeUtils.DateTime] with
    def encode(schema: Schema): TimeUtils.DateTime => AnyRef = _.toString

  given Decoder[TimeUtils.DateTime] with
    def decode(schema: Schema): Any => TimeUtils.DateTime =
      v => TimeUtils.DateTime.parse(v.toString)

  // ── ZonedDateTime (java.time) ──

  given SchemaFor[ZonedDateTime] with
    def schema: Schema = Schema.create(Schema.Type.STRING)

  given Encoder[ZonedDateTime] with
    def encode(schema: Schema): ZonedDateTime => AnyRef = _.toString

  given Decoder[ZonedDateTime] with
    def decode(schema: Schema): Any => ZonedDateTime =
      v => ZonedDateTime.parse(v.toString)

  // ── File (java.io — absolute path string) ──

  given SchemaFor[File] with
    def schema: Schema = Schema.create(Schema.Type.STRING)

  given Encoder[File] with
    def encode(schema: Schema): File => AnyRef =
      f => f.toPath.toAbsolutePath.toString

  given Decoder[File] with
    def decode(schema: Schema): Any => File =
      v => Paths.get(v.toString).toFile

  // ── Path (java.nio.file — absolute path string) ──

  given SchemaFor[Path] with
    def schema: Schema = Schema.create(Schema.Type.STRING)

  given Encoder[Path] with
    def encode(schema: Schema): Path => AnyRef =
      p => p.toAbsolutePath.toString

  given Decoder[Path] with
    def decode(schema: Schema): Any => Path =
      v => Paths.get(v.toString)
}
