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

package com.tomshley.boilerplate.jvm.marshalling.serializers

import com.tomshley.boilerplate.jvm.utils.TimeUtils
import org.json4s.{CustomSerializer, JString}

import java.io.File
import java.nio.file.{Path, Paths}
import java.time.ZonedDateTime
import scala.reflect.{ClassTag, classTag}

package object json {
  final class DateTimeSerializer
      extends CustomSerializer[TimeUtils.DateTime](
        format =>
          ({
            case JString(dateTimeValue) =>
              TimeUtils.DateTime.parse(dateTimeValue)
          }, {
            case dateTime: TimeUtils.DateTime => JString(dateTime.toString())
          })
      )
  final class ZonedDateTimeSerializer
      extends CustomSerializer[ZonedDateTime](
        format =>
          ({
            case JString(zonedDateTimeValue) =>
              ZonedDateTime.parse(zonedDateTimeValue)
          }, {
            case zonedDateTimeValue: ZonedDateTime => JString(zonedDateTimeValue.toString)
          })
      )
  final class AbsPathFileSerializer
      extends CustomSerializer[File](
        format =>
          ({
            case JString(absolutePath) => Paths.get(absolutePath).toFile
          }, {
            case file: File => JString(file.toPath.toAbsolutePath.toString)
          })
      )

  final class AbsPathSerializer
      extends CustomSerializer[Path](
        format =>
          ({
            case JString(absolutePath) => Paths.get(absolutePath)
          }, {
            case path: Path => JString(path.toAbsolutePath.toString)
          })
      )

  final class JavaEnumNameSerializer[E <: java.lang.Enum[E]: ClassTag]
      extends CustomSerializer[E](
        format =>
          ({
            case JString(value) =>
              val myEnumFind = classTag[E].runtimeClass.getEnumConstants
                .asInstanceOf[Array[E]]
                .find(_.toString == value)
              myEnumFind.get

          }, {
            case myEnum: E =>

              JString(myEnum.toString)
          })
      )
}
