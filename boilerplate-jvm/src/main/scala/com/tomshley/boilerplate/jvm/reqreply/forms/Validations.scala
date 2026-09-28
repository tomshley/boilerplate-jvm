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

package com.tomshley.boilerplate.jvm.reqreply.forms

import java.nio.charset.StandardCharsets.UTF_8
import scala.util.matching.Regex

/** Shared form-field shape checks. [[isValidEmailFormat]] accepts a basic
 *  `local@domain.tld` shape within the RFC 5321 octet bounds — 254 octets for
 *  the whole address and 64 for the local part. [[isValidPhoneFormat]] accepts
 *  a digits-only run of 10–15 digits or a grouped form carrying a separator,
 *  7–15 digits, and an optional `x`/`ext` extension. Both patterns are written
 *  to behave identically under Java regex and the HTML `pattern` attribute,
 *  and every input is length-checked before matching. */
trait Validations {

  /** The pattern sources are exposed because a rendered `<input pattern>`
   *  attribute must match what the server predicates enforce — exposing the
   *  string keeps browser-side and server-side checks on one definition. */
  final val phoneFormatPattern: String =
    "^(\\+?[0-9]{10,15}|\\+?(?=(?:[^0-9xXeE]*[0-9]){7})(?!(?:[^0-9xXeE]*[0-9]){16})(?=[^xXeE]*[0-9][^xXeE]*[ \\.\\-\\(\\)][^xXeE]*[0-9])[0-9\\(][0-9\\. \\-\\(\\)]{3,29}[0-9]( ?([xX]|[eE][xX][tT]\\.?) ?[0-9]{1,6})?)$"
  final val emailFormatPattern: String =
    "^[^\\s@,;:<>\\(\\)\\[\\]\\\\\"\\p{Cc}\\uFEFF]+@[^\\s@,;:<>\\(\\)\\[\\]\\\\\"\\p{Cc}\\uFEFF]+\\.[^\\s@,;:<>\\(\\)\\[\\]\\\\\"\\p{Cc}\\uFEFF]+$"

  private final val emailExpression: Regex = new Regex("(?U)" + emailFormatPattern)
  private final val phoneExpression: Regex = new Regex("(?U)" + phoneFormatPattern)

  /** RFC 5321 octet limit for the whole email address. */
  final val emailMaxLength: Int = 254

  /** RFC 5321 §4.5.3.1.1 octet limit for the local part — the text before `@`. */
  final val emailLocalPartMaxLength: Int = 64

  /** The longest value [[phoneFormatPattern]] can accept: optional `+` (1),
   *  body `[0-9(]` plus up to 29 more and a final digit (31), extension
   *  ` ext. ` plus six digits (12). Checking it before matching keeps
   *  matching cost bounded on hostile input. */
  final val phoneMaxLength: Int = 44

  def isValidEmailFormat(email: String): Boolean =
    Option(email).exists { value =>
      value.length <= emailMaxLength &&
        value.getBytes(UTF_8).length <= emailMaxLength &&
        value.takeWhile(_ != '@').getBytes(UTF_8).length <= emailLocalPartMaxLength &&
        emailExpression.matches(value)
    }

  def isValidPhoneFormat(phone: String): Boolean =
    Option(phone).exists(value =>
      value.length <= phoneMaxLength && phoneExpression.matches(value))

  /** Present and carrying visible content — the generic "required field"
   *  predicate. */
  def isNonBlank(value: String): Boolean =
    Option(value).exists(_.trim.nonEmpty)

  /** Length bounds as data — the caller owns the per-field limits, this owns
   *  the comparison shape. */
  def isLengthWithin(value: String, min: Int, max: Int): Boolean =
    Option(value).exists(v => v.length >= min && v.length <= max)

  def isShortEnough(message: String, limit: Int): Boolean =
    Option(message).exists(_.length <= limit)
}

object Validations extends Validations
