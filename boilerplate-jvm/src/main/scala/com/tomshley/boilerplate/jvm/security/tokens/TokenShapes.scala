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

package com.tomshley.boilerplate.jvm.security.tokens

/**
 * String-shape predicates for building decode ladders over mixed token
 * generations, in the house 'pimp my library' style:
 *
 * {{{
 * import com.tomshley.boilerplate.jvm.security.tokens.TokenShapes.*
 *
 * if candidate.isUuidShaped then ...
 * else if candidate.isDecimalUint32 then ...
 * }}}
 *
 * Shape checks only — no parsing side effects, no judgment about validity.
 * Callers own the ladder (which shapes are admitted, in which order).
 */
object TokenShapes {

  private val HyphenPositions = Set(8, 13, 18, 23)
  private val Uint32Max = 4294967295L

  extension (candidate: String) {

    /** True for the canonical 36-char `8-4-4-4-12` hex-and-hyphen shape (either hex case). */
    def isUuidShaped: Boolean =
      candidate != null && candidate.length == 36 && candidate.zipWithIndex.forall { (c, i) =>
        if (HyphenPositions.contains(i)) c == '-' else isHexDigit(c)
      }

    /**
     * True for a plain decimal rendering of an unsigned 32-bit integer: 1..10
     * digits, no sign, value at most 4294967295. Leading zeros are accepted.
     */
    def isDecimalUint32: Boolean =
      candidate != null && candidate.nonEmpty && candidate.length <= 10 &&
        candidate.forall(c => c >= '0' && c <= '9') &&
        java.lang.Long.parseLong(candidate) <= Uint32Max
  }

  private def isHexDigit(c: Char): Boolean =
    (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F')
}
