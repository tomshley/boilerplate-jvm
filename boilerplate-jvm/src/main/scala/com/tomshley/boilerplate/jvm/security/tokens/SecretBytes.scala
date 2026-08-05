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

import java.security.MessageDigest

/**
 * Key-material container, package-private on purpose: key bytes never cross the
 * `tokens` surface.
 *
 * Deliberately NOT `MintedPimpedBytes` — that type carries `@JsonValue`, which
 * would let Jackson serialize key material into a payload. This one has no
 * serialization hooks, a redacted `toString`, constant-time content equality
 * (`MessageDigest.isEqual`), and a length-only `hashCode` so key bytes cannot be
 * probed through hash collisions.
 *
 * Key material is not wiped: instances are immutable and shared by every keyring
 * derived from them (`withKey`/`retire`), so a `zeroize` would be a use-after-free
 * footgun — and on the JVM it cannot be guaranteed anyway (GC copies, heap dumps,
 * swap). Custody, not erasure, is the control here.
 */
private[tokens] final class SecretBytes private (private val bytes: Array[Byte]) {

  private[tokens] def length: Int = bytes.length

  /** No-copy access for the MAC primitive. Must never escape the `tokens` package. */
  private[tokens] def unsafeBytes: Array[Byte] = bytes

  override def equals(other: Any): Boolean = other match {
    case that: SecretBytes => MessageDigest.isEqual(this.bytes, that.bytes)
    case _                 => false
  }

  override def hashCode: Int = bytes.length

  override def toString: String = s"SecretBytes(length=${bytes.length})"
}

private[tokens] object SecretBytes {

  def fromHex(hex: String): SecretBytes = {
    require(hex != null && hex.nonEmpty, "hex key must not be null or empty")
    require(hex.length % 2 == 0, s"hex key must have an even length, got ${hex.length}")
    require(hex.forall(isHexDigit), "hex key must contain only hex digits")
    new SecretBytes(hex.grouped(2).map(pair => Integer.parseInt(pair, 16).toByte).toArray)
  }

  private def isHexDigit(c: Char): Boolean =
    (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F')
}
