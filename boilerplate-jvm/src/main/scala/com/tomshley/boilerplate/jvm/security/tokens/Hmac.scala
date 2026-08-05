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

import java.nio.charset.StandardCharsets
import java.security.MessageDigest
import javax.crypto.Mac
import javax.crypto.spec.SecretKeySpec

/**
 * Domain-separated truncated HMAC, package-private: computed MACs never escape,
 * so nothing outside can compare one with a non-constant-time equality.
 *
 * JCA primitives only. The MAC is the first [[Hmac.MacLength]] bytes of
 * HMAC-SHA256 over `domainSeparator-bytes ++ message`. JCA's `Mac` is mutable and
 * not thread-safe, so an instance is created, used, and dropped inside [[mac]] —
 * the surface stays a pure function of key, separator, and message.
 */
private[tokens] object Hmac {

  /** Truncated MAC length in bytes (96 bits). */
  val MacLength: Int = 12

  private val Algorithm = "HmacSHA256"

  def mac(secret: SecretBytes, domainSeparator: String, message: Array[Byte]): Array[Byte] = {
    val engine = Mac.getInstance(Algorithm)
    engine.init(new SecretKeySpec(secret.unsafeBytes, Algorithm))
    engine.update(domainSeparator.getBytes(StandardCharsets.UTF_8))
    engine.update(message)
    engine.doFinal().take(MacLength)
  }

  def verify(secret: SecretBytes, domainSeparator: String, message: Array[Byte], presented: Array[Byte]): Boolean =
    MessageDigest.isEqual(mac(secret, domainSeparator, message), presented)
}
