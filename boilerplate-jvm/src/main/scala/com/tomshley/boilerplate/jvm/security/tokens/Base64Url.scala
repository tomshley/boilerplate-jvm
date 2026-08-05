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
 * Strict, unpadded, URL-safe base64, package-private. An encoding either matches
 * the canonical shape exactly or it is malformed — padding characters, the
 * standard alphabet's `+`/`/`, whitespace, impossible lengths, and non-canonical
 * trailing bits all decode to `None`. There is no lenient path.
 *
 * The canonicality check (decode, re-encode, compare) closes the malleability
 * seam where a final character carries unused bits: without it an authenticated
 * value would have more than one accepted wire form.
 */
private[tokens] object Base64Url {

  private val UrlSafeAlphabet = "^[A-Za-z0-9_-]+$".r

  /** Unpadded encoded length for a decoded byte count. */
  def encodedLength(decodedLength: Int): Int = (decodedLength * 4 + 2) / 3

  def encode(bytes: Array[Byte]): String =
    java.util.Base64.getUrlEncoder.withoutPadding.encodeToString(bytes)

  /** Strict decode; `None` unless the input is canonical unpadded url-safe base64. */
  def decode(encoded: String): Option[Array[Byte]] =
    if (encoded == null || !UrlSafeAlphabet.matches(encoded)) None
    else
      scala.util
        .Try(java.util.Base64.getUrlDecoder.decode(encoded))
        .toOption
        .filter(bytes => encode(bytes) == encoded)

  /** Strict decode that additionally pins the decoded length. */
  def decodeExact(encoded: String, expectedDecodedLength: Int): Option[Array[Byte]] =
    if (encoded == null || encoded.length != encodedLength(expectedDecodedLength)) None
    else decode(encoded).filter(_.length == expectedDecodedLength)
}
