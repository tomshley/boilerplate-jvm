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

package com.tomshley.boilerplate.jvm.utils

import java.security.MessageDigest

object ChecksumUtil extends ChecksumUtil

trait ChecksumUtil {
  /** MD5 hex digest — suitable for integrity checks and fingerprinting, not for security hashing. */
  def toMD5(contents: String): String = {
    MessageDigest.getInstance("MD5").digest(contents.getBytes("UTF-8")).map(0xFF & _).map {
      "%02x".format(_)
    }.foldLeft("") {
      _ + _
    }
  }

  /** SHA-256 hex digest — use when a cryptographically strong hash is required. */
  def toSHA256(contents: String): String = {
    MessageDigest.getInstance("SHA-256").digest(contents.getBytes("UTF-8")).map(0xFF & _).map {
      "%02x".format(_)
    }.foldLeft("") {
      _ + _
    }
  }

  def computeCrc32(data: Array[Byte]): Long = {
    val crc = new java.util.zip.CRC32()
    crc.update(data)
    crc.getValue
  }
}
