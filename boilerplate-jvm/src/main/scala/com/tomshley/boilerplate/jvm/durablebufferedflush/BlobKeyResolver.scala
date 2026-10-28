/*
 * copyright 2026 tomshley llc
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

package com.tomshley.boilerplate.jvm.durablebufferedflush

final case class BlobKeyPrefix(value: String) extends AnyVal

trait BlobKeyResolver {
  def chunkKey(blobKeyPrefix: BlobKeyPrefix, sequence: Long): String
}

object PrefixedSequentialBlobKeyResolver extends BlobKeyResolver {
  override def chunkKey(blobKeyPrefix: BlobKeyPrefix, sequence: Long): String =
    f"${blobKeyPrefix.value}/$sequence%09d"
}
