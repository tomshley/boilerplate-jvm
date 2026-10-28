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

final case class FlusherMetaView(
    lastSpooledSeq: Long,
    blobKeyPrefix: BlobKeyPrefix
)

trait FlusherMetaAdapter[M] {
  def toFlusherMetaView(meta: M): FlusherMetaView
}

final class SpoolMetaFlusherAdapter(
    prefixBuilder: SpoolMeta => BlobKeyPrefix
) extends FlusherMetaAdapter[SpoolMeta] {
  override def toFlusherMetaView(meta: SpoolMeta): FlusherMetaView =
    FlusherMetaView(
      lastSpooledSeq = meta.lastSpooledSeq,
      blobKeyPrefix = prefixBuilder(meta)
    )
}

object SpoolMetaFlusherAdapter {
  def apply(prefixBuilder: SpoolMeta => BlobKeyPrefix): SpoolMetaFlusherAdapter =
    new SpoolMetaFlusherAdapter(prefixBuilder)
}
