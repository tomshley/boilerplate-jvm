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

import scala.util.Try

trait ClaimPort[Envelope, ReplyBinding] {
  def decodeEnvelope(bytes: Array[Byte]): Try[Envelope]

  def openReplyBinding(
      onConfirmedClaimsCount: Long => Unit,
      onRejected: Throwable => Unit
  ): ReplyBinding

  def bindEntityId(replyBinding: ReplyBinding, entityId: String): Unit

  def clearEntityId(replyBinding: ReplyBinding): Unit

  def closeReplyBinding(replyBinding: ReplyBinding): Unit

  def dispatchClaim(
      entityId: String,
      envelope: Envelope,
      rawBytesLength: Long,
      replyBinding: ReplyBinding
  ): Unit
}
