/*
 * Copyright 2026 Tomshley LLC.
 * All Rights Reserved.
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
