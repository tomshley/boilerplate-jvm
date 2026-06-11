/*
 * Copyright 2026 Tomshley LLC.
 * All Rights Reserved.
 */

package com.tomshley.boilerplate.jvm.durablebufferedflush

import java.time.Instant
import scala.concurrent.duration.FiniteDuration

final case class FlushPauseTimedOut(entityId: String, timeout: FiniteDuration)
    extends RuntimeException(s"Backpressure pause timeout ($timeout) for entity $entityId")

final case class FlushPauseCancelled(entityId: String, causeOpt: Option[Throwable])
    extends RuntimeException(s"Backpressure pause cancelled for entity $entityId", causeOpt.orNull)

final case class SessionView[Device](
    isClosed: Boolean,
    isAborted: Boolean,
    openedAt: Option[Instant],
    device: Option[Device],
    deviceCorrelationId: Option[String],
    objectHashHex: String,
    declaredPayloadSize: Long,
    claimsCount: Long,
    totalClaimedBytes: Long,
    lastClaimSequence: Long
)

/** @param fileName producer-declared file name, when the transport carries
  *                 one (e.g. a TCP metadata frame). `None` for transports
  *                 whose envelope has no file identity. Threaded to
  *                 [[SessionPort.register]] so downstream read models can
  *                 correlate re-uploads of the same producer file across
  *                 content-hash-keyed entities. */
final case class FlushTransferDescriptor[Device](
    entityId: String,
    device: Device,
    deviceId: String,
    deviceCorrelationId: String,
    objectHashHex: String,
    declaredPayloadSize: Long,
    totalExpectedChunks: Long,
    fileName: Option[String] = None
)

final case class FlushConnectionBinding[ReplyBinding](
    lagMonitor: ClaimLagMonitor,
    replyBinding: ReplyBinding,
    flusher: Option[ChunkFlusher] = None
)

final case class FlushPreparedTransfer[ReplyBinding](
    binding: FlushConnectionBinding[ReplyBinding],
    receivedChunks: Long,
    lastAcceptedSeq: Long,
    isComplete: Boolean
)

final case class FlushAcceptedChunk[ReplyBinding](
    binding: FlushConnectionBinding[ReplyBinding]
)

final case class FlushFinalizationResult[ReplyBinding](
    binding: FlushConnectionBinding[ReplyBinding],
    outcome: FinalizeOutcome = FinalizeOutcome.Unverified(FinalizeOutcome.UnverifiedReason.NotEvaluated)
)
