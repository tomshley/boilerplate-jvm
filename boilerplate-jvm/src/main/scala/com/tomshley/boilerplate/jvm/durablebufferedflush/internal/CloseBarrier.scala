/*
 * Copyright 2026 Tomshley LLC.
 * All Rights Reserved.
 */

package com.tomshley.boilerplate.jvm.durablebufferedflush.internal

import com.tomshley.boilerplate.jvm.durablebufferedflush.*
import org.apache.pekko.actor.typed.ActorSystem
import org.apache.pekko.pattern.after

import scala.concurrent.{ExecutionContext, Future}
import scala.util.control.NonFatal

final class CloseBarrier[Device, Summary, Envelope, ReplyBinding](
    spool: ChunkSpool,
    sessionPort: SessionPort[Device, Summary],
    claimPort: ClaimPort[Envelope, ReplyBinding],
    config: FlushConfig,
    system: ActorSystem[?]
) {

  private given ExecutionContext = system.executionContext

  private val scheduler = system.classicSystem.scheduler

  def closeWithRetry(
      entityId: String,
      lastSpooledSeq: Long,
      expectedClaimsCount: Long,
      expectedTotalBytes: Long,
      expectedLastSequence: Long,
      replyBinding: ReplyBinding,
      retriesLeft: Int = config.close.maxRetries
  ): Future[Summary] = {
    given org.apache.pekko.util.Timeout = config.close.askTimeout
    sessionPort.closeWithValidation(
      entityId,
      expectedClaimsCount,
      expectedTotalBytes,
      expectedLastSequence
    ).recoverWith {
      case ex if retriesLeft > 0 && !CloseValidationFailure.isFatal(ex) =>
        system.log.warn(
          "Close validation failed for entity {} ({} retries left): {}",
          entityId,
          retriesLeft,
          ex.getMessage
        )
        resendMissingClaims(entityId, lastSpooledSeq, replyBinding).recover { case NonFatal(resendEx) =>
          system.log.warn(
            "Resend failed for entity {}, proceeding to retry Close: {}",
            entityId,
            resendEx.getMessage
          )
          ()
        }.flatMap { _ =>
          after(config.close.retryDelay, scheduler) {
            closeWithRetry(
              entityId = entityId,
              lastSpooledSeq = lastSpooledSeq,
              expectedClaimsCount = expectedClaimsCount,
              expectedTotalBytes = expectedTotalBytes,
              expectedLastSequence = expectedLastSequence,
              replyBinding = replyBinding,
              retriesLeft = retriesLeft - 1
            )
          }
        }
    }
  }

  def resendMissingClaims(
      entityId: String,
      lastSpooledSeq: Long,
      replyBinding: ReplyBinding
  ): Future[Unit] = {
    given org.apache.pekko.util.Timeout = config.close.inspectTimeout
    sessionPort.inspect(entityId).flatMap { summary =>
      val sessionView = sessionPort.toSessionView(summary)
      val contiguousClaimsCount = sessionView.lastClaimSequence + 1L
      if (sessionView.claimsCount != contiguousClaimsCount) {
        Future.failed(new IllegalStateException(
          s"Non-contiguous claim state for entity $entityId: claimsCount=${sessionView.claimsCount}, lastClaimSequence=${sessionView.lastClaimSequence}"
        ))
      } else {
        val firstMissing = contiguousClaimsCount
        if (firstMissing > lastSpooledSeq) {
          Future.successful(())
        } else {
          system.log.info(
            "Resending claims for entity {} seqs {} to {} ({} missing)",
            entityId,
            firstMissing,
            lastSpooledSeq,
            lastSpooledSeq - firstMissing + 1L
          )
          val batchSize = math.max(1, config.recovery.parallelism)
          (firstMissing to lastSpooledSeq).grouped(batchSize).foldLeft(Future.successful(())) { (accF, batch) =>
            accF.flatMap { _ =>
              Future.traverse(batch.toList) { seq =>
                spool.readChunk(entityId, seq).flatMap { bytes =>
                  Future.fromTry(claimPort.decodeEnvelope(bytes)).map { envelope =>
                    claimPort.dispatchClaim(entityId, envelope, bytes.length.toLong, replyBinding)
                  }
                }
              }.map(_ => ())
            }
          }
        }
      }
    }
  }
}
