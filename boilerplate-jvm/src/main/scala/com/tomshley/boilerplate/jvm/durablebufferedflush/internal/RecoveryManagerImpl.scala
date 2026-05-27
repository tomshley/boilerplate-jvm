/*
 * Copyright 2026 Tomshley LLC.
 * All Rights Reserved.
 */

package com.tomshley.boilerplate.jvm.durablebufferedflush.internal

import com.tomshley.boilerplate.jvm.durablebufferedflush.{
  ChunkFlusherFactory,
  ChunkSpool,
  ClaimPort,
  FlushConfig,
  OrphanReconciler,
  SessionPort,
  RecoveryManager,
  RecoveryReport,
  SessionView,
  SpoolMeta
}
import org.apache.pekko.actor.typed.ActorSystem
import org.apache.pekko.pattern.after

import java.util.concurrent.TimeoutException
import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration.*
import scala.util.control.NonFatal

final class RecoveryManagerImpl[Device, Summary, Envelope, ReplyBinding](
    spool: ChunkSpool,
    flusherFactory: ChunkFlusherFactory,
    sessionPort: SessionPort[Device, Summary],
    claimPort: ClaimPort[Envelope, ReplyBinding],
    config: FlushConfig,
    system: ActorSystem[?]
) extends RecoveryManager
    with OrphanReconciler {

  private given ExecutionContext = system.executionContext

  private val scheduler = system.classicSystem.scheduler
  private val closeBarrier = new CloseBarrier(
    spool = spool,
    sessionPort = sessionPort,
    claimPort = claimPort,
    config = config,
    system = system
  )

  override def recover(): Future[RecoveryReport] =
    withRecoveryReplyBinding { recoveryReplyBinding =>
      spool.listEntities().flatMap { entityIds =>
        runRecoveryPass(entityIds.distinct.sorted, recoveryReplyBinding)
      }
    }

  override def reconcileOrphans(isActive: String => Future[Boolean]): Future[RecoveryReport] =
    withRecoveryReplyBinding { recoveryReplyBinding =>
      spool.listEntities().flatMap { entityIds =>
        val sortedIds = entityIds.distinct.sorted
        partitionOrphans(sortedIds, isActive).flatMap { orphans =>
          runRecoveryPass(orphans, recoveryReplyBinding)
        }
      }
    }

  /** Resolve `isActive` for every entity in batches sized by
    * `config.recovery.parallelism`, returning the entities for which the
    * predicate resolved to `false` (the orphans). Batched so that an
    * expensive `isActive` implementation (e.g. an actor ask) cannot be
    * fanned out unboundedly. A predicate failure or per-entity timeout is
    * logged and treated as `true` (skip this entity this pass) — the
    * conservative choice. */
  private def partitionOrphans(
      entityIds: Seq[String],
      isActive: String => Future[Boolean]
  ): Future[Seq[String]] =
    entityIds.grouped(config.recovery.parallelism).foldLeft(Future.successful(Vector.empty[String])) {
      (accFuture, batch) =>
        accFuture.flatMap { acc =>
          Future.traverse(batch) { id =>
            withPerEntityTimeout(isActive(id), id, "isActive").recover { case NonFatal(ex) =>
              system.log.warn(
                "reconcileOrphans: isActive check failed for entity {}: {} — treating as active to skip reconciliation this pass",
                id,
                ex.getMessage
              )
              true
            }.map(active => Option.when(!active)(id))
          }.map(results => acc ++ results.flatten)
        }
    }

  /** Wrap a per-entity Future with `config.recovery.perEntityTimeout`. Used to
    * bound the wall-clock cost of any single per-entity recovery step so that
    * a stuck downstream cannot wedge an entire reconciliation pass.
    *
    * On timeout the returned Future fails with [[TimeoutException]]; the
    * caller's existing failure-containment logic decides how to interpret
    * that (e.g. `partitionOrphans` treats it as "active", `recoverSession`
    * contains it as a per-entity `Failed`).
    *
    * The original Future is NOT cancelled — Scala `Future` has no
    * cancellation primitive — but the timeout completes the result
    * deterministically and frees any caller waiting on it. */
  private def withPerEntityTimeout[A](
      work: Future[A],
      entityId: String,
      opName: String
  ): Future[A] = {
    val timeoutDuration = config.recovery.perEntityTimeout
    val timeoutFuture: Future[A] = after(timeoutDuration, scheduler) {
      Future.failed[A](new TimeoutException(
        s"per-entity timeout ($timeoutDuration) exceeded for entity $entityId during $opName"
      ))
    }
    Future.firstCompletedOf(Seq(work, timeoutFuture))
  }

  private def runRecoveryPass(
      entityIds: Seq[String],
      recoveryReplyBinding: ReplyBinding
  ): Future[RecoveryReport] =
    recoverBatches(entityIds, recoveryReplyBinding).map { results =>
      RecoveryReport(
        sessionsRecovered = results.count { case RecoverySessionResult.Recovered(_) => true; case _ => false },
        sessionsAborted = results.count(_ == RecoverySessionResult.Aborted),
        sessionsCleaned = results.count(_ == RecoverySessionResult.Cleaned),
        sessionsFailed = results.count(_ == RecoverySessionResult.Failed),
        totalClaimsResent = results.map(_.claimsResent).sum
      )
    }

  private def recoverBatches(
      entityIds: Seq[String],
      recoveryReplyBinding: ReplyBinding
  ): Future[Seq[RecoverySessionResult]] =
    entityIds.grouped(config.recovery.parallelism).foldLeft(Future.successful(Vector.empty[RecoverySessionResult])) {
      (accFuture, batch) =>
        accFuture.flatMap { acc =>
          Future.traverse(batch)(recoverSession(_, recoveryReplyBinding)).map(acc ++ _)
        }
    }

  private def recoverSession(
      entityId: String,
      recoveryReplyBinding: ReplyBinding
  ): Future[RecoverySessionResult] =
    withPerEntityTimeout(
      recoverSessionUnsafe(entityId, recoveryReplyBinding),
      entityId,
      "recoverSession"
    ).recover { case NonFatal(ex) =>
      system.log.error("Recovery failed for entity {} — containing failure: {}", entityId, ex.getMessage)
      RecoverySessionResult.Failed
    }

  private def recoverSessionUnsafe(
      entityId: String,
      recoveryReplyBinding: ReplyBinding
  ): Future[RecoverySessionResult] =
    spool.readMeta(entityId).map(Right(_)).recover { case NonFatal(ex) => Left(ex) }.flatMap {
      case Left(ex) =>
        handleUnreadableSpool(entityId, ex)
      case Right(None) =>
        handleUnreadableSpool(entityId, new IllegalStateException(s"Spool meta missing for entity $entityId"))
      case Right(Some(meta)) =>
        validateMeta(entityId, meta) match {
          case Some(problem) =>
            handleUnreadableSpool(entityId, new IllegalStateException(problem))
          case None =>
            inspectForRecovery(entityId).flatMap { summary =>
              val view = sessionPort.toSessionView(summary)
              if (view.openedAt.isEmpty || view.isClosed) {
                cleanup(entityId).map(_ => RecoverySessionResult.Cleaned)
              } else if (view.lastClaimSequence > meta.lastSpooledSeq) {
                abortAndCleanup(entityId, s"Recovery failed: spool behind journal (actor=${view.lastClaimSequence}, spool=${meta.lastSpooledSeq})")
              } else {
                resumeIfNeeded(entityId, view).flatMap { resumedSummary =>
                  val resumedView = sessionPort.toSessionView(resumedSummary)
                  if (resumedView.lastClaimSequence > meta.lastSpooledSeq) {
                    abortAndCleanup(entityId, s"Recovery failed: spool behind journal (actor=${resumedView.lastClaimSequence}, spool=${meta.lastSpooledSeq})")
                  } else if (meta.isComplete) {
                    recoverCompleteSession(entityId, meta, recoveryReplyBinding)
                  } else {
                    recoverPartialSession(entityId, meta, recoveryReplyBinding)
                  }
                }
              }
            }
        }
    }

  private def recoverCompleteSession(
      entityId: String,
      meta: SpoolMeta,
      recoveryReplyBinding: ReplyBinding
  ): Future[RecoverySessionResult] =
    resendMissingClaims(entityId, meta.lastSpooledSeq, recoveryReplyBinding).flatMap { claimsResent =>
      drainSpool(entityId, meta).flatMap { _ =>
        closeBarrier.closeWithRetry(
          entityId = entityId,
          lastSpooledSeq = meta.lastSpooledSeq,
          expectedClaimsCount = meta.lastSpooledSeq + 1L,
          expectedTotalBytes = meta.totalSpooledBytes,
          expectedLastSequence = meta.lastSpooledSeq,
          replyBinding = recoveryReplyBinding
        ).flatMap { _ =>
          cleanup(entityId).map(_ => RecoverySessionResult.Recovered(claimsResent))
        }
      }
    }.recoverWith { case NonFatal(ex) =>
      abortAndCleanup(entityId, s"Recovery finalize failed: ${ex.getMessage}")
    }

  private def recoverPartialSession(
      entityId: String,
      meta: SpoolMeta,
      recoveryReplyBinding: ReplyBinding
  ): Future[RecoverySessionResult] =
    resendMissingClaims(entityId, meta.lastSpooledSeq, recoveryReplyBinding).flatMap { claimsResent =>
      drainSpool(entityId, meta)
        .recover { case NonFatal(ex) =>
          system.log.warn("Partial recovery drain failed for entity {}: {}", entityId, ex.getMessage)
          ()
        }
        .map(_ => RecoverySessionResult.Recovered(claimsResent))
    }.recoverWith { case NonFatal(ex) =>
      abortAndCleanup(entityId, s"Recovery failed: ${ex.getMessage}")
    }

  private def handleUnreadableSpool(entityId: String, cause: Throwable): Future[RecoverySessionResult] =
    inspectForRecovery(entityId).flatMap { summary =>
      val view = sessionPort.toSessionView(summary)
      if (view.openedAt.isDefined && !view.isClosed) {
        abortAndCleanup(entityId, s"Recovery failed: ${cause.getMessage}")
      } else {
        cleanup(entityId).map(_ => RecoverySessionResult.Cleaned)
      }
    }

  private def resumeIfNeeded(
      entityId: String,
      view: SessionView[Device]
  ): Future[Summary] =
    if (!view.isAborted) {
      inspectForRecovery(entityId)
    } else {
      view.device match {
        case Some(device) =>
          given org.apache.pekko.util.Timeout = config.recovery.inspectTimeout
          sessionPort.register(
            entityId,
            device,
            view.deviceCorrelationId.getOrElse(""),
            view.objectHashHex,
            view.declaredPayloadSize
          )
        case None =>
          Future.failed(new IllegalStateException(s"Recovery cannot resume aborted entity $entityId: device info missing"))
      }
    }

  private def inspectForRecovery(entityId: String, retriesLeft: Int = 2): Future[Summary] = {
    given org.apache.pekko.util.Timeout = config.recovery.inspectTimeout
    sessionPort.inspect(entityId).recoverWith {
      case _: TimeoutException if retriesLeft > 0 =>
        after(1.second, scheduler) {
          inspectForRecovery(entityId, retriesLeft - 1)
        }
    }
  }

  private def resendMissingClaims(
      entityId: String,
      lastSpooledSeq: Long,
      recoveryReplyBinding: ReplyBinding
  ): Future[Long] = {
    given org.apache.pekko.util.Timeout = config.close.inspectTimeout
    sessionPort.inspect(entityId).flatMap { summary =>
      val view = sessionPort.toSessionView(summary)
      val contiguousClaimsCount = view.lastClaimSequence + 1L
      if (view.claimsCount != contiguousClaimsCount) {
        Future.failed(new IllegalStateException(
          s"Non-contiguous claim state for entity $entityId: claimsCount=${view.claimsCount}, lastClaimSequence=${view.lastClaimSequence}"
        ))
      } else {
        val firstMissing = contiguousClaimsCount
        if (firstMissing > lastSpooledSeq) {
          Future.successful(0L)
        } else {
          val batchSize = math.max(1, config.recovery.parallelism)
          (firstMissing to lastSpooledSeq).grouped(batchSize).foldLeft(Future.successful(0L)) { (accF, batch) =>
            accF.flatMap { acc =>
              Future.traverse(batch.toList) { seq =>
                spool.readChunk(entityId, seq).flatMap { bytes =>
                  Future.fromTry(claimPort.decodeEnvelope(bytes)).map { envelope =>
                    claimPort.dispatchClaim(entityId, envelope, bytes.length.toLong, recoveryReplyBinding)
                    1L
                  }
                }
              }.map(results => acc + results.sum)
            }
          }
        }
      }
    }
  }

  private def drainSpool(entityId: String, meta: SpoolMeta): Future[Unit] = {
    val flusher = flusherFactory.create(entityId, spool, meta.flushedSeq + 1L)
    flusher.start()
    flusher.drain().transformWith {
      case scala.util.Success(finalFlushedSeq) =>
        flusher.stop()
        if (finalFlushedSeq != meta.lastSpooledSeq) {
          Future.failed(new IllegalStateException(
            s"Flusher drain incomplete for entity $entityId: flushed=$finalFlushedSeq expected=${meta.lastSpooledSeq}"
          ))
        } else {
          Future.successful(())
        }
      case scala.util.Failure(ex) =>
        flusher.stop()
        Future.failed(ex)
    }
  }

  private def abortAndCleanup(entityId: String, reason: String): Future[RecoverySessionResult] = {
    given org.apache.pekko.util.Timeout = config.recovery.inspectTimeout
    sessionPort.abort(entityId, reason).recover { case NonFatal(ex) =>
      system.log.warn("Recovery abort failed for entity {}: {}", entityId, ex.getMessage)
      ()
    }.flatMap { _ =>
      cleanup(entityId).map(_ => RecoverySessionResult.Aborted)
    }
  }

  private def cleanup(entityId: String): Future[Unit] =
    spool.cleanup(entityId)

  private def validateMeta(entityId: String, meta: SpoolMeta): Option[String] =
    if (meta.entityId != entityId) {
      Some(s"Spool meta entityId mismatch for $entityId: meta=${meta.entityId}")
    } else if (meta.lastSpooledSeq < -1L) {
      Some(s"Invalid lastSpooledSeq=${meta.lastSpooledSeq} for entity $entityId")
    } else if (meta.flushedSeq < -1L) {
      Some(s"Invalid flushedSeq=${meta.flushedSeq} for entity $entityId")
    } else if (meta.flushedSeq > meta.lastSpooledSeq) {
      Some(s"Invalid spool watermark for entity $entityId: flushed=${meta.flushedSeq} spooled=${meta.lastSpooledSeq}")
    } else if (meta.totalSpooledBytes < 0L) {
      Some(s"Invalid totalSpooledBytes=${meta.totalSpooledBytes} for entity $entityId")
    } else if (meta.declaredPayloadSize <= 0L) {
      Some(s"Invalid declaredPayloadSize=${meta.declaredPayloadSize} for entity $entityId")
    } else if (meta.totalExpectedChunks <= 0L) {
      Some(s"Invalid totalExpectedChunks=${meta.totalExpectedChunks} for entity $entityId")
    } else {
      None
    }

  private def withRecoveryReplyBinding[T](body: ReplyBinding => Future[T]): Future[T] = {
    val replyBinding = claimPort.openReplyBinding(_ => (), ex => {
      system.log.debug("Recovery claim reply error: {}", ex.toString)
      ()
    })
    body(replyBinding).andThen { case _ =>
      claimPort.closeReplyBinding(replyBinding)
      ()
    }
  }

  private sealed trait RecoverySessionResult {
    def claimsResent: Long
  }

  private object RecoverySessionResult {
    case object Cleaned extends RecoverySessionResult {
      override val claimsResent: Long = 0L
    }

    case object Aborted extends RecoverySessionResult {
      override val claimsResent: Long = 0L
    }

    final case class Recovered(claimsResent: Long) extends RecoverySessionResult

    case object Failed extends RecoverySessionResult {
      override val claimsResent: Long = 0L
    }
  }
}
