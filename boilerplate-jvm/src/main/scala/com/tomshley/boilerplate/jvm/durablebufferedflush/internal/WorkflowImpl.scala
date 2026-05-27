/*
 * Copyright 2026 Tomshley LLC.
 * All Rights Reserved.
 */

package com.tomshley.boilerplate.jvm.durablebufferedflush.internal

import com.tomshley.boilerplate.jvm.durablebufferedflush.*
import org.apache.pekko.actor.typed.{ActorRef, ActorSystem, Behavior, Scheduler}
import org.apache.pekko.actor.typed.scaladsl.AskPattern.*
import org.apache.pekko.actor.typed.scaladsl.Behaviors
import org.apache.pekko.pattern.after

import java.util.UUID
import scala.concurrent.{ExecutionContext, Future, TimeoutException}
import scala.concurrent.duration.*
import scala.util.control.NonFatal

private object FinalizationGate {
  sealed trait Command

  final case class Acquire(entityId: String, replyTo: ActorRef[Boolean]) extends Command

  final case class IsInFlight(entityId: String, replyTo: ActorRef[Boolean]) extends Command

  final case class Release(entityId: String) extends Command

  def apply(): Behavior[Command] =
    active(Set.empty)

  private def active(inFlight: Set[String]): Behavior[Command] =
    Behaviors.receiveMessage {
      case Acquire(entityId, replyTo) =>
        if (inFlight.contains(entityId)) {
          replyTo ! false
          Behaviors.same
        } else {
          replyTo ! true
          active(inFlight + entityId)
        }
      case IsInFlight(entityId, replyTo) =>
        replyTo ! inFlight.contains(entityId)
        Behaviors.same
      case Release(entityId) =>
        active(inFlight - entityId)
    }
}

final class WorkflowImpl[Device, Summary, Envelope, ReplyBinding](
    spool: ChunkSpool,
    flusherFactory: ChunkFlusherFactory,
    sessionPort: SessionPort[Device, Summary],
    claimPort: ClaimPort[Envelope, ReplyBinding],
    config: FlushConfig,
    system: ActorSystem[?],
    admissionController: AdmissionController = AdmissionController.AlwaysOpen,
    pressureConfig: SpoolPressureConfig = SpoolPressureConfig.Disabled
) extends Workflow[Device, Summary, Envelope, ReplyBinding] {

  private given ActorSystem[?] = system
  private given ExecutionContext = system.executionContext
  private given Scheduler = system.scheduler

  private val scheduler = system.classicSystem.scheduler
  private val finalizationGate = system.systemActorOf(
    FinalizationGate(),
    s"durablebufferedflush-finalization-gate-${UUID.randomUUID()}"
  )
  private val closeBarrier = new CloseBarrier(
    spool = spool,
    sessionPort = sessionPort,
    claimPort = claimPort,
    config = config,
    system = system
  )

  private def observeKernelFuture(op: String, future: Future[?]): Unit =
    future.failed.foreach(ex =>
      system.log.warn("Flush kernel async op {} failed: {}", op, ex.getMessage)
    )

  def openBinding(): FlushConnectionBinding[ReplyBinding] = {
    val lagMonitor = ClaimLagMonitor(
      claimLagSoft = config.backpressure.claimLagSoft,
      claimLagHard = config.backpressure.claimLagHard
    )
    val replyBinding = claimPort.openReplyBinding(
      onConfirmedClaimsCount = { claimsCount =>
        observeKernelFuture(s"claim-confirmed-$claimsCount", lagMonitor.onClaimConfirmed(claimsCount))
      },
      onRejected = { _ =>
        observeKernelFuture("claim-rejected", lagMonitor.onClaimError())
      }
    )
    FlushConnectionBinding(lagMonitor = lagMonitor, replyBinding = replyBinding)
  }

  def onConnectionClosed(
      binding: FlushConnectionBinding[ReplyBinding],
      entityIdOpt: Option[String],
      disconnectCause: Option[Throwable] = None
  ): Unit = {
    binding.flusher.foreach(_.stop())
    entityIdOpt.foreach(_ => claimPort.clearEntityId(binding.replyBinding))
    observeKernelFuture(
      s"connection-closed-${entityIdOpt.getOrElse("<unbound>")}",
      binding.lagMonitor.cancelPause(
        FlushPauseCancelled(entityIdOpt.getOrElse("<unbound>"), disconnectCause)
      )
    )
    binding.lagMonitor.stop()
    claimPort.closeReplyBinding(binding.replyBinding)
  }

  def isFinalizationInFlight(entityId: String): Future[Boolean] = {
    given org.apache.pekko.util.Timeout = config.close.askTimeout
    finalizationGate.ask(replyTo => FinalizationGate.IsInFlight(entityId, replyTo))
  }

  def prepareTransfer(
      binding: FlushConnectionBinding[ReplyBinding],
      descriptor: FlushTransferDescriptor[Device]
  ): Future[FlushPreparedTransfer[ReplyBinding]] =
    // Admission gate is queried once per session at admission time. The
    // hot path (`acceptChunk`) is intentionally never gated — a session
    // that has already been admitted continues to flow even if the
    // controller closes mid-session, because closing a stream that has
    // already paid the fixed cost of admission would sacrifice durable
    // in-flight bytes for a small reduction in pressure.
    admissionController.isOpen().flatMap { gateOpen =>
      if (!gateOpen)
        Future.failed(SpoolPressureCriticalException(pressureConfig.suggestedRetryAfter))
      else {
        given org.apache.pekko.util.Timeout = config.recovery.inspectTimeout
        for {
          existingSummary <- sessionPort.inspect(descriptor.entityId)
          existingView = sessionPort.toSessionView(existingSummary)
          result <- if (existingView.isClosed)
                      prepareClosedSession(binding, descriptor)
                    else
                      prepareOpenSession(binding, descriptor, existingView)
        } yield result
      }
    }

  private def prepareClosedSession(
      binding: FlushConnectionBinding[ReplyBinding],
      descriptor: FlushTransferDescriptor[Device]
  ): Future[FlushPreparedTransfer[ReplyBinding]] =
    for {
      maybeMeta <- spool.readMeta(descriptor.entityId)
      _         <- maybeMeta.fold(Future.successful(()))(_ => spool.cleanup(descriptor.entityId))
      _          = claimPort.bindEntityId(binding.replyBinding, descriptor.entityId)
      _         <- binding.lagMonitor.reset()
    } yield FlushPreparedTransfer(
      binding = binding.copy(flusher = None),
      receivedChunks = descriptor.totalExpectedChunks,
      lastAcceptedSeq = descriptor.totalExpectedChunks - 1L,
      isComplete = true
    )

  private def prepareOpenSession(
      binding: FlushConnectionBinding[ReplyBinding],
      descriptor: FlushTransferDescriptor[Device],
      existingView: SessionView[Device]
  ): Future[FlushPreparedTransfer[ReplyBinding]] = {
    given org.apache.pekko.util.Timeout = config.recovery.inspectTimeout
    for {
      rawMeta       <- spool.readMeta(descriptor.entityId)
      cleanedMeta   <- cleanMetaForUnregistered(descriptor.entityId, existingView, rawMeta)
      _             <- abortIfSpoolLost(descriptor.entityId, existingView, cleanedMeta)
      validatedMeta <- sanitizeReconnectMeta(descriptor.entityId, cleanedMeta)
      _             <- sessionPort.register(
                         descriptor.entityId, descriptor.device,
                         descriptor.deviceCorrelationId, descriptor.objectHashHex,
                         descriptor.declaredPayloadSize
                       )
      meta          <- resolveSpoolMeta(descriptor, validatedMeta)
      prepared      <- startFlusherAndSeedLag(binding, descriptor, meta)
    } yield prepared
  }

  def acceptChunk(
      entityId: String,
      binding: FlushConnectionBinding[ReplyBinding],
      envelope: Envelope,
      bytes: Array[Byte],
      zeroBasedSeq: Long,
      lastAcceptedSeq: Long
  ): Future[FlushAcceptedChunk[ReplyBinding]] = {
    if (zeroBasedSeq <= lastAcceptedSeq) {
      Future.successful(FlushAcceptedChunk(binding))
    } else if (zeroBasedSeq != lastAcceptedSeq + 1L) {
      Future.failed(new IllegalStateException(
        s"Chunk sequence mismatch for entity $entityId: expected next seq ${lastAcceptedSeq + 1L}, got $zeroBasedSeq"
      ))
    } else {
      spool.write(entityId, zeroBasedSeq, bytes).flatMap { _ =>
        binding.lagMonitor.onSpooled(zeroBasedSeq).flatMap { _ =>
          val claimResult =
            try {
              claimPort.dispatchClaim(entityId, envelope, bytes.length.toLong, binding.replyBinding)
              binding.lagMonitor.onClaimAttempted(zeroBasedSeq)
            } catch {
              case NonFatal(ex) =>
                system.log.warn("Claim dispatch failed for entity {} seq {}: {}", entityId, zeroBasedSeq, ex.getMessage)
                binding.lagMonitor.onClaimError()
            }

          claimResult.flatMap { _ =>
            binding.lagMonitor.pauseIfNeeded().flatMap {
              case Some(pauseFuture) =>
                val timeoutFuture = after(config.backpressure.pauseTimeout, scheduler) {
                  Future.failed(new TimeoutException(
                    s"Backpressure pause timeout (${config.backpressure.pauseTimeout}) for entity $entityId"
                  ))
                }
                Future.firstCompletedOf(Seq(pauseFuture, timeoutFuture)).map { _ =>
                  FlushAcceptedChunk(binding)
                }.recoverWith {
                  case ex: TimeoutException =>
                    binding.lagMonitor.cancelPause(ex).flatMap { _ =>
                      Future.failed(FlushPauseTimedOut(entityId, config.backpressure.pauseTimeout))
                    }
                  case NonFatal(ex) =>
                    Future.failed(ex)
                }
              case None =>
                Future.successful(FlushAcceptedChunk(binding))
            }
          }
        }
      }
    }
  }

  def finalizeTransfer(
      entityId: String,
      binding: FlushConnectionBinding[ReplyBinding],
      lastSpooledSeq: Long
  ): Future[FlushFinalizationResult[ReplyBinding]] = {
    binding.flusher match {
      case None =>
        Future.failed(new IllegalStateException(s"No chunk flusher bound for entity $entityId"))
      case Some(flusher) =>
        withFinalizationFence(entityId) {
          val happy = for {
            finalFlushedSeq <- flusher.drain()
            _ <- if (finalFlushedSeq != lastSpooledSeq)
                   Future.failed(new IllegalStateException(
                     s"Flusher drain incomplete for entity $entityId: flushed=$finalFlushedSeq expected=$lastSpooledSeq"
                   ))
                 else Future.successful(())
            maybeMeta <- spool.readMeta(entityId)
            meta <- maybeMeta match {
              case None       => Future.failed(new IllegalStateException(s"Spool meta missing after drain for entity $entityId"))
              case Some(meta) => Future.successful(meta)
            }
            _ <- closeBarrier.closeWithRetry(
                   entityId = entityId,
                   lastSpooledSeq = lastSpooledSeq,
                   expectedClaimsCount = meta.totalExpectedChunks,
                   expectedTotalBytes = meta.totalSpooledBytes,
                   expectedLastSequence = lastSpooledSeq,
                   replyBinding = binding.replyBinding
                 )
            _ <- spool.cleanup(entityId).recover { case NonFatal(ex) =>
              system.log.warn("Spool cleanup failed for entity {} after close: {}", entityId, ex.getMessage)
            }
            _ = flusher.stop()
            _ = claimPort.clearEntityId(binding.replyBinding)
            _ <- binding.lagMonitor.reset()
          } yield FlushFinalizationResult(binding.copy(flusher = None))

          happy.recoverWith { case NonFatal(ex) =>
            cleanupAfterFailure(entityId, binding, flusher, ex)
          }
        }
    }
  }

  private def cleanupAfterFailure(
      entityId: String,
      binding: FlushConnectionBinding[ReplyBinding],
      flusher: ChunkFlusher,
      cause: Throwable
  ): Future[Nothing] = {
    flusher.stop()
    system.log.error("Final spool drain/close failed for entity {}: {} — aborting session", entityId, cause.getMessage)
    given org.apache.pekko.util.Timeout = config.close.askTimeout
    for {
      _ <- sessionPort.abort(entityId, s"Close barrier failed: ${cause.getMessage}").recover { case NonFatal(abortEx) =>
        system.log.warn("Post-failure abort also failed for entity {}: {}", entityId, abortEx.getMessage)
      }
      _ <- spool.cleanup(entityId).recover { case NonFatal(cleanupEx) =>
        system.log.warn("Spool cleanup failed for entity {} after abort: {}", entityId, cleanupEx.getMessage)
      }
      _ = claimPort.clearEntityId(binding.replyBinding)
      _ <- Future.firstCompletedOf(Seq(
        binding.lagMonitor.reset(),
        after(3.seconds, scheduler)(Future.successful(()))
      ))
      result <- Future.failed[Nothing](cause)
    } yield result
  }

  private def cleanMetaForUnregistered(
      entityId: String,
      view: SessionView[Device],
      meta: Option[SpoolMeta]
  ): Future[Option[SpoolMeta]] =
    if (view.openedAt.isEmpty && meta.isDefined)
      spool.cleanup(entityId).map(_ => None)
    else
      Future.successful(meta)

  private def abortIfSpoolLost(
      entityId: String,
      view: SessionView[Device],
      meta: Option[SpoolMeta]
  ): Future[Unit] =
    if (meta.isEmpty && view.openedAt.isDefined && view.claimsCount > 0L && !view.isAborted)
      abortAndCleanup(entityId, "Spool missing on reconnect; rebuilding from client retry")
    else
      Future.successful(())

  private def resolveSpoolMeta(
      descriptor: FlushTransferDescriptor[Device],
      meta: Option[SpoolMeta]
  ): Future[SpoolMeta] = meta match {
    case Some(m)
        if m.declaredPayloadSize == descriptor.declaredPayloadSize &&
          m.totalExpectedChunks == descriptor.totalExpectedChunks &&
          m.objectHashHex == descriptor.objectHashHex =>
      Future.successful(m)
    case Some(m) =>
      rebuildReconnectSpool(
        descriptor,
        s"Spool metadata mismatch on reconnect; rebuilding from client retry (declaredPayloadSize=${m.declaredPayloadSize}, totalExpectedChunks=${m.totalExpectedChunks})"
      )
    case None =>
      spool.initialize(
        descriptor.entityId,
        SpoolMeta.initial(
          entityId = descriptor.entityId,
          deviceId = descriptor.deviceId,
          objectHashHex = descriptor.objectHashHex,
          declaredPayloadSize = descriptor.declaredPayloadSize,
          totalExpectedChunks = descriptor.totalExpectedChunks
        )
      )
  }

  private def startFlusherAndSeedLag(
      binding: FlushConnectionBinding[ReplyBinding],
      descriptor: FlushTransferDescriptor[Device],
      meta: SpoolMeta
  ): Future[FlushPreparedTransfer[ReplyBinding]] = {
    binding.flusher.foreach(_.stop())
    val flusher = flusherFactory.create(descriptor.entityId, spool, meta.flushedSeq + 1L)
    flusher.start()
    claimPort.bindEntityId(binding.replyBinding, descriptor.entityId)
    for {
      _ <- binding.lagMonitor.reset()
      _ <- if (meta.lastSpooledSeq >= 0L)
             for {
               _ <- binding.lagMonitor.onSpooled(meta.lastSpooledSeq)
               _ <- binding.lagMonitor.onClaimAttempted(meta.lastSpooledSeq)
               _ <- binding.lagMonitor.onClaimConfirmed(meta.lastSpooledSeq + 1L)
             } yield ()
           else
             Future.successful(())
    } yield FlushPreparedTransfer(
      binding = binding.copy(flusher = Some(flusher)),
      receivedChunks = math.max(0L, meta.lastSpooledSeq + 1L),
      lastAcceptedSeq = meta.lastSpooledSeq,
      isComplete = meta.isComplete
    )
  }

  private def sanitizeReconnectMeta(
      entityId: String,
      existingMeta: Option[SpoolMeta]
  ): Future[Option[SpoolMeta]] =
    existingMeta match {
      case Some(meta) =>
        hasUnexpectedNextChunk(entityId, meta).flatMap {
          case true =>
            abortAndCleanup(entityId, "Spool skew on reconnect; rebuilding from client retry").map(_ => None)
          case false => Future.successful(Some(meta))
        }
      case None => Future.successful(None)
    }

  private def abortAndCleanup(entityId: String, reason: String): Future[Unit] = {
    given org.apache.pekko.util.Timeout = config.close.askTimeout
    sessionPort.abort(entityId, reason)
      .recover { case NonFatal(ex) =>
        system.log.warn("Reconnect/recovery abort failed for entity {}: {}", entityId, ex.getMessage)
        ()
      }
      .flatMap { _ =>
        spool.cleanup(entityId).recover { case NonFatal(ex) =>
          system.log.warn("Reconnect/recovery cleanup failed for entity {}: {}", entityId, ex.getMessage)
          ()
        }
      }
  }

  private def rebuildReconnectSpool(
      descriptor: FlushTransferDescriptor[Device],
      reason: String
  ): Future[SpoolMeta] =
    abortAndCleanup(descriptor.entityId, reason).flatMap { _ =>
      given org.apache.pekko.util.Timeout = config.recovery.inspectTimeout
      sessionPort.register(
        descriptor.entityId,
        descriptor.device,
        descriptor.deviceCorrelationId,
        descriptor.objectHashHex,
        descriptor.declaredPayloadSize
      ).flatMap { _ =>
        spool.initialize(
          descriptor.entityId,
          SpoolMeta.initial(
            entityId = descriptor.entityId,
            deviceId = descriptor.deviceId,
            objectHashHex = descriptor.objectHashHex,
            declaredPayloadSize = descriptor.declaredPayloadSize,
            totalExpectedChunks = descriptor.totalExpectedChunks
          )
        )
      }
    }

  private def hasUnexpectedNextChunk(entityId: String, meta: SpoolMeta): Future[Boolean] = {
    val nextSeq = meta.lastSpooledSeq + 1L
    spool.readChunk(entityId, nextSeq).map(_ => true).recover {
      case _: java.nio.file.NoSuchFileException => false
      case _: java.io.FileNotFoundException => false
    }
  }

  private def withFinalizationFence[A](entityId: String)(body: => Future[A]): Future[A] = {
    given org.apache.pekko.util.Timeout = config.close.askTimeout
    finalizationGate.ask(replyTo => FinalizationGate.Acquire(entityId, replyTo)).flatMap {
      case false =>
        Future.failed(new IllegalStateException(s"Entity $entityId is already finalizing or cleaning up"))
      case true =>
        body.andThen { case _ =>
          finalizationGate ! FinalizationGate.Release(entityId)
          ()
        }
    }
  }

}
