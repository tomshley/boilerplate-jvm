/*
 * Copyright 2026 Tomshley LLC.
 * All Rights Reserved.
 */

package com.tomshley.boilerplate.jvm.durablebufferedflush

import com.tomshley.boilerplate.jvm.durablebufferedflush.internal.WorkflowImpl
import org.apache.pekko.actor.typed.ActorSystem

import scala.concurrent.Future

trait Workflow[Device, Summary, Envelope, ReplyBinding] {
  def openBinding(): FlushConnectionBinding[ReplyBinding]

  def onConnectionClosed(
      binding: FlushConnectionBinding[ReplyBinding],
      entityIdOpt: Option[String],
      disconnectCause: Option[Throwable] = None
  ): Unit

  def isFinalizationInFlight(entityId: String): Future[Boolean]

  def prepareTransfer(
      binding: FlushConnectionBinding[ReplyBinding],
      descriptor: FlushTransferDescriptor[Device]
  ): Future[FlushPreparedTransfer[ReplyBinding]]

  def acceptChunk(
      entityId: String,
      binding: FlushConnectionBinding[ReplyBinding],
      envelope: Envelope,
      bytes: Array[Byte],
      zeroBasedSeq: Long,
      lastAcceptedSeq: Long
  ): Future[FlushAcceptedChunk[ReplyBinding]]

  /** Drain, verify content integrity, and close the transfer.
    *
    * The verification verdict is returned as a value on
    * [[FlushFinalizationResult.outcome]] — see [[FinalizeOutcome]]. The
    * caller's mismatch policy arrives as data via `mismatchDirective`:
    * [[HashMismatchDirective.CloseAnyway]] (default — observation only,
    * today's close semantics) or [[HashMismatchDirective.HoldOpen]]
    * (mismatch leaves the session open and the spool intact for an
    * explicit [[resetTransfer]] decision).
    */
  def finalizeTransfer(
      entityId: String,
      binding: FlushConnectionBinding[ReplyBinding],
      lastSpooledSeq: Long,
      mismatchDirective: HashMismatchDirective = HashMismatchDirective.CloseAnyway
  ): Future[FlushFinalizationResult[ReplyBinding]]

  /** Reset an entity's transfer so the next one starts from sequence
    * zero: abort the session entity and delete its spool. Reuses the
    * reconnect-rebuild machinery — the producer's next `prepareTransfer`
    * re-registers a fresh session instead of resume-skipping into the same
    * failing finalize. Intended for the caller's
    * [[HashMismatchDirective.HoldOpen]] arm; idempotent and safe to call
    * on an already-aborted or already-clean entity.
    */
  def resetTransfer(entityId: String, reason: String): Future[Unit]
}

object Workflow {

  /** Default factory. Backward-compatible six-argument shape — admission is
    * permanently open ([[AdmissionController.AlwaysOpen]]) and pressure
    * config is the [[SpoolPressureConfig.Disabled]] sentinel. Existing
    * call sites that wired a workflow before the pressure architecture
    * existed continue to compile unchanged. */
  def apply[Device, Summary, Envelope, ReplyBinding](
      spool: ChunkSpool,
      flusherFactory: ChunkFlusherFactory,
      sessionPort: SessionPort[Device, Summary],
      claimPort: ClaimPort[Envelope, ReplyBinding],
      config: FlushConfig,
      system: ActorSystem[?]
  ): Workflow[Device, Summary, Envelope, ReplyBinding] =
    new WorkflowImpl(
      spool = spool,
      flusherFactory = flusherFactory,
      sessionPort = sessionPort,
      claimPort = claimPort,
      config = config,
      system = system
    )

  /** Pressure-aware factory. The supplied [[AdmissionController]] is read
    * once per session in [[Workflow.prepareTransfer]]; when closed, the
    * call fails with [[SpoolPressureCriticalException]] carrying
    * `pressureConfig.suggestedRetryAfter`. The hot path
    * ([[Workflow.acceptChunk]]) is intentionally never gated — see
    * [[AdmissionController]] for the rationale. */
  def apply[Device, Summary, Envelope, ReplyBinding](
      spool: ChunkSpool,
      flusherFactory: ChunkFlusherFactory,
      sessionPort: SessionPort[Device, Summary],
      claimPort: ClaimPort[Envelope, ReplyBinding],
      config: FlushConfig,
      admissionController: AdmissionController,
      pressureConfig: SpoolPressureConfig,
      system: ActorSystem[?]
  ): Workflow[Device, Summary, Envelope, ReplyBinding] =
    new WorkflowImpl(
      spool = spool,
      flusherFactory = flusherFactory,
      sessionPort = sessionPort,
      claimPort = claimPort,
      config = config,
      system = system,
      admissionController = admissionController,
      pressureConfig = pressureConfig
    )
}
