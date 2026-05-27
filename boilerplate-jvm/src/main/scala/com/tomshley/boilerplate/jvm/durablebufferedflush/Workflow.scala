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

  def finalizeTransfer(
      entityId: String,
      binding: FlushConnectionBinding[ReplyBinding],
      lastSpooledSeq: Long
  ): Future[FlushFinalizationResult[ReplyBinding]]
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
