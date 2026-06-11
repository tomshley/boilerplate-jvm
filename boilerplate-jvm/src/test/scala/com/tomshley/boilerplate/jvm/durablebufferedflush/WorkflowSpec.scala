/*
 * Copyright 2026 Tomshley LLC.
 * All Rights Reserved.
 */

package com.tomshley.boilerplate.jvm.durablebufferedflush

import org.apache.pekko.actor.testkit.typed.scaladsl.ActorTestKit
import org.apache.pekko.actor.typed.ActorSystem
import org.apache.pekko.util.Timeout
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.{Eventually, ScalaFutures}
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{Millis, Seconds, Span}
import org.scalatest.wordspec.AnyWordSpec

import java.time.Instant
import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.concurrent.duration.*
import scala.util.Try

final class DefaultFlushWorkflowSpec
    extends AnyWordSpec
    with Matchers
    with BeforeAndAfterAll
    with ScalaFutures
    with Eventually {

  override implicit val patienceConfig: PatienceConfig =
    PatienceConfig(timeout = Span(5, Seconds), interval = Span(50, Millis))

  private val testKit = ActorTestKit("DefaultFlushWorkflowSpec")
  private given ActorSystem[?] = testKit.system
  private given ExecutionContext = testKit.system.executionContext

  override def afterAll(): Unit = {
    testKit.shutdownTestKit()
    super.afterAll()
  }

  private def awaitLagOp(future: Future[Unit]): Unit =
    future.futureValue shouldBe ()

  private def lagSnapshot(monitor: ClaimLagMonitor): ClaimLagSnapshot =
    monitor.snapshot().futureValue

  private def activePause(monitor: ClaimLagMonitor): Future[Unit] =
    monitor.enterPause().futureValue.getOrElse(fail("expected active pause"))

  "DefaultFlushWorkflow" should {

    "fail fast on invalid backpressure config when opening a binding" in {
      val workflow = newWorkflow(config = makeConfig(claimLagSoft = 2L, claimLagHard = 2L))

      an[IllegalArgumentException] should be thrownBy workflow.openBinding()
    }

    "wire reply callbacks into the lag monitor when opening a binding" in {
      val claimPort = new RecordingClaimPort
      val workflow = newWorkflow(claimPort = claimPort)

      val binding = workflow.openBinding()
      awaitLagOp(binding.lagMonitor.onSpooled(4L))
      awaitLagOp(binding.lagMonitor.onClaimAttempted(4L))

      claimPort.confirmClaimsCount(5L)
      eventually {
        lagSnapshot(binding.lagMonitor).lastClaimConfirmedSeq shouldBe 4L
        lagSnapshot(binding.lagMonitor).claimLag shouldBe 0L
        lagSnapshot(binding.lagMonitor).inflightClaims shouldBe 0L
      }

      claimPort.reject(new RuntimeException("boom"))
      eventually {
        lagSnapshot(binding.lagMonitor).claimErrorCount shouldBe 1L
      }
    }

    "prepareTransfer return a completed transfer for an already closed session" in {
      val descriptor = transferDescriptor(totalExpectedChunks = 4L)
      val spool = new RecordingChunkSpool
      spool.seedMeta(descriptor.entityId, spoolMetaFor(descriptor, lastSpooledSeq = 3L, flushedSeq = 3L, totalSpooledBytes = 128L))
      val sessionPort = new RecordingSessionPort
      sessionPort.inspectHandler = _ => Future.successful(
        sessionSummaryFor(
          descriptor,
          isClosed = true,
          claimsCount = descriptor.totalExpectedChunks,
          lastClaimSequence = descriptor.totalExpectedChunks - 1L
        )
      )
      val claimPort = new RecordingClaimPort
      val workflow = newWorkflow(spool = spool, sessionPort = sessionPort, claimPort = claimPort)

      val binding = workflow.openBinding()
      val prepared = workflow.prepareTransfer(binding, descriptor).futureValue

      prepared.receivedChunks shouldBe 4L
      prepared.lastAcceptedSeq shouldBe 3L
      prepared.isComplete shouldBe true
      prepared.binding.flusher shouldBe None
      spool.cleanupCalls should contain only descriptor.entityId
      sessionPort.registerCalls shouldBe empty
      prepared.binding.replyBinding.entityId shouldBe descriptor.entityId
    }

    "prepareTransfer initialize a fresh spool and start a flusher when no prior meta exists" in {
      val descriptor = transferDescriptor(totalExpectedChunks = 3L)
      val spool = new RecordingChunkSpool
      val flusher = new RecordingChunkFlusher(descriptor.entityId)
      val flusherFactory = new RecordingChunkFlusherFactory
      flusherFactory.enqueue(flusher)
      val sessionPort = new RecordingSessionPort
      sessionPort.inspectHandler = _ => Future.successful(sessionSummaryFor(descriptor))
      val claimPort = new RecordingClaimPort
      val workflow = newWorkflow(
        spool = spool,
        flusherFactory = flusherFactory,
        sessionPort = sessionPort,
        claimPort = claimPort
      )

      val binding = workflow.openBinding()
      val prepared = workflow.prepareTransfer(binding, descriptor).futureValue

      spool.initializeCalls should have size 1
      flusherFactory.created should have size 1
      flusherFactory.created.head.startSeq shouldBe 0L
      flusher.startCount shouldBe 1
      sessionPort.registerCalls should have size 1
      prepared.receivedChunks shouldBe 0L
      prepared.lastAcceptedSeq shouldBe -1L
      prepared.isComplete shouldBe false
      prepared.binding.flusher.getOrElse(fail("expected flusher for fresh spool")) shouldBe flusher
      prepared.binding.replyBinding.entityId shouldBe descriptor.entityId
    }

    "prepareTransfer reuse matching reconnect meta and seed the lag monitor from the spool watermark" in {
      val descriptor = transferDescriptor(totalExpectedChunks = 5L)
      val meta = spoolMetaFor(descriptor, lastSpooledSeq = 2L, flushedSeq = 1L, totalSpooledBytes = 96L)
      val spool = new RecordingChunkSpool
      spool.seedMeta(descriptor.entityId, meta)
      val flusher = new RecordingChunkFlusher(descriptor.entityId)
      val flusherFactory = new RecordingChunkFlusherFactory
      flusherFactory.enqueue(flusher)
      val sessionPort = new RecordingSessionPort
      sessionPort.inspectHandler = _ => Future.successful(
        sessionSummaryFor(descriptor, claimsCount = 3L, lastClaimSequence = 2L)
      )
      val claimPort = new RecordingClaimPort
      val workflow = newWorkflow(
        spool = spool,
        flusherFactory = flusherFactory,
        sessionPort = sessionPort,
        claimPort = claimPort
      )

      val binding = workflow.openBinding()
      val prepared = workflow.prepareTransfer(binding, descriptor).futureValue

      prepared.receivedChunks shouldBe 3L
      prepared.lastAcceptedSeq shouldBe 2L
      prepared.binding.flusher.getOrElse(fail("expected flusher for reconnect resume")) shouldBe flusher
      flusherFactory.created.head.startSeq shouldBe 2L
      lagSnapshot(prepared.binding.lagMonitor).spooledSeq shouldBe 2L
      lagSnapshot(prepared.binding.lagMonitor).lastClaimAttemptedSeq shouldBe 2L
      lagSnapshot(prepared.binding.lagMonitor).lastClaimConfirmedSeq shouldBe 2L
    }

    "prepareTransfer rebuild from client retry when the actor is open but the spool is missing" in {
      val descriptor = transferDescriptor(totalExpectedChunks = 4L)
      val spool = new RecordingChunkSpool
      val flusher = new RecordingChunkFlusher(descriptor.entityId)
      val flusherFactory = new RecordingChunkFlusherFactory
      flusherFactory.enqueue(flusher)
      val sessionPort = new RecordingSessionPort
      sessionPort.inspectHandler = _ => Future.successful(
        sessionSummaryFor(descriptor, claimsCount = 2L, lastClaimSequence = 1L)
      )
      val claimPort = new RecordingClaimPort
      val workflow = newWorkflow(
        spool = spool,
        flusherFactory = flusherFactory,
        sessionPort = sessionPort,
        claimPort = claimPort
      )

      val binding = workflow.openBinding()
      val prepared = workflow.prepareTransfer(binding, descriptor).futureValue

      sessionPort.abortCalls should have size 1
      sessionPort.abortCalls.head.reason should include("Spool missing on reconnect")
      sessionPort.registerCalls should have size 1
      spool.cleanupCalls should contain(descriptor.entityId)
      spool.initializeCalls should have size 1
      prepared.receivedChunks shouldBe 0L
      prepared.lastAcceptedSeq shouldBe -1L
      prepared.binding.flusher.getOrElse(fail("expected flusher after spool rebuild")) shouldBe flusher
    }

    "prepareTransfer rebuild reconnect state when spool metadata mismatches the new transfer descriptor" in {
      val descriptor = transferDescriptor(totalExpectedChunks = 4L, declaredPayloadSize = 2048L)
      val mismatchedMeta = spoolMetaFor(descriptor, lastSpooledSeq = 1L, flushedSeq = 0L, totalSpooledBytes = 64L)
        .copy(declaredPayloadSize = 1024L)
      val spool = new RecordingChunkSpool
      spool.seedMeta(descriptor.entityId, mismatchedMeta)
      val flusher = new RecordingChunkFlusher(descriptor.entityId)
      val flusherFactory = new RecordingChunkFlusherFactory
      flusherFactory.enqueue(flusher)
      val sessionPort = new RecordingSessionPort
      sessionPort.inspectHandler = _ => Future.successful(sessionSummaryFor(descriptor, claimsCount = 2L, lastClaimSequence = 1L))
      val claimPort = new RecordingClaimPort
      val workflow = newWorkflow(
        spool = spool,
        flusherFactory = flusherFactory,
        sessionPort = sessionPort,
        claimPort = claimPort
      )

      val binding = workflow.openBinding()
      val prepared = workflow.prepareTransfer(binding, descriptor).futureValue

      sessionPort.abortCalls should have size 1
      sessionPort.abortCalls.head.reason should include("Spool metadata mismatch on reconnect")
      sessionPort.registerCalls should have size 2
      spool.cleanupCalls should contain(descriptor.entityId)
      spool.initializeCalls should have size 1
      prepared.receivedChunks shouldBe 0L
      prepared.lastAcceptedSeq shouldBe -1L
      prepared.binding.flusher.getOrElse(fail("expected flusher after metadata mismatch rebuild")) shouldBe flusher
    }

    "acceptChunk write and dispatch the next contiguous chunk on the happy path" in {
      val spool = new RecordingChunkSpool
      val claimPort = new RecordingClaimPort
      val workflow = newWorkflow(spool = spool, claimPort = claimPort)

      val binding = workflow.openBinding()
      val bytes = "payload-1".getBytes()
      val result = workflow.acceptChunk("entity-1", binding, "env-1", bytes, zeroBasedSeq = 0L, lastAcceptedSeq = -1L).futureValue

      result.binding shouldBe binding
      spool.writeCalls should have size 1
      spool.chunkBytes("entity-1", 0L).sameElements(bytes) shouldBe true
      claimPort.dispatchCalls should have size 1
      lagSnapshot(binding.lagMonitor).spooledSeq shouldBe 0L
      lagSnapshot(binding.lagMonitor).lastClaimAttemptedSeq shouldBe 0L
      lagSnapshot(binding.lagMonitor).claimErrorCount shouldBe 0L
    }

    "acceptChunk wait on an already-active pause instead of bypassing it" in {
      val spool = new RecordingChunkSpool
      val claimPort = new RecordingClaimPort
      val workflow = newWorkflow(
        spool = spool,
        claimPort = claimPort,
        config = makeConfig(claimLagSoft = 1L, claimLagHard = 2L, pauseTimeout = 500.millis)
      )

      val binding = workflow.openBinding()
      awaitLagOp(binding.lagMonitor.onSpooled(2L))
      awaitLagOp(binding.lagMonitor.onClaimAttempted(2L))
      val initialPause = activePause(binding.lagMonitor)

      initialPause.isCompleted shouldBe false

      val acceptFuture = workflow.acceptChunk(
        entityId = "entity-1",
        binding = binding,
        envelope = "env-3",
        bytes = "payload-3".getBytes(),
        zeroBasedSeq = 2L,
        lastAcceptedSeq = 1L
      )

      acceptFuture.isCompleted shouldBe false

      claimPort.confirmClaimsCount(3L)
      initialPause.futureValue shouldBe ()
      acceptFuture.futureValue.binding shouldBe binding
      binding.lagMonitor.isPaused.futureValue shouldBe false
    }

    "prepareTransfer reset a stale active pause before reusing the binding" in {
      val descriptor = transferDescriptor(totalExpectedChunks = 3L)
      val spool = new RecordingChunkSpool
      val flusher = new RecordingChunkFlusher(descriptor.entityId)
      val flusherFactory = new RecordingChunkFlusherFactory
      flusherFactory.enqueue(flusher)
      val sessionPort = new RecordingSessionPort
      sessionPort.inspectHandler = _ => Future.successful(sessionSummaryFor(descriptor))
      val claimPort = new RecordingClaimPort
      val workflow = newWorkflow(
        spool = spool,
        flusherFactory = flusherFactory,
        sessionPort = sessionPort,
        claimPort = claimPort
      )

      val binding = workflow.openBinding()
      awaitLagOp(binding.lagMonitor.onSpooled(4L))
      awaitLagOp(binding.lagMonitor.onClaimAttempted(4L))
      val stalePause = activePause(binding.lagMonitor)

      stalePause.isCompleted shouldBe false

      val prepared = workflow.prepareTransfer(binding, descriptor).futureValue

      stalePause.futureValue shouldBe ()
      prepared.binding.lagMonitor.isPaused.futureValue shouldBe false
      prepared.receivedChunks shouldBe 0L
    }

    "acceptChunk acknowledge duplicate chunks without writing or dispatching again" in {
      val spool = new RecordingChunkSpool
      val claimPort = new RecordingClaimPort
      val workflow = newWorkflow(spool = spool, claimPort = claimPort)

      val binding = workflow.openBinding()
      val result = workflow.acceptChunk("entity-1", binding, "env-1", "payload".getBytes(), zeroBasedSeq = 0L, lastAcceptedSeq = 0L).futureValue

      result.binding shouldBe binding
      spool.writeCalls shouldBe empty
      claimPort.dispatchCalls shouldBe empty
    }

    "acceptChunk fail on a non-contiguous chunk sequence mismatch" in {
      val spool = new RecordingChunkSpool
      val claimPort = new RecordingClaimPort
      val workflow = newWorkflow(spool = spool, claimPort = claimPort)

      val binding = workflow.openBinding()
      val ex = workflow.acceptChunk("entity-1", binding, "env-3", "payload".getBytes(), zeroBasedSeq = 2L, lastAcceptedSeq = 0L).failed.futureValue

      ex shouldBe a[IllegalStateException]
      ex.getMessage should include("expected next seq 1")
      spool.writeCalls shouldBe empty
      claimPort.dispatchCalls shouldBe empty
    }

    "acceptChunk fail with a pause timeout and clear the pause state" in {
      val spool = new RecordingChunkSpool
      val claimPort = new RecordingClaimPort
      val workflow = newWorkflow(
        spool = spool,
        claimPort = claimPort,
        config = makeConfig(claimLagSoft = 1L, claimLagHard = 2L, pauseTimeout = 20.millis)
      )

      val binding = workflow.openBinding()
      val ex = workflow.acceptChunk(
        entityId = "entity-1",
        binding = binding,
        envelope = "env-3",
        bytes = "payload-3".getBytes(),
        zeroBasedSeq = 2L,
        lastAcceptedSeq = 1L
      ).failed.futureValue

      ex shouldBe FlushPauseTimedOut("entity-1", 20.millis)
      binding.lagMonitor.isPaused.futureValue shouldBe false
      spool.writeCalls should have size 1
      claimPort.dispatchCalls should have size 1
    }

    "finalizeTransfer drain close cleanup and clear the binding on success" in {
      val descriptor = transferDescriptor(totalExpectedChunks = 4L)
      val meta = spoolMetaFor(descriptor, lastSpooledSeq = 3L, flushedSeq = 3L, totalSpooledBytes = 192L)
      val spool = new RecordingChunkSpool
      spool.seedMeta(descriptor.entityId, meta)
      val sessionPort = new RecordingSessionPort
      val claimPort = new RecordingClaimPort
      val workflow = newWorkflow(spool = spool, sessionPort = sessionPort, claimPort = claimPort)
      val flusher = new RecordingChunkFlusher(descriptor.entityId)
      flusher.drainResult = Future.successful(3L)

      val openedBinding = workflow.openBinding()
      claimPort.bindEntityId(openedBinding.replyBinding, descriptor.entityId)
      awaitLagOp(openedBinding.lagMonitor.onSpooled(3L))
      awaitLagOp(openedBinding.lagMonitor.onClaimAttempted(3L))
      val binding = openedBinding.copy(flusher = Some(flusher))

      val result = workflow.finalizeTransfer(descriptor.entityId, binding, lastSpooledSeq = 3L).futureValue

      result.binding.flusher shouldBe None
      flusher.stopCount shouldBe 1
      sessionPort.closeCalls should contain only CloseCall(descriptor.entityId, 4L, 192L, 3L)
      sessionPort.abortCalls shouldBe empty
      spool.cleanupCalls should contain(descriptor.entityId)
      claimPort.clearCalls should contain(binding.replyBinding)
      binding.replyBinding.entityId shouldBe ""
      lagSnapshot(binding.lagMonitor).spooledSeq shouldBe -1L
      lagSnapshot(binding.lagMonitor).lastClaimAttemptedSeq shouldBe -1L
      lagSnapshot(binding.lagMonitor).lastClaimConfirmedSeq shouldBe -1L
    }

    "finalizeTransfer abort cleanup and propagate the failure when close validation fails fatally" in {
      val descriptor = transferDescriptor(totalExpectedChunks = 4L)
      val meta = spoolMetaFor(descriptor, lastSpooledSeq = 3L, flushedSeq = 3L, totalSpooledBytes = 192L)
      val spool = new RecordingChunkSpool
      spool.seedMeta(descriptor.entityId, meta)
      val sessionPort = new RecordingSessionPort
      sessionPort.closeHandler = _ => Future.failed(CloseValidationFailure.BytesMismatch(7L, 8L))
      val claimPort = new RecordingClaimPort
      val workflow = newWorkflow(spool = spool, sessionPort = sessionPort, claimPort = claimPort)
      val flusher = new RecordingChunkFlusher(descriptor.entityId)
      flusher.drainResult = Future.successful(3L)

      val openedBinding = workflow.openBinding()
      claimPort.bindEntityId(openedBinding.replyBinding, descriptor.entityId)
      awaitLagOp(openedBinding.lagMonitor.onSpooled(3L))
      val binding = openedBinding.copy(flusher = Some(flusher))

      val ex = workflow.finalizeTransfer(descriptor.entityId, binding, lastSpooledSeq = 3L).failed.futureValue

      ex shouldBe CloseValidationFailure.BytesMismatch(7L, 8L)
      flusher.stopCount shouldBe 1
      sessionPort.abortCalls should have size 1
      sessionPort.abortCalls.head.reason should include("Close barrier failed")
      spool.cleanupCalls should contain(descriptor.entityId)
      claimPort.clearCalls should contain(binding.replyBinding)
      binding.replyBinding.entityId shouldBe ""
      lagSnapshot(binding.lagMonitor).spooledSeq shouldBe -1L
      lagSnapshot(binding.lagMonitor).lastClaimAttemptedSeq shouldBe -1L
      lagSnapshot(binding.lagMonitor).lastClaimConfirmedSeq shouldBe -1L
      binding.lagMonitor.isPaused.futureValue shouldBe false
      workflow.isFinalizationInFlight(descriptor.entityId).futureValue shouldBe false
    }

    "finalizeTransfer reject a second finalization while the first one is still in flight" in {
      val descriptor = transferDescriptor(totalExpectedChunks = 1L, declaredPayloadSize = 128L)
      val meta = spoolMetaFor(descriptor, lastSpooledSeq = 0L, flushedSeq = 0L, totalSpooledBytes = 128L)
      val spool = new RecordingChunkSpool
      spool.seedMeta(descriptor.entityId, meta)
      val sessionPort = new RecordingSessionPort
      val claimPort = new RecordingClaimPort
      val workflow = newWorkflow(spool = spool, sessionPort = sessionPort, claimPort = claimPort)
      val flusher = new RecordingChunkFlusher(descriptor.entityId)
      val drainPromise = Promise[Long]()
      flusher.drainResult = drainPromise.future

      val openedBinding = workflow.openBinding()
      claimPort.bindEntityId(openedBinding.replyBinding, descriptor.entityId)
      val binding = openedBinding.copy(flusher = Some(flusher))

      val firstFinalize = workflow.finalizeTransfer(descriptor.entityId, binding, lastSpooledSeq = 0L)
      workflow.isFinalizationInFlight(descriptor.entityId).futureValue shouldBe true

      val secondEx = workflow.finalizeTransfer(descriptor.entityId, binding, lastSpooledSeq = 0L).failed.futureValue
      secondEx shouldBe a[IllegalStateException]
      secondEx.getMessage should include("already finalizing")

      drainPromise.success(0L)
      firstFinalize.futureValue.binding.flusher shouldBe None
      workflow.isFinalizationInFlight(descriptor.entityId).futureValue shouldBe false
    }

    "own connection-close cleanup for flusher pause reply binding and entity binding" in {
      val claimPort = new RecordingClaimPort
      val workflow = newWorkflow(claimPort = claimPort)

      val flusher = new RecordingChunkFlusher("entity-1")
      val openedBinding = workflow.openBinding()
      claimPort.bindEntityId(openedBinding.replyBinding, "entity-1")
      awaitLagOp(openedBinding.lagMonitor.onSpooled(4L))
      awaitLagOp(openedBinding.lagMonitor.onClaimAttempted(4L))
      val binding = openedBinding.copy(flusher = Some(flusher))
      val pauseFuture = activePause(binding.lagMonitor)
      val disconnect = new RuntimeException("disconnect")

      pauseFuture.isCompleted shouldBe false

      workflow.onConnectionClosed(binding, Some("entity-1"), Some(disconnect))

      flusher.stopCount shouldBe 1
      claimPort.clearCalls should contain(binding.replyBinding)
      binding.replyBinding.entityId shouldBe ""
      claimPort.closeCalls should contain(binding.replyBinding)

      val pauseCancelled = pauseFuture.failed.futureValue
      pauseCancelled shouldBe a[FlushPauseCancelled]
      pauseCancelled.getCause shouldBe disconnect
      pauseCancelled.asInstanceOf[FlushPauseCancelled].entityId shouldBe "entity-1"
      pauseCancelled.asInstanceOf[FlushPauseCancelled].causeOpt.get shouldBe disconnect
    }

    "prepareTransfer for an aborted session should resume via register and reuse matching spool" in {
      val descriptor = transferDescriptor(totalExpectedChunks = 3L)
      val spool = new RecordingChunkSpool
      spool.seedMeta(descriptor.entityId, spoolMetaFor(descriptor, lastSpooledSeq = 1L, flushedSeq = 1L, totalSpooledBytes = 64L))
      val sessionPort = new RecordingSessionPort
      sessionPort.inspectHandler = _ => Future.successful(
        sessionSummaryFor(descriptor, isAborted = true, device = Some("device-1"))
      )
      val claimPort = new RecordingClaimPort
      val flusherFactory = new RecordingChunkFlusherFactory
      val workflow = newWorkflow(spool = spool, flusherFactory = flusherFactory, sessionPort = sessionPort, claimPort = claimPort)

      val binding = workflow.openBinding()
      val prepared = workflow.prepareTransfer(binding, descriptor).futureValue

      prepared.receivedChunks shouldBe 2L
      prepared.lastAcceptedSeq shouldBe 1L
      prepared.isComplete shouldBe false
      prepared.binding.flusher shouldBe defined
      sessionPort.registerCalls should have size 1
      flusherFactory.created should have size 1
      flusherFactory.created.head.startSeq shouldBe 2L
    }

    "finalizeTransfer complete normally after a slow drain" in {
      val descriptor = transferDescriptor(totalExpectedChunks = 2L, declaredPayloadSize = 128L)
      val meta = spoolMetaFor(descriptor, lastSpooledSeq = 1L, flushedSeq = 1L, totalSpooledBytes = 128L)
      val spool = new RecordingChunkSpool
      spool.seedMeta(descriptor.entityId, meta)
      val sessionPort = new RecordingSessionPort
      val claimPort = new RecordingClaimPort
      val workflow = newWorkflow(spool = spool, sessionPort = sessionPort, claimPort = claimPort)
      val flusher = new RecordingChunkFlusher(descriptor.entityId)
      val drainPromise = Promise[Long]()
      flusher.drainResult = drainPromise.future

      val openedBinding = workflow.openBinding()
      claimPort.bindEntityId(openedBinding.replyBinding, descriptor.entityId)
      awaitLagOp(openedBinding.lagMonitor.onSpooled(1L))
      awaitLagOp(openedBinding.lagMonitor.onClaimAttempted(1L))
      val binding = openedBinding.copy(flusher = Some(flusher))

      val finalizeFuture = workflow.finalizeTransfer(descriptor.entityId, binding, lastSpooledSeq = 1L)
      finalizeFuture.isCompleted shouldBe false

      drainPromise.success(1L)
      val result = finalizeFuture.futureValue
      result.binding.flusher shouldBe None
      flusher.stopCount shouldBe 1
      sessionPort.closeCalls should have size 1
      spool.cleanupCalls should contain(descriptor.entityId)
    }

    "connection-close cleanup should be safe even without a flusher or entity binding" in {
      val claimPort = new RecordingClaimPort
      val workflow = newWorkflow(claimPort = claimPort)
      val openedBinding = workflow.openBinding()

      workflow.onConnectionClosed(openedBinding, None, None)

      claimPort.closeCalls should contain(openedBinding.replyBinding)
    }

    "finalizeTransfer cleanup spool even when close succeeds but entity was already cleaned" in {
      val descriptor = transferDescriptor(totalExpectedChunks = 1L, declaredPayloadSize = 64L)
      val meta = spoolMetaFor(descriptor, lastSpooledSeq = 0L, flushedSeq = 0L, totalSpooledBytes = 64L)
      val spool = new RecordingChunkSpool
      spool.seedMeta(descriptor.entityId, meta)
      val sessionPort = new RecordingSessionPort
      val claimPort = new RecordingClaimPort
      val workflow = newWorkflow(spool = spool, sessionPort = sessionPort, claimPort = claimPort)
      val flusher = new RecordingChunkFlusher(descriptor.entityId)
      flusher.drainResult = Future.successful(0L)

      val openedBinding = workflow.openBinding()
      claimPort.bindEntityId(openedBinding.replyBinding, descriptor.entityId)
      val binding = openedBinding.copy(flusher = Some(flusher))

      val result = workflow.finalizeTransfer(descriptor.entityId, binding, lastSpooledSeq = 0L).futureValue
      result.binding.flusher shouldBe None
      spool.cleanupCalls should contain(descriptor.entityId)

      spool.cleanup(descriptor.entityId).futureValue
      spool.cleanupCalls.count(_ == descriptor.entityId) shouldBe 2
    }

    // Track F.14.4 — when a pressure-aware Workflow is wired with a closed
    // admission controller, prepareTransfer fails fast with
    // SpoolPressureCriticalException carrying the configured retry-after.
    // The session port is never inspected — admission gates the call before
    // any per-session work runs.
    "prepareTransfer fail with SpoolPressureCriticalException when admission is closed" in {
      val descriptor = transferDescriptor()
      val spool = new RecordingChunkSpool
      val sessionPort = new RecordingSessionPort
      val claimPort = new RecordingClaimPort
      val admission = AdmissionController(testKit.system)
      admission.close("synthetic critical").futureValue
      val pressureConfig = SpoolPressureConfig.Disabled.copy(
        suggestedRetryAfter = 17.seconds
      )
      val workflow = Workflow[String, TestSessionSummary, String, TestReplyBinding](
        spool = spool,
        flusherFactory = new RecordingChunkFlusherFactory,
        sessionPort = sessionPort,
        claimPort = claimPort,
        config = makeConfig(),
        admissionController = admission,
        pressureConfig = pressureConfig,
        system = testKit.system
      )

      val binding = workflow.openBinding()
      val ex = workflow.prepareTransfer(binding, descriptor).failed.futureValue
      ex shouldBe a[SpoolPressureCriticalException]
      ex.asInstanceOf[SpoolPressureCriticalException].retryAfter shouldBe 17.seconds
      // Admission gate ran BEFORE any session-port traffic. The default
      // inspect handler throws AssertionError for any unexpected call —
      // matching SpoolPressureCriticalException above proves inspect was
      // never invoked.
    }

    // Track F.14.4 — admission is read once per session at admission time.
    // The hot path (acceptChunk) is never gated. After a session is
    // admitted, closing admission must NOT affect the in-flight session.
    "acceptChunk continue to flow for an admitted session even after admission closes" in {
      val descriptor = transferDescriptor(totalExpectedChunks = 4L)
      val spool = new RecordingChunkSpool
      val sessionPort = new RecordingSessionPort
      sessionPort.inspectHandler = _ => Future.successful(sessionSummaryFor(descriptor))
      val claimPort = new RecordingClaimPort
      val flusher = new RecordingChunkFlusher(descriptor.entityId)
      val flusherFactory = new RecordingChunkFlusherFactory
      flusherFactory.enqueue(flusher)
      val admission = AdmissionController(testKit.system)
      val workflow = Workflow[String, TestSessionSummary, String, TestReplyBinding](
        spool = spool,
        flusherFactory = flusherFactory,
        sessionPort = sessionPort,
        claimPort = claimPort,
        config = makeConfig(),
        admissionController = admission,
        pressureConfig = SpoolPressureConfig.Disabled,
        system = testKit.system
      )

      // Admit the session while admission is open.
      val binding = workflow.openBinding()
      val prepared = workflow.prepareTransfer(binding, descriptor).futureValue

      // Now close admission, mid-session.
      admission.close("synthetic critical mid-session").futureValue

      // The hot-path chunk acceptance must continue regardless.
      val accepted = workflow.acceptChunk(
        descriptor.entityId,
        prepared.binding,
        envelope = "chunk-0",
        bytes = "chunk-0".getBytes,
        zeroBasedSeq = 0L,
        lastAcceptedSeq = -1L
      ).futureValue
      accepted.binding.replyBinding shouldBe binding.replyBinding
      spool.writeCalls.size shouldBe 1
    }
  }

  private val fixedInstant = Instant.parse("2026-03-23T00:00:00Z")

  private def makeConfig(
      claimLagSoft: Long = 2L,
      claimLagHard: Long = 4L,
      pauseTimeout: FiniteDuration = 1.second,
      maxRetries: Int = 1
  ): FlushConfig =
    FlushConfig(
      backpressure = FlushBackpressureConfig(
        claimLagSoft = claimLagSoft,
        claimLagHard = claimLagHard,
        pauseTimeout = pauseTimeout
      ),
      close = FlushCloseConfig(
        askTimeout = Timeout(1.second),
        inspectTimeout = Timeout(1.second),
        retryDelay = 10.millis,
        maxRetries = maxRetries
      ),
      recovery = FlushRecoveryConfig(
        parallelism = 1,
        inspectTimeout = Timeout(1.second),
        perEntityTimeout = 5.seconds
      )
    )

  private def newWorkflow(
      spool: ChunkSpool = new RecordingChunkSpool,
      flusherFactory: ChunkFlusherFactory = new RecordingChunkFlusherFactory,
      sessionPort: SessionPort[String, TestSessionSummary] = new RecordingSessionPort,
      claimPort: RecordingClaimPort = new RecordingClaimPort,
      config: FlushConfig = makeConfig()
  ): Workflow[String, TestSessionSummary, String, TestReplyBinding] =
    Workflow[String, TestSessionSummary, String, TestReplyBinding](
      spool = spool,
      flusherFactory = flusherFactory,
      sessionPort = sessionPort,
      claimPort = claimPort,
      config = config,
      system = testKit.system
    )

  private def transferDescriptor(
      entityId: String = "entity-1",
      device: String = "device-1",
      deviceId: String = "device-id-1",
      deviceCorrelationId: String = "subject-1",
      objectHashHex: String = "hash-123",
      declaredPayloadSize: Long = 1024L,
      totalExpectedChunks: Long = 4L
  ): FlushTransferDescriptor[String] =
    FlushTransferDescriptor(
      entityId = entityId,
      device = device,
      deviceId = deviceId,
      deviceCorrelationId = deviceCorrelationId,
      objectHashHex = objectHashHex,
      declaredPayloadSize = declaredPayloadSize,
      totalExpectedChunks = totalExpectedChunks
    )

  private def sessionSummaryFor(
      descriptor: FlushTransferDescriptor[String],
      isClosed: Boolean = false,
      isAborted: Boolean = false,
      openedAt: Option[Instant] = Some(fixedInstant),
      claimsCount: Long = 0L,
      totalClaimedBytes: Long = 0L,
      lastClaimSequence: Long = -1L,
      device: Option[String] = None,
      deviceCorrelationId: Option[String] = None
  ): TestSessionSummary =
    TestSessionSummary(
      SessionView(
        isClosed = isClosed,
        isAborted = isAborted,
        openedAt = openedAt,
        device = device.orElse(Some(descriptor.device)),
        deviceCorrelationId = deviceCorrelationId.orElse(Some(descriptor.deviceCorrelationId)),
        objectHashHex = descriptor.objectHashHex,
        declaredPayloadSize = descriptor.declaredPayloadSize,
        claimsCount = claimsCount,
        totalClaimedBytes = totalClaimedBytes,
        lastClaimSequence = lastClaimSequence
      )
    )

  private def spoolMetaFor(
      descriptor: FlushTransferDescriptor[String],
      lastSpooledSeq: Long,
      flushedSeq: Long,
      totalSpooledBytes: Long
  ): SpoolMeta =
    SpoolMeta(
      entityId = descriptor.entityId,
      deviceId = descriptor.deviceId,
      objectHashHex = descriptor.objectHashHex,
      lastSpooledSeq = lastSpooledSeq,
      totalSpooledBytes = totalSpooledBytes,
      flushedSeq = flushedSeq,
      declaredPayloadSize = descriptor.declaredPayloadSize,
      totalExpectedChunks = descriptor.totalExpectedChunks,
      createdAt = fixedInstant
    )

  private final case class TestSessionSummary(view: SessionView[String])

  private final case class RegisterCall(
      entityId: String,
      device: String,
      deviceCorrelationId: String,
      objectHashHex: String,
      declaredPayloadSize: Long
  )

  private final case class AbortCall(entityId: String, reason: String)

  private final case class CloseCall(
      entityId: String,
      expectedClaimsCount: Long,
      expectedTotalBytes: Long,
      expectedLastSequence: Long
  )

  private final case class DispatchCall(
      entityId: String,
      envelope: String,
      rawBytesLength: Long,
      binding: TestReplyBinding
  )

  private final case class CreatedFlusher(
      entityId: String,
      startSeq: Long,
      flusher: RecordingChunkFlusher
  )

  private final class RecordingChunkSpool extends ChunkSpool {
    private val metas = mutable.Map.empty[String, SpoolMeta]
    private val chunks = mutable.Map.empty[(String, Long), Array[Byte]]

    val writeCalls = mutable.ArrayBuffer.empty[(String, Long, Array[Byte])]
    val initializeCalls = mutable.ArrayBuffer.empty[(String, SpoolMeta)]
    val cleanupCalls = mutable.ArrayBuffer.empty[String]

    def seedMeta(entityId: String, meta: SpoolMeta): Unit =
      metas.update(entityId, meta)

    def seedChunk(entityId: String, seq: Long, bytes: Array[Byte]): Unit =
      chunks.update((entityId, seq), bytes.clone())

    def chunkBytes(entityId: String, seq: Long): Array[Byte] =
      chunks((entityId, seq)).clone()

    override def write(entityId: String, seq: Long, bytes: Array[Byte]): Future[Long] = {
      val copy = bytes.clone()
      chunks.update((entityId, seq), copy)
      writeCalls += ((entityId, seq, copy))
      Future.successful(copy.length.toLong)
    }

    override def readChunk(entityId: String, seq: Long): Future[Array[Byte]] =
      chunks.get((entityId, seq)) match {
        case Some(bytes) => Future.successful(bytes.clone())
        case None => Future.failed(new java.nio.file.NoSuchFileException(s"$entityId/$seq"))
      }

    override def readMeta(entityId: String): Future[Option[SpoolMeta]] =
      Future.successful(metas.get(entityId))

    override def updateMeta(entityId: String, meta: SpoolMeta): Future[Unit] = {
      metas.update(entityId, meta)
      Future.successful(())
    }

    override def updateFlushedSeq(entityId: String, seq: Long): Future[Unit] = {
      val updated = metas(entityId).withFlushed(seq)
      metas.update(entityId, updated)
      Future.successful(())
    }

    override def initialize(entityId: String, meta: SpoolMeta): Future[SpoolMeta] = {
      metas.update(entityId, meta)
      initializeCalls += ((entityId, meta))
      Future.successful(meta)
    }

    override def cleanup(entityId: String): Future[Unit] = {
      metas.remove(entityId)
      chunks.keys.filter(_._1 == entityId).toList.foreach(chunks.remove)
      cleanupCalls += entityId
      Future.successful(())
    }

    override def listEntities(): Future[Seq[String]] =
      Future.successful((metas.keys ++ chunks.keys.map(_._1)).toSeq.distinct.sorted)
  }

  private final class RecordingChunkFlusherFactory extends ChunkFlusherFactory {
    private val queued = mutable.Queue.empty[RecordingChunkFlusher]

    val created = mutable.ArrayBuffer.empty[CreatedFlusher]

    def enqueue(flusher: RecordingChunkFlusher): Unit =
      queued.enqueue(flusher)

    override def create(entityId: String, spool: ChunkSpool, startSeq: Long): ChunkFlusher = {
      val flusher = if (queued.nonEmpty) queued.dequeue() else new RecordingChunkFlusher(entityId)
      created += CreatedFlusher(entityId, startSeq, flusher)
      flusher
    }
  }

  private final class RecordingSessionPort extends SessionPort[String, TestSessionSummary] {
    val registerCalls = mutable.ArrayBuffer.empty[RegisterCall]
    val abortCalls = mutable.ArrayBuffer.empty[AbortCall]
    val closeCalls = mutable.ArrayBuffer.empty[CloseCall]

    var inspectHandler: String => Future[TestSessionSummary] = entityId =>
      Future.failed(new AssertionError(s"Unexpected inspect for $entityId"))

    var registerHandler: RegisterCall => Future[TestSessionSummary] = call =>
      Future.successful(
        TestSessionSummary(
          SessionView(
            isClosed = false,
            isAborted = false,
            openedAt = Some(fixedInstant),
            device = Some(call.device),
            deviceCorrelationId = Some(call.deviceCorrelationId),
            objectHashHex = call.objectHashHex,
            declaredPayloadSize = call.declaredPayloadSize,
            claimsCount = 0L,
            totalClaimedBytes = 0L,
            lastClaimSequence = -1L
          )
        )
      )

    var abortHandler: AbortCall => Future[TestSessionSummary] = _ =>
      Future.successful(
        TestSessionSummary(
          SessionView(
            isClosed = false,
            isAborted = true,
            openedAt = Some(fixedInstant),
            device = Some("device-1"),
            deviceCorrelationId = Some("subject-1"),
            objectHashHex = "hash-123",
            declaredPayloadSize = 1024L,
            claimsCount = 0L,
            totalClaimedBytes = 0L,
            lastClaimSequence = -1L
          )
        )
      )

    var closeHandler: CloseCall => Future[TestSessionSummary] = call =>
      Future.successful(
        TestSessionSummary(
          SessionView(
            isClosed = true,
            isAborted = false,
            openedAt = Some(fixedInstant),
            device = Some("device-1"),
            deviceCorrelationId = Some("subject-1"),
            objectHashHex = "hash-123",
            declaredPayloadSize = 1024L,
            claimsCount = call.expectedClaimsCount,
            totalClaimedBytes = call.expectedTotalBytes,
            lastClaimSequence = call.expectedLastSequence
          )
        )
      )

    override def register(
        entityId: String,
        device: String,
        deviceCorrelationId: String,
        objectHashHex: String,
        declaredPayloadSize: Long,
        fileName: Option[String]
    )(using Timeout): Future[TestSessionSummary] = {
      val call = RegisterCall(entityId, device, deviceCorrelationId, objectHashHex, declaredPayloadSize)
      registerCalls += call
      registerHandler(call)
    }

    override def inspect(entityId: String)(using Timeout): Future[TestSessionSummary] =
      inspectHandler(entityId)

    override def abort(entityId: String, reason: String)(using Timeout): Future[TestSessionSummary] = {
      val call = AbortCall(entityId, reason)
      abortCalls += call
      abortHandler(call)
    }

    override def closeWithValidation(
        entityId: String,
        expectedClaimsCount: Long,
        expectedTotalBytes: Long,
        expectedLastSequence: Long
    )(using Timeout): Future[TestSessionSummary] = {
      val call = CloseCall(entityId, expectedClaimsCount, expectedTotalBytes, expectedLastSequence)
      closeCalls += call
      closeHandler(call)
    }

    override def toSessionView(summary: TestSessionSummary): SessionView[String] =
      summary.view
  }

  private final class TestReplyBinding(
      val onConfirmedClaimsCount: Long => Unit,
      val onRejected: Throwable => Unit
  ) {
    private var currentEntityId: String = ""

    def entityId: String = currentEntityId

    def bindEntityId(entityId: String): Unit =
      currentEntityId = entityId

    def clearEntityId(): Unit =
      currentEntityId = ""

    def confirmClaimsCount(claimsCount: Long): Unit =
      onConfirmedClaimsCount(claimsCount)

    def reject(ex: Throwable): Unit =
      onRejected(ex)
  }

  private final class RecordingClaimPort extends ClaimPort[String, TestReplyBinding] {
    private var latestReplyBinding: Option[TestReplyBinding] = None
    val bindCalls = mutable.ArrayBuffer.empty[(TestReplyBinding, String)]
    val clearCalls = mutable.ArrayBuffer.empty[TestReplyBinding]
    val closeCalls = mutable.ArrayBuffer.empty[TestReplyBinding]
    val dispatchCalls = mutable.ArrayBuffer.empty[DispatchCall]

    def confirmClaimsCount(claimsCount: Long): Unit =
      latestReplyBinding.getOrElse(fail("expected reply binding for confirmClaimsCount")).confirmClaimsCount(claimsCount)

    def reject(ex: Throwable): Unit =
      latestReplyBinding.getOrElse(fail("expected reply binding for reject")).reject(ex)

    override def decodeEnvelope(bytes: Array[Byte]): Try[String] =
      Try(new String(bytes))

    override def openReplyBinding(
        onConfirmedClaimsCount: Long => Unit,
        onRejected: Throwable => Unit
    ): TestReplyBinding = {
      val replyBinding = new TestReplyBinding(onConfirmedClaimsCount, onRejected)
      latestReplyBinding = Some(replyBinding)
      replyBinding
    }

    override def bindEntityId(replyBinding: TestReplyBinding, entityId: String): Unit = {
      bindCalls += ((replyBinding, entityId))
      replyBinding.bindEntityId(entityId)
    }

    override def clearEntityId(replyBinding: TestReplyBinding): Unit = {
      clearCalls += replyBinding
      replyBinding.clearEntityId()
    }

    override def closeReplyBinding(replyBinding: TestReplyBinding): Unit =
      closeCalls += replyBinding

    override def dispatchClaim(
        entityId: String,
        envelope: String,
        rawBytesLength: Long,
        replyBinding: TestReplyBinding
    ): Unit =
      dispatchCalls += DispatchCall(entityId, envelope, rawBytesLength, replyBinding)
  }

  private final class RecordingChunkFlusher(val entityId: String) extends ChunkFlusher {
    var startCount: Int = 0
    var stopCount: Int = 0

    var drainResult: Future[Long] = Future.successful(-1L)
    var currentFlushedSeq: Long = -1L

    override def start(): Unit =
      startCount += 1

    override def stop(): Unit =
      stopCount += 1

    override def drain(): Future[Long] =
      drainResult

    override def flushedSeq: Future[Long] =
      Future.successful(currentFlushedSeq)

    override def isRunning: Future[Boolean] =
      Future.successful(stopCount == 0)
  }
}
