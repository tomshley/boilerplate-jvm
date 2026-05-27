/*
 * Copyright 2026 Tomshley LLC.
 * All Rights Reserved.
 */

package com.tomshley.boilerplate.jvm.durablebufferedflush

import com.tomshley.boilerplate.jvm.durablebufferedflush.internal.RecoveryManagerImpl
import org.apache.pekko.actor.testkit.typed.scaladsl.ActorTestKit
import org.apache.pekko.actor.typed.ActorSystem
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{Millis, Seconds, Span}
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.concurrent.duration.*

final class RecoveryManagerImplSpec
    extends AnyWordSpec
    with Matchers
    with ScalaFutures
    with BeforeAndAfterAll
    with FlushSpecSupport {

  override implicit val patienceConfig: PatienceConfig =
    PatienceConfig(timeout = Span(5, Seconds), interval = Span(50, Millis))

  private val testKit = ActorTestKit("RecoveryManagerImplSpec")
  private given ActorSystem[?] = testKit.system
  private given ExecutionContext = testKit.system.executionContext

  override def afterAll(): Unit = {
    testKit.shutdownTestKit()
    super.afterAll()
  }

  "RecoveryManagerImpl" should {

    "clean up already-closed sessions during recovery" in {
      val spool = new RecordingChunkSpool()
      val flusherFactory = new RecordingChunkFlusherFactory()
      val sessionPort = new RecordingSessionPort()
      val claimPort = new RecordingClaimPort()
      val entityId = "entity-recovery-clean"

      spool.seedMeta(entityId, spoolMeta(entityId = entityId, lastSpooledSeq = 1L, totalSpooledBytes = 64L))
      sessionPort.inspectHandler = _ => Future.successful(
        TestSessionSummary(sessionView(isClosed = true, claimsCount = 2L, lastClaimSequence = 1L))
      )

      val manager = new RecoveryManagerImpl[String, TestSessionSummary, String, TestReplyBinding](
        spool = spool,
        flusherFactory = flusherFactory,
        sessionPort = sessionPort,
        claimPort = claimPort,
        config = makeConfig(),
        system = testKit.system
      )

      val report = manager.recover().futureValue

      report.sessionsRecovered shouldBe 0
      report.sessionsAborted shouldBe 0
      report.sessionsCleaned shouldBe 1
      report.sessionsFailed shouldBe 0
      report.totalClaimsResent shouldBe 0L
      spool.cleanupCalls should contain only entityId
      claimPort.openCalls should have size 1
      claimPort.closeCalls should have size 1
    }

    "abort and clean up active sessions when spool metadata is unreadable" in {
      val spool = new RecordingChunkSpool()
      val flusherFactory = new RecordingChunkFlusherFactory()
      val sessionPort = new RecordingSessionPort()
      val claimPort = new RecordingClaimPort()
      val entityId = "entity-recovery-unreadable"

      spool.listEntitiesHandler = () => Future.successful(Seq(entityId))
      spool.readMetaHandler = _ => Future.failed(new IllegalStateException("meta boom"))
      sessionPort.inspectHandler = _ => Future.successful(
        TestSessionSummary(sessionView(isClosed = false, isAborted = false, openedAt = Some(fixedInstant)))
      )

      val manager = new RecoveryManagerImpl[String, TestSessionSummary, String, TestReplyBinding](
        spool = spool,
        flusherFactory = flusherFactory,
        sessionPort = sessionPort,
        claimPort = claimPort,
        config = makeConfig(),
        system = testKit.system
      )

      val report = manager.recover().futureValue

      report.sessionsRecovered shouldBe 0
      report.sessionsAborted shouldBe 1
      report.sessionsCleaned shouldBe 0
      report.sessionsFailed shouldBe 0
      report.totalClaimsResent shouldBe 0L
      sessionPort.abortCalls should have size 1
      sessionPort.abortCalls.head.reason should include("Recovery failed: meta boom")
      spool.cleanupCalls should contain only entityId
      claimPort.closeCalls should have size 1
    }

    "recover a complete session by resending missing claims draining closing and cleaning up" in {
      val spool = new RecordingChunkSpool()
      val flusherFactory = new RecordingChunkFlusherFactory()
      val sessionPort = new RecordingSessionPort()
      val claimPort = new RecordingClaimPort()
      val entityId = "entity-recovery-complete"
      val meta = spoolMeta(
        entityId = entityId,
        lastSpooledSeq = 2L,
        totalSpooledBytes = 96L,
        flushedSeq = 0L,
        totalExpectedChunks = 3L
      )
      val flusher = new RecordingChunkFlusher(entityId)

      flusher.drainResult = Future.successful(2L)
      flusherFactory.enqueue(flusher)
      spool.seedMeta(entityId, meta)
      spool.seedChunk(entityId, 1L, "chunk-1".getBytes)
      spool.seedChunk(entityId, 2L, "chunk-2".getBytes)
      sessionPort.inspectHandler = _ => Future.successful(
        TestSessionSummary(sessionView(
          isClosed = false,
          isAborted = false,
          openedAt = Some(fixedInstant),
          claimsCount = 1L,
          totalClaimedBytes = 32L,
          lastClaimSequence = 0L
        ))
      )
      sessionPort.closeHandler = call =>
        Future.successful(
          TestSessionSummary(sessionView(
            isClosed = true,
            claimsCount = call.expectedClaimsCount,
            totalClaimedBytes = call.expectedTotalBytes,
            lastClaimSequence = call.expectedLastSequence
          ))
        )

      val manager = new RecoveryManagerImpl[String, TestSessionSummary, String, TestReplyBinding](
        spool = spool,
        flusherFactory = flusherFactory,
        sessionPort = sessionPort,
        claimPort = claimPort,
        config = makeConfig(),
        system = testKit.system
      )

      val report = manager.recover().futureValue

      report.sessionsRecovered shouldBe 1
      report.sessionsAborted shouldBe 0
      report.sessionsCleaned shouldBe 0
      report.sessionsFailed shouldBe 0
      report.totalClaimsResent shouldBe 2L
      flusherFactory.created should have size 1
      flusherFactory.created.head.startSeq shouldBe 1L
      flusher.startCount shouldBe 1
      flusher.stopCount shouldBe 1
      claimPort.dispatchCalls.map(_.envelope) shouldBe Seq("chunk-1", "chunk-2")
      sessionPort.closeCalls should contain only CloseCall(
        entityId = entityId,
        expectedClaimsCount = 3L,
        expectedTotalBytes = 96L,
        expectedLastSequence = 2L
      )
      spool.cleanupCalls should contain only entityId
      claimPort.closeCalls should have size 1
    }

    "abort and clean up when spool metadata has flushedSeq greater than lastSpooledSeq" in {
      val spool = new RecordingChunkSpool()
      val flusherFactory = new RecordingChunkFlusherFactory()
      val sessionPort = new RecordingSessionPort()
      val claimPort = new RecordingClaimPort()
      val entityId = "entity-skewed-meta"

      spool.seedMeta(entityId, spoolMeta(
        entityId = entityId,
        lastSpooledSeq = 1L,
        totalSpooledBytes = 32L,
        flushedSeq = 5L
      ))
      sessionPort.inspectHandler = _ => Future.successful(
        TestSessionSummary(sessionView(isClosed = false))
      )

      val manager = new RecoveryManagerImpl[String, TestSessionSummary, String, TestReplyBinding](
        spool = spool,
        flusherFactory = flusherFactory,
        sessionPort = sessionPort,
        claimPort = claimPort,
        config = makeConfig(),
        system = testKit.system
      )

      val report = manager.recover().futureValue

      report.sessionsAborted shouldBe 1
      report.sessionsRecovered shouldBe 0
      sessionPort.abortCalls should have size 1
      spool.cleanupCalls should contain only entityId
    }

    "recover a session where all claims are already confirmed and only drain and close are needed" in {
      val spool = new RecordingChunkSpool()
      val flusherFactory = new RecordingChunkFlusherFactory()
      val sessionPort = new RecordingSessionPort()
      val claimPort = new RecordingClaimPort()
      val entityId = "entity-no-resend"
      val meta = spoolMeta(
        entityId = entityId,
        lastSpooledSeq = 2L,
        totalSpooledBytes = 96L,
        flushedSeq = 0L,
        totalExpectedChunks = 3L
      )
      val flusher = new RecordingChunkFlusher(entityId)
      flusher.drainResult = Future.successful(2L)
      flusherFactory.enqueue(flusher)

      spool.seedMeta(entityId, meta)
      sessionPort.inspectHandler = _ => Future.successful(
        TestSessionSummary(sessionView(
          isClosed = false,
          claimsCount = 3L,
          totalClaimedBytes = 96L,
          lastClaimSequence = 2L
        ))
      )
      sessionPort.closeHandler = call =>
        Future.successful(
          TestSessionSummary(sessionView(
            isClosed = true,
            claimsCount = call.expectedClaimsCount,
            totalClaimedBytes = call.expectedTotalBytes,
            lastClaimSequence = call.expectedLastSequence
          ))
        )

      val manager = new RecoveryManagerImpl[String, TestSessionSummary, String, TestReplyBinding](
        spool = spool,
        flusherFactory = flusherFactory,
        sessionPort = sessionPort,
        claimPort = claimPort,
        config = makeConfig(),
        system = testKit.system
      )

      val report = manager.recover().futureValue

      report.sessionsRecovered shouldBe 1
      report.totalClaimsResent shouldBe 0L
      claimPort.dispatchCalls shouldBe empty
      flusher.startCount shouldBe 1
      flusher.stopCount shouldBe 1
      sessionPort.closeCalls should have size 1
      spool.cleanupCalls should contain only entityId
    }

    "reconcileOrphans drains a single inactive orphan" in {
      val spool = new RecordingChunkSpool()
      val flusherFactory = new RecordingChunkFlusherFactory()
      val sessionPort = new RecordingSessionPort()
      val claimPort = new RecordingClaimPort()
      val entityId = "entity-orphan-inactive"
      val meta = spoolMeta(
        entityId = entityId,
        lastSpooledSeq = 1L,
        totalSpooledBytes = 64L,
        flushedSeq = -1L,
        totalExpectedChunks = 4L
      )
      val flusher = new RecordingChunkFlusher(entityId)
      flusher.drainResult = Future.successful(1L)
      flusherFactory.enqueue(flusher)

      spool.seedMeta(entityId, meta)
      spool.seedChunk(entityId, 0L, "chunk-0".getBytes)
      spool.seedChunk(entityId, 1L, "chunk-1".getBytes)
      sessionPort.inspectHandler = _ => Future.successful(
        TestSessionSummary(sessionView(
          isClosed = false,
          isAborted = false,
          openedAt = Some(fixedInstant),
          claimsCount = 0L,
          lastClaimSequence = -1L
        ))
      )

      val manager = new RecoveryManagerImpl[String, TestSessionSummary, String, TestReplyBinding](
        spool = spool,
        flusherFactory = flusherFactory,
        sessionPort = sessionPort,
        claimPort = claimPort,
        config = makeConfig(),
        system = testKit.system
      )

      val report = manager.reconcileOrphans(_ => Future.successful(false)).futureValue

      report.sessionsRecovered shouldBe 1
      report.sessionsCleaned shouldBe 0
      report.sessionsAborted shouldBe 0
      report.sessionsFailed shouldBe 0
      report.totalClaimsResent shouldBe 2L
      flusher.startCount shouldBe 1
      flusher.stopCount shouldBe 1
    }

    "reconcileOrphans skips entities reported as active by the predicate" in {
      val spool = new RecordingChunkSpool()
      val flusherFactory = new RecordingChunkFlusherFactory()
      val sessionPort = new RecordingSessionPort()
      val claimPort = new RecordingClaimPort()
      val activeId = "entity-active"
      val orphanId = "entity-orphan"

      spool.seedMeta(activeId, spoolMeta(entityId = activeId, lastSpooledSeq = 0L, totalSpooledBytes = 16L))
      spool.seedMeta(orphanId, spoolMeta(entityId = orphanId, lastSpooledSeq = 1L, totalSpooledBytes = 32L))
      sessionPort.inspectHandler = _ => Future.successful(
        TestSessionSummary(sessionView(isClosed = true, claimsCount = 1L, lastClaimSequence = 0L))
      )

      val manager = new RecoveryManagerImpl[String, TestSessionSummary, String, TestReplyBinding](
        spool = spool,
        flusherFactory = flusherFactory,
        sessionPort = sessionPort,
        claimPort = claimPort,
        config = makeConfig(),
        system = testKit.system
      )

      val isActiveCalls = scala.collection.mutable.ArrayBuffer.empty[String]
      val isActive: String => Future[Boolean] = id => {
        isActiveCalls += id
        Future.successful(id == activeId)
      }

      val report = manager.reconcileOrphans(isActive).futureValue

      isActiveCalls.toSet shouldBe Set(activeId, orphanId)
      // Only the orphan went through recovery; cleanup confirms it (closed-session path → Cleaned).
      report.sessionsCleaned shouldBe 1
      report.sessionsRecovered shouldBe 0
      report.sessionsAborted shouldBe 0
      report.sessionsFailed shouldBe 0
      spool.cleanupCalls should contain only orphanId
    }

    "reconcileOrphans returns an empty report when no orphans are present" in {
      val spool = new RecordingChunkSpool()
      val flusherFactory = new RecordingChunkFlusherFactory()
      val sessionPort = new RecordingSessionPort()
      val claimPort = new RecordingClaimPort()

      spool.listEntitiesHandler = () => Future.successful(Seq.empty)

      val manager = new RecoveryManagerImpl[String, TestSessionSummary, String, TestReplyBinding](
        spool = spool,
        flusherFactory = flusherFactory,
        sessionPort = sessionPort,
        claimPort = claimPort,
        config = makeConfig(),
        system = testKit.system
      )

      val report = manager.reconcileOrphans(_ => Future.successful(false)).futureValue

      report shouldBe RecoveryReport.empty
      spool.cleanupCalls shouldBe empty
      flusherFactory.created shouldBe empty
    }

    "reconcileOrphans treats isActive predicate failures as active and skips recovery" in {
      val spool = new RecordingChunkSpool()
      val flusherFactory = new RecordingChunkFlusherFactory()
      val sessionPort = new RecordingSessionPort()
      val claimPort = new RecordingClaimPort()
      val flakyId = "entity-flaky"
      val cleanId = "entity-clean"

      spool.seedMeta(flakyId, spoolMeta(entityId = flakyId, lastSpooledSeq = 0L, totalSpooledBytes = 16L))
      spool.seedMeta(cleanId, spoolMeta(entityId = cleanId, lastSpooledSeq = 0L, totalSpooledBytes = 16L))
      sessionPort.inspectHandler = _ => Future.successful(
        TestSessionSummary(sessionView(isClosed = true, claimsCount = 1L, lastClaimSequence = 0L))
      )

      val manager = new RecoveryManagerImpl[String, TestSessionSummary, String, TestReplyBinding](
        spool = spool,
        flusherFactory = flusherFactory,
        sessionPort = sessionPort,
        claimPort = claimPort,
        config = makeConfig(),
        system = testKit.system
      )

      val isActive: String => Future[Boolean] = {
        case id if id == flakyId => Future.failed(new RuntimeException("predicate boom"))
        case _                   => Future.successful(false)
      }

      val report = manager.reconcileOrphans(isActive).futureValue

      // Only the clean entity is recovered (closed → cleaned). Flaky entity is skipped this pass.
      report.sessionsCleaned shouldBe 1
      report.sessionsRecovered shouldBe 0
      spool.cleanupCalls should contain only cleanId
    }

    // F50-T2 (Track E.3) — a stuck `isActive` predicate must not block a
    // reconciliation pass; the per-entity timeout fires and the entity is
    // treated as active (skipped this pass — the conservative choice).
    "reconcileOrphans applies per-entity timeout to a stuck isActive predicate and treats it as active" in {
      val spool = new RecordingChunkSpool()
      val flusherFactory = new RecordingChunkFlusherFactory()
      val sessionPort = new RecordingSessionPort()
      val claimPort = new RecordingClaimPort()
      val stuckId = "entity-stuck-active"
      val cleanId = "entity-clean-active"

      spool.seedMeta(stuckId, spoolMeta(entityId = stuckId, lastSpooledSeq = 0L, totalSpooledBytes = 16L))
      spool.seedMeta(cleanId, spoolMeta(entityId = cleanId, lastSpooledSeq = 0L, totalSpooledBytes = 16L))
      sessionPort.inspectHandler = _ => Future.successful(
        TestSessionSummary(sessionView(isClosed = true, claimsCount = 1L, lastClaimSequence = 0L))
      )

      val manager = new RecoveryManagerImpl[String, TestSessionSummary, String, TestReplyBinding](
        spool = spool,
        flusherFactory = flusherFactory,
        sessionPort = sessionPort,
        claimPort = claimPort,
        config = makeConfig(recoveryPerEntityTimeout = 100.millis),
        system = testKit.system
      )

      // The predicate hangs forever for `stuckId` and answers normally for `cleanId`.
      val isActive: String => Future[Boolean] = {
        case id if id == stuckId => Promise[Boolean]().future
        case _                   => Future.successful(false)
      }

      val report = manager.reconcileOrphans(isActive).futureValue

      // The clean entity is recovered (closed → cleaned). The stuck entity is
      // skipped this pass — the timeout was treated as "active".
      report.sessionsCleaned shouldBe 1
      report.sessionsRecovered shouldBe 0
      spool.cleanupCalls should contain only cleanId
    }

    // F50-T1 (Track E.3) — a stuck per-entity recovery step must not poison
    // an entire reconciliation pass; the per-entity timeout fires and the
    // entity is contained as a per-entity Failed, while the rest of the
    // batch is recovered normally.
    "reconcileOrphans applies per-entity timeout to a stuck recovery step and contains it as a per-entity failure" in {
      val spool = new RecordingChunkSpool()
      val flusherFactory = new RecordingChunkFlusherFactory()
      val sessionPort = new RecordingSessionPort()
      val claimPort = new RecordingClaimPort()
      val stuckId = "entity-stuck-recover"
      val cleanId = "entity-clean-recover"

      spool.seedMeta(stuckId, spoolMeta(entityId = stuckId, lastSpooledSeq = 0L, totalSpooledBytes = 16L))
      spool.seedMeta(cleanId, spoolMeta(entityId = cleanId, lastSpooledSeq = 0L, totalSpooledBytes = 16L))

      // SessionPort.inspect hangs for `stuckId` (the recovery step calls
      // inspect early; that hang triggers the per-entity timeout). For
      // `cleanId` it returns a closed view so the entity gets cleaned.
      sessionPort.inspectHandler = {
        case id if id == stuckId => Promise[TestSessionSummary]().future
        case _                   => Future.successful(
          TestSessionSummary(sessionView(isClosed = true, claimsCount = 1L, lastClaimSequence = 0L))
        )
      }

      val manager = new RecoveryManagerImpl[String, TestSessionSummary, String, TestReplyBinding](
        spool = spool,
        flusherFactory = flusherFactory,
        sessionPort = sessionPort,
        claimPort = claimPort,
        config = makeConfig(recoveryPerEntityTimeout = 100.millis),
        system = testKit.system
      )

      val report = manager.reconcileOrphans(_ => Future.successful(false)).futureValue

      // Stuck entity is contained as a per-entity Failed; clean entity is
      // cleaned normally; the rest of the batch is unaffected by the
      // wedged inspect call.
      report.sessionsFailed shouldBe 1
      report.sessionsCleaned shouldBe 1
      spool.cleanupCalls should contain only cleanId
    }

    "abort a complete session when drain fails during recovery" in {
      val spool = new RecordingChunkSpool()
      val flusherFactory = new RecordingChunkFlusherFactory()
      val sessionPort = new RecordingSessionPort()
      val claimPort = new RecordingClaimPort()
      val entityId = "entity-drain-fail"
      val meta = spoolMeta(
        entityId = entityId,
        lastSpooledSeq = 1L,
        totalSpooledBytes = 64L,
        flushedSeq = -1L,
        totalExpectedChunks = 2L
      )
      val flusher = new RecordingChunkFlusher(entityId)
      flusher.drainResult = Future.failed(new RuntimeException("drain kaboom"))
      flusherFactory.enqueue(flusher)

      spool.seedMeta(entityId, meta)
      spool.seedChunk(entityId, 0L, "chunk-0".getBytes)
      spool.seedChunk(entityId, 1L, "chunk-1".getBytes)
      sessionPort.inspectHandler = _ => Future.successful(
        TestSessionSummary(sessionView(
          isClosed = false,
          claimsCount = 0L,
          lastClaimSequence = -1L
        ))
      )

      val manager = new RecoveryManagerImpl[String, TestSessionSummary, String, TestReplyBinding](
        spool = spool,
        flusherFactory = flusherFactory,
        sessionPort = sessionPort,
        claimPort = claimPort,
        config = makeConfig(),
        system = testKit.system
      )

      val report = manager.recover().futureValue

      report.sessionsAborted shouldBe 1
      report.sessionsRecovered shouldBe 0
      report.sessionsFailed shouldBe 0
      flusher.stopCount shouldBe 1
      sessionPort.abortCalls should have size 1
      spool.cleanupCalls should contain only entityId
    }
  }
}
