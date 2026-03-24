package com.tomshley.boilerplate.jvm.durablebufferedflush

import com.tomshley.boilerplate.jvm.durablebufferedflush.internal.RecoveryManagerImpl
import org.apache.pekko.actor.testkit.typed.scaladsl.ActorTestKit
import org.apache.pekko.actor.typed.ActorSystem
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{Millis, Seconds, Span}
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.ExecutionContext
import scala.concurrent.Future

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
