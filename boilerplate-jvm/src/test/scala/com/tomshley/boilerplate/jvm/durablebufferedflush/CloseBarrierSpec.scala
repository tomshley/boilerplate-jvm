package com.tomshley.boilerplate.jvm.durablebufferedflush

import com.tomshley.boilerplate.jvm.durablebufferedflush.CloseValidationFailure
import com.tomshley.boilerplate.jvm.durablebufferedflush.internal.CloseBarrier
import org.apache.pekko.actor.testkit.typed.scaladsl.ActorTestKit
import org.apache.pekko.actor.typed.ActorSystem
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{Millis, Seconds, Span}
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.duration.*

final class DefaultFlushCloseBarrierSpec
    extends AnyWordSpec
    with Matchers
    with ScalaFutures
    with BeforeAndAfterAll
    with FlushSpecSupport {

  override implicit val patienceConfig: PatienceConfig =
    PatienceConfig(timeout = Span(5, Seconds), interval = Span(50, Millis))

  private val testKit = ActorTestKit("DefaultFlushCloseBarrierSpec")
  private given ActorSystem[?] = testKit.system
  private given ExecutionContext = testKit.system.executionContext

  override def afterAll(): Unit = {
    testKit.shutdownTestKit()
    super.afterAll()
  }

  "DefaultFlushCloseBarrier" should {

    "resend only the missing claims from the spool" in {
      val spool = new RecordingChunkSpool()
      val sessionPort = new RecordingSessionPort()
      val claimPort = new RecordingClaimPort()
      val entityId = "entity-close-resend"

      spool.seedChunk(entityId, 1L, "chunk-1".getBytes)
      spool.seedChunk(entityId, 2L, "chunk-2".getBytes)
      sessionPort.inspectHandler = _ => Future.successful(
        TestSessionSummary(sessionView(lastClaimSequence = 0L, claimsCount = 1L))
      )

      val barrier = new CloseBarrier[String, TestSessionSummary, String, TestReplyBinding](
        spool = spool,
        sessionPort = sessionPort,
        claimPort = claimPort,
        config = makeConfig(),
        system = testKit.system
      )
      val replyBinding = claimPort.openReplyBinding(_ => (), _ => ())

      barrier.resendMissingClaims(entityId, lastSpooledSeq = 2L, replyBinding).futureValue shouldBe ()

      claimPort.dispatchCalls.map(call => call.entityId -> call.envelope) shouldBe Seq(
        entityId -> "chunk-1",
        entityId -> "chunk-2"
      )
      spool.readChunkCalls shouldBe Seq(entityId -> 1L, entityId -> 2L)
    }

    "retry a recoverable close validation failure after resending missing claims" in {
      val spool = new RecordingChunkSpool()
      val sessionPort = new RecordingSessionPort()
      val claimPort = new RecordingClaimPort()
      val entityId = "entity-close-retry"
      var closeAttempts = 0

      spool.seedChunk(entityId, 1L, "chunk-1".getBytes)
      spool.seedChunk(entityId, 2L, "chunk-2".getBytes)
      sessionPort.inspectHandler = _ => Future.successful(
        TestSessionSummary(sessionView(lastClaimSequence = 0L, claimsCount = 1L))
      )
      sessionPort.closeHandler = call => {
        closeAttempts += 1
        if (closeAttempts == 1) {
          Future.failed(CloseValidationFailure.ClaimsCountMismatch(entity = 1L, expected = 3L))
        } else {
          Future.successful(
            TestSessionSummary(sessionView(
              isClosed = true,
              claimsCount = call.expectedClaimsCount,
              totalClaimedBytes = call.expectedTotalBytes,
              lastClaimSequence = call.expectedLastSequence
            ))
          )
        }
      }

      val barrier = new CloseBarrier[String, TestSessionSummary, String, TestReplyBinding](
        spool = spool,
        sessionPort = sessionPort,
        claimPort = claimPort,
        config = makeConfig(closeRetryDelay = 10.millis, closeMaxRetries = 2),
        system = testKit.system
      )
      val replyBinding = claimPort.openReplyBinding(_ => (), _ => ())

      val summary = barrier.closeWithRetry(
        entityId = entityId,
        lastSpooledSeq = 2L,
        expectedClaimsCount = 3L,
        expectedTotalBytes = 96L,
        expectedLastSequence = 2L,
        replyBinding = replyBinding
      ).futureValue

      summary.view.isClosed shouldBe true
      sessionPort.closeCalls should have size 2
      claimPort.dispatchCalls.map(_.envelope) shouldBe Seq("chunk-1", "chunk-2")
    }

    "propagate fatal close validation failures without resending claims" in {
      val spool = new RecordingChunkSpool()
      val sessionPort = new RecordingSessionPort()
      val claimPort = new RecordingClaimPort()
      val entityId = "entity-close-fatal"

      sessionPort.closeHandler = _ =>
        Future.failed(CloseValidationFailure.BytesMismatch(entity = 7L, expected = 8L))

      val barrier = new CloseBarrier[String, TestSessionSummary, String, TestReplyBinding](
        spool = spool,
        sessionPort = sessionPort,
        claimPort = claimPort,
        config = makeConfig(closeRetryDelay = 10.millis, closeMaxRetries = 2),
        system = testKit.system
      )
      val replyBinding = claimPort.openReplyBinding(_ => (), _ => ())

      val failure = barrier.closeWithRetry(
        entityId = entityId,
        lastSpooledSeq = 2L,
        expectedClaimsCount = 3L,
        expectedTotalBytes = 96L,
        expectedLastSequence = 2L,
        replyBinding = replyBinding
      ).failed.futureValue

      failure shouldBe CloseValidationFailure.BytesMismatch(entity = 7L, expected = 8L)
      sessionPort.closeCalls should have size 1
      sessionPort.inspectCalls shouldBe empty
      claimPort.dispatchCalls shouldBe empty
    }

    "resend claims in batched order preserving sequence when parallelism is 1" in {
      val spool = new RecordingChunkSpool()
      val sessionPort = new RecordingSessionPort()
      val claimPort = new RecordingClaimPort()
      val entityId = "entity-batched-resend"

      (0L to 4L).foreach { seq =>
        spool.seedChunk(entityId, seq, s"chunk-$seq".getBytes)
      }
      sessionPort.inspectHandler = _ => Future.successful(
        TestSessionSummary(sessionView(lastClaimSequence = -1L, claimsCount = 0L))
      )

      val barrier = new CloseBarrier[String, TestSessionSummary, String, TestReplyBinding](
        spool = spool,
        sessionPort = sessionPort,
        claimPort = claimPort,
        config = makeConfig(recoveryParallelism = 1),
        system = testKit.system
      )
      val replyBinding = claimPort.openReplyBinding(_ => (), _ => ())

      barrier.resendMissingClaims(entityId, lastSpooledSeq = 4L, replyBinding).futureValue shouldBe ()

      claimPort.dispatchCalls.map(_.envelope) shouldBe Seq("chunk-0", "chunk-1", "chunk-2", "chunk-3", "chunk-4")
    }

    "fail fast when a chunk decode fails during resend" in {
      val spool = new RecordingChunkSpool()
      val sessionPort = new RecordingSessionPort()
      val claimPort = new RecordingClaimPort()
      val entityId = "entity-decode-fail"

      spool.seedChunk(entityId, 0L, "good".getBytes)
      spool.seedChunk(entityId, 1L, "bad".getBytes)
      spool.seedChunk(entityId, 2L, "also-good".getBytes)
      sessionPort.inspectHandler = _ => Future.successful(
        TestSessionSummary(sessionView(lastClaimSequence = -1L, claimsCount = 0L))
      )
      claimPort.decodeHandler = bytes => {
        val s = new String(bytes)
        if (s == "bad") scala.util.Failure(new RuntimeException("corrupt envelope"))
        else scala.util.Success(s)
      }

      val barrier = new CloseBarrier[String, TestSessionSummary, String, TestReplyBinding](
        spool = spool,
        sessionPort = sessionPort,
        claimPort = claimPort,
        config = makeConfig(recoveryParallelism = 1),
        system = testKit.system
      )
      val replyBinding = claimPort.openReplyBinding(_ => (), _ => ())

      val ex = barrier.resendMissingClaims(entityId, lastSpooledSeq = 2L, replyBinding).failed.futureValue
      ex.getMessage should include("corrupt envelope")
    }

    "resend is a no-op when all claims are already confirmed" in {
      val spool = new RecordingChunkSpool()
      val sessionPort = new RecordingSessionPort()
      val claimPort = new RecordingClaimPort()
      val entityId = "entity-all-confirmed"

      sessionPort.inspectHandler = _ => Future.successful(
        TestSessionSummary(sessionView(lastClaimSequence = 3L, claimsCount = 4L))
      )

      val barrier = new CloseBarrier[String, TestSessionSummary, String, TestReplyBinding](
        spool = spool,
        sessionPort = sessionPort,
        claimPort = claimPort,
        config = makeConfig(),
        system = testKit.system
      )
      val replyBinding = claimPort.openReplyBinding(_ => (), _ => ())

      barrier.resendMissingClaims(entityId, lastSpooledSeq = 3L, replyBinding).futureValue shouldBe ()

      claimPort.dispatchCalls shouldBe empty
      spool.readChunkCalls shouldBe empty
    }

    "fail fast when entity claim state is not contiguous" in {
      val spool = new RecordingChunkSpool()
      val sessionPort = new RecordingSessionPort()
      val claimPort = new RecordingClaimPort()
      val entityId = "entity-non-contiguous"

      sessionPort.inspectHandler = _ => Future.successful(
        TestSessionSummary(sessionView(lastClaimSequence = 2L, claimsCount = 2L))
      )

      val barrier = new CloseBarrier[String, TestSessionSummary, String, TestReplyBinding](
        spool = spool,
        sessionPort = sessionPort,
        claimPort = claimPort,
        config = makeConfig(),
        system = testKit.system
      )
      val replyBinding = claimPort.openReplyBinding(_ => (), _ => ())

      val failure = barrier.resendMissingClaims(entityId, lastSpooledSeq = 3L, replyBinding).failed.futureValue

      failure shouldBe a[IllegalStateException]
      failure.getMessage should include("Non-contiguous claim state")
      spool.readChunkCalls shouldBe empty
      claimPort.dispatchCalls shouldBe empty
    }
  }
}
