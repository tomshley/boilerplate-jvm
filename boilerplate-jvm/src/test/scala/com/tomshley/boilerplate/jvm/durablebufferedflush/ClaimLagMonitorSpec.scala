package com.tomshley.boilerplate.jvm.durablebufferedflush

import org.apache.pekko.actor.testkit.typed.scaladsl.ActorTestKit
import org.apache.pekko.actor.typed.ActorSystem
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

final class ClaimLagMonitorSpec
    extends AnyWordSpec
    with Matchers
    with ScalaFutures
    with BeforeAndAfterAll {

  private val testKit = ActorTestKit("ClaimLagMonitorSpec")
  private given ActorSystem[?] = testKit.system

  override def afterAll(): Unit = {
    testKit.shutdownTestKit()
    super.afterAll()
  }

  "ClaimLagMonitor" should {

    "reject negative external sequence and claims-count inputs" in {
      val monitor = new ClaimLagMonitor(claimLagSoft = 2L, claimLagHard = 4L)

      monitor.onSpooled(-1L).failed.futureValue shouldBe a[IllegalArgumentException]
      monitor.onClaimAttempted(-1L).failed.futureValue shouldBe a[IllegalArgumentException]
      monitor.onClaimConfirmed(-1L).failed.futureValue shouldBe a[IllegalArgumentException]
    }

    "enter pause only after crossing the hard threshold and clear it once lag drops below soft threshold" in {
      val monitor = new ClaimLagMonitor(claimLagSoft = 2L, claimLagHard = 4L)

      monitor.onSpooled(3L).futureValue shouldBe ()
      monitor.onClaimAttempted(3L).futureValue shouldBe ()
      monitor.pauseIfNeeded().futureValue shouldBe None

      monitor.onSpooled(5L).futureValue shouldBe ()
      monitor.onClaimAttempted(5L).futureValue shouldBe ()
      val initialPause = monitor.enterPause().futureValue.getOrElse(fail("expected active pause"))

      initialPause.isCompleted shouldBe false
      monitor.isPaused.futureValue shouldBe true
      monitor.shouldPause.futureValue shouldBe true

      monitor.onClaimConfirmed(6L).futureValue shouldBe ()
      initialPause.futureValue shouldBe ()
      monitor.isPaused.futureValue shouldBe false
      monitor.shouldResume.futureValue shouldBe true
      monitor.claimLag.futureValue shouldBe 0L
      monitor.inflightClaims.futureValue shouldBe 0L
    }

    "not create a fresh pause after lag has already recovered" in {
      val monitor = new ClaimLagMonitor(claimLagSoft = 2L, claimLagHard = 4L)

      monitor.onSpooled(5L).futureValue shouldBe ()
      monitor.onClaimAttempted(5L).futureValue shouldBe ()
      val initialPause = monitor.enterPause().futureValue.getOrElse(fail("expected active pause"))

      initialPause.isCompleted shouldBe false
      monitor.onClaimConfirmed(6L).futureValue shouldBe ()
      initialPause.futureValue shouldBe ()

      monitor.enterPause().futureValue shouldBe None
      monitor.pauseIfNeeded().futureValue shouldBe None
      monitor.isPaused.futureValue shouldBe false
    }

    "cancel an active pause and surface the cancellation cause" in {
      val monitor = new ClaimLagMonitor(claimLagSoft = 2L, claimLagHard = 4L)

      monitor.onSpooled(5L).futureValue shouldBe ()
      monitor.onClaimAttempted(5L).futureValue shouldBe ()
      val pauseFuture = monitor.enterPause().futureValue.getOrElse(fail("expected active pause"))
      val cause = new RuntimeException("disconnect")

      monitor.cancelPause(cause).futureValue shouldBe ()

      pauseFuture.failed.futureValue shouldBe cause
      monitor.isPaused.futureValue shouldBe false
    }

    "reset clear counters and release any active pause" in {
      val monitor = new ClaimLagMonitor(claimLagSoft = 2L, claimLagHard = 4L)

      monitor.onSpooled(5L).futureValue shouldBe ()
      monitor.onClaimAttempted(5L).futureValue shouldBe ()
      monitor.onClaimError().futureValue shouldBe ()
      val pauseFuture = monitor.enterPause().futureValue.getOrElse(fail("expected active pause"))

      monitor.reset().futureValue shouldBe ()

      pauseFuture.futureValue shouldBe ()
      monitor.snapshot().futureValue shouldBe ClaimLagSnapshot(
        spooledSeq = -1L,
        lastClaimAttemptedSeq = -1L,
        lastClaimConfirmedSeq = -1L,
        claimErrorCount = 0L,
        claimLagSoft = 2L,
        claimLagHard = 4L,
        isPaused = false
      )
    }
  }
}
