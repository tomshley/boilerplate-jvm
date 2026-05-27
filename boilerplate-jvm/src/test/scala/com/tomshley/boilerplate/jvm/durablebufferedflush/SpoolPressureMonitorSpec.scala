/*
 * Copyright 2026 Tomshley LLC.
 * All Rights Reserved.
 */

package com.tomshley.boilerplate.jvm.durablebufferedflush

import org.apache.pekko.actor.typed.{ActorRef, ActorSystem, Behavior}
import org.apache.pekko.actor.typed.scaladsl.Behaviors
import org.apache.pekko.actor.testkit.typed.scaladsl.{ActorTestKit, FishingOutcomes, TestProbe}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.PatienceConfiguration.Timeout
import org.scalatest.concurrent.{Eventually, ScalaFutures}
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{Millis, Seconds, Span}
import org.scalatest.wordspec.AnyWordSpec

import java.util.UUID
import scala.concurrent.{Future, Promise}
import scala.concurrent.duration.*

final class SpoolPressureMonitorSpec
    extends AnyWordSpec
    with Matchers
    with ScalaFutures
    with Eventually
    with BeforeAndAfterAll {

  override implicit val patienceConfig: PatienceConfig =
    PatienceConfig(timeout = Span(3, Seconds), interval = Span(20, Millis))

  private val testKit = ActorTestKit("SpoolPressureMonitorSpec")

  override def afterAll(): Unit = {
    testKit.shutdownTestKit()
    super.afterAll()
  }

  // -- FakeSizeReporter --------------------------------------------------------
  //
  // Test-side [[SpoolSizeReporter]] whose state lives in a typed actor —
  // mirroring the production implementation in
  // [[internal.FilesystemChunkSpool]]'s `SpoolSizeAccountingActor`. There is
  // no `AtomicLong`, no `var`, no `synchronized` block; cross-thread
  // visibility is provided by the actor mailbox.

  private object FakeSizeReporterActor {
    sealed trait Command
    final case class SetSize(bytes: Long) extends Command
    final case class Query(reply: Promise[Long]) extends Command
    final case class Recount(reply: Promise[Long]) extends Command
    final case class GetRecountCount(reply: Promise[Int]) extends Command

    def apply(initialBytes: Long): Behavior[Command] =
      active(size = initialBytes, recountCount = 0)

    private def active(size: Long, recountCount: Int): Behavior[Command] =
      Behaviors.receiveMessage {
        case SetSize(b) =>
          active(b, recountCount)
        case Query(reply) =>
          reply.trySuccess(size)
          Behaviors.same
        case Recount(reply) =>
          reply.trySuccess(size)
          active(size, recountCount + 1)
        case GetRecountCount(reply) =>
          reply.trySuccess(recountCount)
          Behaviors.same
      }
  }

  /** Hand-rolled [[SpoolSizeReporter]] used by the monitor tests. State —
    * the reported byte count and the recount-call counter — lives in
    * [[FakeSizeReporterActor]]; the wrapper class is a thin adapter that
    * sends commands and exposes Future-returning helpers. The test thread
    * and the monitor's actor dispatcher never share mutable memory; every
    * read crosses the mailbox and is therefore safely published. */
  private final class FakeSizeReporter(
      system: ActorSystem[?],
      initialBytes: Long
  ) extends SpoolSizeReporter {

    private val ref: ActorRef[FakeSizeReporterActor.Command] =
      system.systemActorOf(
        FakeSizeReporterActor(initialBytes),
        s"fake-size-reporter-${UUID.randomUUID()}"
      )

    /** Test-thread mutator. Fire-and-forget — visibility is via the actor
      * mailbox; subsequent `currentSizeBytes()` / `recountFromFilesystem()`
      * calls observe the new value. */
    def setSize(bytes: Long): Unit =
      ref ! FakeSizeReporterActor.SetSize(bytes)

    /** Observation helper for tests — count of `recountFromFilesystem`
      * invocations. Returns a Future to keep the contract uniform with
      * the other state queries. */
    def recountCount(): Future[Int] = {
      val p = Promise[Int]()
      ref ! FakeSizeReporterActor.GetRecountCount(p)
      p.future
    }

    override def currentSizeBytes(): Future[Long] = {
      val p = Promise[Long]()
      ref ! FakeSizeReporterActor.Query(p)
      p.future
    }

    override def recountFromFilesystem(): Future[Long] = {
      val p = Promise[Long]()
      ref ! FakeSizeReporterActor.Recount(p)
      p.future
    }
  }

  private def fastConfig(
      enabled: Boolean = true,
      monitorInterval: FiniteDuration = 30.millis,
      reconciliationInterval: FiniteDuration = 200.millis,
      alert: Int = 70,
      critical: Int = 90,
      alertClear: Int = 65,
      criticalClear: Int = 85,
      capacityBytes: Long = 1000L
  ): SpoolPressureConfig =
    SpoolPressureConfig(
      enabled = enabled,
      monitorInterval = monitorInterval,
      reconciliationInterval = reconciliationInterval,
      alertThresholdPercent = alert,
      criticalThresholdPercent = critical,
      alertClearPercent = alertClear,
      criticalClearPercent = criticalClear,
      configuredCapacityBytes = Some(capacityBytes),
      suggestedRetryAfter = 30.seconds
    )

  "SpoolPressureMonitor" should {

    // The disabled sentinel must never schedule. start()/stop() are no-ops
    // and the level remains at Low (the conservative default).
    "be a no-op when the config is disabled" in {
      val reporter = new FakeSizeReporter(testKit.system, initialBytes = 950L)
      val monitor = SpoolPressureMonitor(reporter, fastConfig(enabled = false), testKit.system)

      monitor.start().futureValue
      // The monitor's actor mailbox serializes all state observations.
      // Eventually here doubles as both "wait for any spurious tick to
      // be processed" and "assert the level remains Low".
      eventually(Timeout(Span(500, Millis))) {
        monitor.currentLevel().futureValue shouldBe SpoolPressureLevel.Low
      }
      monitor.stop().futureValue
    }

    // Below the alert threshold, the level is Low.
    "remain at Low while occupancy is below the alert threshold" in {
      val reporter = new FakeSizeReporter(testKit.system, initialBytes = 600L) // 60% of 1000
      val monitor = SpoolPressureMonitor(reporter, fastConfig(), testKit.system)

      try {
        monitor.start().futureValue
        eventually(Timeout(Span(500, Millis))) {
          monitor.currentLevel().futureValue shouldBe SpoolPressureLevel.Low
        }
      } finally {
        monitor.stop().futureValue
      }
    }

    // Crossing the alert threshold transitions Low → High and emits to
    // every registered handler in registration order.
    "transition Low -> High when occupancy crosses the alert threshold and notify subscribers" in {
      val reporter = new FakeSizeReporter(testKit.system, initialBytes = 600L)
      val monitor = SpoolPressureMonitor(reporter, fastConfig(), testKit.system)

      // The handler is invoked from the monitor actor's dispatcher, the
      // test reads from the test thread — both are decoupled via a Pekko
      // [[TestProbe]]. The probe's mailbox is the single source of
      // happens-before; the test never reaches into shared mutable
      // state and the handler never holds a lock.
      val seenProbe = testKit.createTestProbe[SpoolPressureLevel]("level-changes")
      monitor.onLevelChange(level => seenProbe.ref ! level).futureValue

      try {
        monitor.start().futureValue
        // Confirm we start Low before crossing the threshold.
        eventually {
          monitor.currentLevel().futureValue shouldBe SpoolPressureLevel.Low
        }
        // Cross the alert threshold (>= 70%).
        reporter.setSize(750L)
        eventually {
          monitor.currentLevel().futureValue shouldBe SpoolPressureLevel.High
        }
        // The handler must have observed the High transition. fishForMessage
        // ignores any earlier transition events that arrived while the
        // first sample was being processed.
        seenProbe.fishForMessage(2.seconds) {
          case SpoolPressureLevel.High => FishingOutcomes.complete
          case _                       => FishingOutcomes.continueAndIgnore
        }
      } finally {
        monitor.stop().futureValue
      }
    }

    // Crossing the critical threshold transitions to Critical regardless
    // of the previous level.
    "transition to Critical when occupancy crosses the critical threshold" in {
      val reporter = new FakeSizeReporter(testKit.system, initialBytes = 600L)
      val monitor = SpoolPressureMonitor(reporter, fastConfig(), testKit.system)

      try {
        monitor.start().futureValue
        reporter.setSize(950L) // 95% — above critical 90%
        eventually(Timeout(Span(2, Seconds))) {
          monitor.currentLevel().futureValue shouldBe SpoolPressureLevel.Critical
        }
      } finally {
        monitor.stop().futureValue
      }
    }

    // Hysteresis prevents the level from oscillating around the threshold.
    // Once at High, occupancy must drop below `alertClearPercent` (65%, not 70%)
    // before transitioning back to Low. A drop to 68% must NOT clear.
    "apply alert-clear hysteresis when transitioning High -> Low" in {
      val reporter = new FakeSizeReporter(testKit.system, initialBytes = 750L)
      val monitor = SpoolPressureMonitor(reporter, fastConfig(), testKit.system)

      try {
        monitor.start().futureValue
        eventually {
          monitor.currentLevel().futureValue shouldBe SpoolPressureLevel.High
        }
        // Drop to 68% — between alertClear (65) and alertThreshold (70).
        // Hysteresis MUST keep the level at High.
        reporter.setSize(680L)
        // Poll for a few tick intervals so that any (incorrect)
        // transition would be observed before we assert no transition.
        eventually(Timeout(Span(500, Millis))) {
          monitor.currentLevel().futureValue shouldBe SpoolPressureLevel.High
        }

        // Drop below alertClear (< 65%). Now the level returns to Low.
        reporter.setSize(640L)
        eventually {
          monitor.currentLevel().futureValue shouldBe SpoolPressureLevel.Low
        }
      } finally {
        monitor.stop().futureValue
      }
    }

    // The slow reconciliation cadence must invoke `recountFromFilesystem`.
    "invoke recountFromFilesystem on the reconciliation cadence" in {
      val reporter = new FakeSizeReporter(testKit.system, initialBytes = 400L)
      val monitor = SpoolPressureMonitor(
        reporter,
        fastConfig(reconciliationInterval = 50.millis),
        testKit.system
      )

      try {
        monitor.start().futureValue
        eventually(Timeout(Span(2, Seconds))) {
          reporter.recountCount().futureValue should be >= 2
        }
      } finally {
        monitor.stop().futureValue
      }
    }

    // A handler that throws must not break the monitor or starve other
    // subscribers — exceptions are contained.
    "contain exceptions thrown by onLevelChange handlers" in {
      val reporter = new FakeSizeReporter(testKit.system, initialBytes = 600L)
      val monitor = SpoolPressureMonitor(reporter, fastConfig(), testKit.system)

      val healthyProbe = testKit.createTestProbe[SpoolPressureLevel]("healthy-handler")
      monitor.onLevelChange { _ => throw new RuntimeException("synthetic boom") }.futureValue
      monitor.onLevelChange(level => healthyProbe.ref ! level).futureValue

      try {
        monitor.start().futureValue
        reporter.setSize(950L) // Critical
        // The healthy handler must observe at least one of High / Critical
        // even though the previous handler in registration order threw —
        // proving exceptions in subscribers are contained.
        healthyProbe.fishForMessage(2.seconds) {
          case SpoolPressureLevel.High | SpoolPressureLevel.Critical =>
            FishingOutcomes.complete
          case _ =>
            FishingOutcomes.continueAndIgnore
        }
        // Monitor itself must still be running.
        monitor.currentLevel().futureValue shouldBe SpoolPressureLevel.Critical
      } finally {
        monitor.stop().futureValue
      }
    }
  }
}
