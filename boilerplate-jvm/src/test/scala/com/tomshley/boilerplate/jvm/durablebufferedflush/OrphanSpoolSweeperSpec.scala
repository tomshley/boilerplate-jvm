/*
 * Copyright 2026 Tomshley LLC.
 * All Rights Reserved.
 */

package com.tomshley.boilerplate.jvm.durablebufferedflush

import com.tomshley.boilerplate.jvm.durablebufferedflush.internal.OrphanSpoolSweeperImpl
import com.tomshley.boilerplate.jvm.durablebufferedflush.internal.OrphanSpoolSweeperImpl.SweepOutcome
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
import scala.concurrent.duration.*
import scala.concurrent.{ExecutionContext, Future, Promise}

final class OrphanSpoolSweeperSpec
    extends AnyWordSpec
    with Matchers
    with ScalaFutures
    with Eventually
    with BeforeAndAfterAll {

  override implicit val patienceConfig: PatienceConfig =
    PatienceConfig(timeout = Span(3, Seconds), interval = Span(20, Millis))

  private val testKit = ActorTestKit("OrphanSpoolSweeperSpec")
  private given ExecutionContext = testKit.system.executionContext

  override def afterAll(): Unit = {
    testKit.shutdownTestKit()
    super.afterAll()
  }

  // -- RecordingOrphanReconciler ----------------------------------------------
  //
  // Test double for [[OrphanReconciler]]. State (sweep counter, the next
  // sweep response, the seen `isActive` predicates) lives in a typed actor;
  // the test thread interacts via Future-returning helpers and ! sends.
  // Bonér note: the production [[OrphanReconciler]] / [[OrphanSpoolSweeper]]
  // pair is fully actorized — this fake mirrors the architecture so the
  // tests never reach into shared mutable state, no `var`, no atomic, no
  // `@volatile`.

  private object RecorderActor {
    sealed trait Command
    /** Setter — change the response factory used by subsequent sweeps. */
    final case class SetResponse(factory: () => Future[RecoveryReport]) extends Command
    /** Sent by the production sweeper via the test double; replies on `reply`
      * with the result the response factory produced. */
    final case class Reconcile(
        isActive: String => Future[Boolean],
        reply: Promise[RecoveryReport]
    ) extends Command
    /** Observation queries from the test thread. */
    final case class GetReconcileCount(reply: Promise[Int]) extends Command
    final case class GetIsActiveSeen(reply: Promise[Vector[String => Future[Boolean]]]) extends Command

    def apply(): Behavior[Command] =
      active(
        reconcileCount = 0,
        isActiveSeen = Vector.empty,
        responseFactory = () => Future.successful(RecoveryReport.empty)
      )

    private def active(
        reconcileCount: Int,
        isActiveSeen: Vector[String => Future[Boolean]],
        responseFactory: () => Future[RecoveryReport]
    ): Behavior[Command] =
      Behaviors.receiveMessage {
        case SetResponse(factory) =>
          active(reconcileCount, isActiveSeen, factory)

        case Reconcile(isActive, reply) =>
          // Capture the predicate first so that an observer waiting on
          // `reconcileCount` is guaranteed to see the matching buffer
          // entry once it reads.
          val nextSeen = isActiveSeen :+ isActive
          val nextCount = reconcileCount + 1
          // Materialise the response synchronously here — the response
          // factory must not run concurrent invocations of itself, so
          // we pin it to the actor's single-threaded mailbox.
          reply.completeWith(responseFactory())
          active(nextCount, nextSeen, responseFactory)

        case GetReconcileCount(reply) =>
          reply.trySuccess(reconcileCount)
          Behaviors.same

        case GetIsActiveSeen(reply) =>
          reply.trySuccess(isActiveSeen)
          Behaviors.same
      }
  }

  /** Recording test double for [[OrphanReconciler]] used to verify the
    * sweeper's tick / lifecycle behaviour in isolation from the per-entity
    * reconciliation flow (which is exercised in [[RecoveryManagerImplSpec]]). */
  private final class RecordingOrphanReconciler(system: ActorSystem[?]) extends OrphanReconciler {

    private val ref: ActorRef[RecorderActor.Command] =
      system.systemActorOf(
        RecorderActor(),
        s"orphan-recorder-${UUID.randomUUID()}"
      )

    /** Test-thread mutator. Sets the factory used to produce the result of
      * subsequent `reconcileOrphans` calls. Visibility is via the actor
      * mailbox — there is no shared mutable field. */
    def setReconcileResponse(factory: () => Future[RecoveryReport]): Unit =
      ref ! RecorderActor.SetResponse(factory)

    /** Observation helper — count of `reconcileOrphans` invocations. */
    def reconcileCount(): Future[Int] = {
      val p = Promise[Int]()
      ref ! RecorderActor.GetReconcileCount(p)
      p.future
    }

    /** Observation helper — list of `isActive` predicates seen, in order. */
    def isActiveSeen(): Future[Vector[String => Future[Boolean]]] = {
      val p = Promise[Vector[String => Future[Boolean]]]()
      ref ! RecorderActor.GetIsActiveSeen(p)
      p.future
    }

    override def reconcileOrphans(isActive: String => Future[Boolean]): Future[RecoveryReport] = {
      val reply = Promise[RecoveryReport]()
      ref ! RecorderActor.Reconcile(isActive, reply)
      reply.future
    }
  }

  private def fastConfig(
      enabled: Boolean = true,
      interval: FiniteDuration = 50.millis,
      initialDelay: FiniteDuration = 20.millis,
      maxSweepDuration: FiniteDuration = 5.seconds,
      maxConsecutiveFailures: Int = 3
  ): FlushSweeperConfig =
    FlushSweeperConfig(
      enabled = enabled,
      interval = interval,
      initialDelay = initialDelay,
      maxSweepDuration = maxSweepDuration,
      maxConsecutiveFailures = maxConsecutiveFailures
    )

  "OrphanSpoolSweeper" should {

    "invoke reconcileOrphans on each scheduled tick" in {
      val reconciler = new RecordingOrphanReconciler(testKit.system)
      val sweeper = OrphanSpoolSweeper(
        reconciler = reconciler,
        isActive = _ => Future.successful(false),
        config = fastConfig(),
        system = testKit.system
      )

      try {
        sweeper.start().futureValue
        eventually {
          reconciler.reconcileCount().futureValue should be >= 3
        }
      } finally {
        sweeper.stop().futureValue
      }
    }

    "be a no-op when the config is disabled" in {
      val reconciler = new RecordingOrphanReconciler(testKit.system)
      val sweeper = OrphanSpoolSweeper(
        reconciler = reconciler,
        isActive = _ => Future.successful(false),
        config = fastConfig(enabled = false),
        system = testKit.system
      )

      sweeper.start().futureValue
      Thread.sleep(150)
      reconciler.reconcileCount().futureValue shouldBe 0
      sweeper.stop().futureValue
    }

    "be idempotent on repeated start calls: a single stop fully halts the sweeper" in {
      val reconciler = new RecordingOrphanReconciler(testKit.system)
      val sweeper = OrphanSpoolSweeper(
        reconciler = reconciler,
        isActive = _ => Future.successful(false),
        config = fastConfig(interval = 30.millis, initialDelay = 10.millis),
        system = testKit.system
      )

      try {
        // Three starts in a row — a buggy impl that scheduled a second timer
        // here would survive the single stop() below and keep ticking.
        sweeper.start().futureValue
        sweeper.start().futureValue
        sweeper.start().futureValue

        // Let several ticks fire so we have a meaningful baseline.
        eventually {
          reconciler.reconcileCount().futureValue should be >= 3
        }

        // A single stop must halt every schedule the three start() calls produced.
        sweeper.stop().futureValue
        Thread.sleep(30) // absorb any tick that was already dispatched at cancel time
        val countAtStop = reconciler.reconcileCount().futureValue

        // Several intervals must pass without the count growing.
        Thread.sleep(150)
        reconciler.reconcileCount().futureValue shouldBe countAtStop
      } finally {
        sweeper.stop().futureValue
      }
    }

    "be idempotent on repeated stop calls" in {
      val reconciler = new RecordingOrphanReconciler(testKit.system)
      val sweeper = OrphanSpoolSweeper(
        reconciler = reconciler,
        isActive = _ => Future.successful(false),
        config = fastConfig(),
        system = testKit.system
      )

      sweeper.start().futureValue
      sweeper.stop().futureValue
      sweeper.stop().futureValue
      sweeper.stop().futureValue
    }

    "skip overlapping ticks when a sweep takes longer than the interval" in {
      val reconciler = new RecordingOrphanReconciler(testKit.system)
      val gate = Promise[RecoveryReport]()
      // Hold every sweep open until we release the gate.
      reconciler.setReconcileResponse(() => gate.future)

      val sweeper = OrphanSpoolSweeper(
        reconciler = reconciler,
        isActive = _ => Future.successful(false),
        config = fastConfig(interval = 30.millis, initialDelay = 10.millis),
        system = testKit.system
      )

      try {
        sweeper.start().futureValue
        // Let several ticks fire while the first sweep is still in flight.
        Thread.sleep(200)
        // Exactly one sweep is in flight — the re-entry guard prevents the rest from running.
        reconciler.reconcileCount().futureValue shouldBe 1

        // Releasing the gate completes the in-flight sweep; subsequent ticks are now allowed.
        gate.success(RecoveryReport.empty)
        eventually {
          reconciler.reconcileCount().futureValue should be >= 2
        }
      } finally {
        // Ensure a body failure doesn't leave the in-flight sweep blocked forever.
        gate.trySuccess(RecoveryReport.empty)
        sweeper.stop().futureValue
      }
    }

    "continue ticking after a sweep failure" in {
      val reconciler = new RecordingOrphanReconciler(testKit.system)
      val failuresCounter = new FailureCounter(testKit.system)
      reconciler.setReconcileResponse(() => failuresCounter.next())

      val sweeper = OrphanSpoolSweeper(
        reconciler = reconciler,
        isActive = _ => Future.successful(false),
        config = fastConfig(interval = 40.millis, initialDelay = 10.millis),
        system = testKit.system
      )

      try {
        sweeper.start().futureValue
        eventually {
          reconciler.reconcileCount().futureValue should be >= 4
        }
      } finally {
        sweeper.stop().futureValue
      }
    }

    "prevent concurrent reconciliation across a rapid stop then start sequence" in {
      val reconciler = new RecordingOrphanReconciler(testKit.system)
      val firstSweepStarted = Promise[Unit]()
      val firstSweepGate = Promise[RecoveryReport]()
      val sweepCounter = new FirstCallCounter(testKit.system)

      // First call signals that it has started, then blocks on the gate.
      // Subsequent calls return immediately. Counter state is actor-owned.
      reconciler.setReconcileResponse(() =>
        sweepCounter.fenceFirst(
          first = {
            firstSweepStarted.trySuccess(())
            firstSweepGate.future
          },
          rest = Future.successful(RecoveryReport.empty)
        )
      )

      val sweeper = OrphanSpoolSweeper(
        reconciler = reconciler,
        isActive = _ => Future.successful(false),
        config = fastConfig(interval = 30.millis, initialDelay = 10.millis),
        system = testKit.system
      )

      try {
        sweeper.start().futureValue
        // Wait until the first reconciliation is in flight (gate is holding it).
        firstSweepStarted.future.futureValue

        // Rapid stop → start while the first sweep is still in flight.
        // The leaked runnable from the cancelled schedule and any new runnable
        // from the fresh schedule must not produce two concurrent reconciliations.
        sweeper.stop().futureValue
        sweeper.start().futureValue

        // Several intervals elapse while the gate is still closed.
        // The re-entry guard must prevent a second reconciliation from starting.
        Thread.sleep(150)
        reconciler.reconcileCount().futureValue shouldBe 1

        // Release the gate. Subsequent ticks from the new generation may now proceed.
        firstSweepGate.success(RecoveryReport.empty)
        eventually {
          reconciler.reconcileCount().futureValue should be >= 2
        }
      } finally {
        // Ensure a body failure doesn't leave the first sweep blocked forever.
        firstSweepGate.trySuccess(RecoveryReport.empty)
        sweeper.stop().futureValue
      }
    }

    "pass the supplied isActive predicate through to reconcileOrphans" in {
      val reconciler = new RecordingOrphanReconciler(testKit.system)
      val isActiveFn: String => Future[Boolean] = _ => Future.successful(true)

      val sweeper = OrphanSpoolSweeper(
        reconciler = reconciler,
        isActive = isActiveFn,
        config = fastConfig(interval = 40.millis, initialDelay = 10.millis),
        system = testKit.system
      )

      try {
        sweeper.start().futureValue
        eventually {
          reconciler.reconcileCount().futureValue should be >= 1
        }
        // Verify the same function instance is what the sweeper hands to the reconciler.
        reconciler.isActiveSeen().futureValue.head should be theSameInstanceAs isActiveFn
      } finally {
        sweeper.stop().futureValue
      }
    }

    // F48-T1 (Track E.1) — sweep duration is computed and surfaced on the
    // success path so that a 45-minute sweep on a 15-minute cadence is
    // visible to operators without correlating timestamps. The seam carries
    // the same value the impl writes to its INFO/DEBUG log line.
    "surface durationMs on a successful sweep completion" in {
      val reconciler = new RecordingOrphanReconciler(testKit.system)
      // Hold the sweep open for ~80ms so the recorded durationMs is observably > 0.
      reconciler.setReconcileResponse(() => {
        Thread.sleep(80)
        Future.successful(RecoveryReport(sessionsRecovered = 1, 0, 0, 0, 0L))
      })

      val (sweeper, observer) = observingSweeper(
        reconciler = reconciler,
        config = fastConfig(interval = 200.millis, initialDelay = 10.millis)
      )

      try {
        sweeper.start().futureValue
        // Wait for at least one Succeeded outcome and assert on its durationMs
        // — fishForMessage drives the eventually-style wait on the probe.
        val (durationMs, _) = observer.fishForOutcome(2.seconds) {
          case (_, _: SweepOutcome.Succeeded) => true
          case _                              => false
        }
        durationMs should be >= 70L
      } finally {
        sweeper.stop().futureValue
      }
    }

    // F48-T2 (Track E.1) — the same field is surfaced on the failure path so
    // that a sweep returning a failed Future still produces an observable
    // duration (for diagnosing how long a sick downstream took before
    // failing).
    "surface durationMs on a failed sweep completion" in {
      val reconciler = new RecordingOrphanReconciler(testKit.system)
      reconciler.setReconcileResponse(() => {
        Thread.sleep(60)
        Future.failed(new RuntimeException("synthetic boom"))
      })

      val (sweeper, observer) = observingSweeper(
        reconciler = reconciler,
        config = fastConfig(interval = 200.millis, initialDelay = 10.millis)
      )

      try {
        sweeper.start().futureValue
        val (durationMs, _) = observer.fishForOutcome(2.seconds) {
          case (_, _: SweepOutcome.Failed) => true
          case _                           => false
        }
        durationMs should be >= 50L
      } finally {
        sweeper.stop().futureValue
      }
    }

    // F49-T1 (Track E.2) — sweeps that exceed `maxSweepDuration` count as
    // breaker failures. After `maxConsecutiveFailures` such failures the
    // breaker enters OPEN; subsequent ticks are observed as `FastFailed`
    // outcomes via the seam.
    "trip the circuit breaker after the configured number of consecutive sweep timeouts" in {
      val reconciler = new RecordingOrphanReconciler(testKit.system)
      val gate = Promise[RecoveryReport]()
      // Every sweep blocks indefinitely — every breaker call will hit
      // callTimeout and count as a failure.
      reconciler.setReconcileResponse(() => gate.future)

      val (sweeper, observer) = observingSweeper(
        reconciler = reconciler,
        config = fastConfig(
          interval = 60.millis,
          initialDelay = 10.millis,
          maxSweepDuration = 50.millis,
          maxConsecutiveFailures = 2
        )
      )

      try {
        sweeper.start().futureValue
        // Collect every outcome up to (and including) the first FastFailed.
        // The prefix MUST include at least two `Failed` outcomes — those are
        // the wedged calls that tripped the breaker; using `fishForOutcome`
        // alone here would discard them before the count assertion.
        val outcomes = observer.collectUntil(5.seconds) {
          case (_, SweepOutcome.FastFailed) => true
          case _                            => false
        }
        outcomes.exists {
          case (_, SweepOutcome.FastFailed) => true
          case _                            => false
        } shouldBe true
        outcomes.count {
          case (_, _: SweepOutcome.Failed) => true
          case _                           => false
        } should be >= 2
      } finally {
        gate.trySuccess(RecoveryReport.empty)
        sweeper.stop().futureValue
      }
    }

    // F49-T2 (Track E.2) — once OPEN, subsequent ticks fast-fail through the
    // breaker and never invoke the reconciler. The reconcile counter is the
    // most direct evidence: it stops climbing once the breaker is OPEN.
    "fast-fail subsequent ticks while the circuit breaker is OPEN" in {
      val reconciler = new RecordingOrphanReconciler(testKit.system)
      val gate = Promise[RecoveryReport]()
      reconciler.setReconcileResponse(() => gate.future)

      val (sweeper, observer) = observingSweeper(
        reconciler = reconciler,
        config = fastConfig(
          interval = 30.millis,
          initialDelay = 10.millis,
          maxSweepDuration = 40.millis,
          maxConsecutiveFailures = 2
        )
      )

      try {
        sweeper.start().futureValue
        // Wait for the breaker to OPEN (observed by FastFailed outcomes).
        observer.fishForOutcome(2.seconds) {
          case (_, SweepOutcome.FastFailed) => true
          case _                            => false
        }
        // After the breaker is OPEN the reconciler must not be invoked again
        // until the reset window elapses. Snapshot then confirm the count
        // does not grow over several intervals.
        val countAtTrip = reconciler.reconcileCount().futureValue
        Thread.sleep(120)
        // resetTimeout = interval (30ms), so a half-open trial may fire and
        // bump the count by exactly 1; allow that, but no more.
        reconciler.reconcileCount().futureValue should be <= (countAtTrip + 1)
      } finally {
        gate.trySuccess(RecoveryReport.empty)
        sweeper.stop().futureValue
      }
    }

    // F49-T3 (Track E.2) — after the breaker's `resetTimeout` elapses the
    // breaker enters HALF-OPEN and admits a single trial sweep. A successful
    // trial CLOSES the breaker and the cadence resumes (subsequent ticks
    // surface as Succeeded outcomes).
    "reset the circuit breaker after the configured reset timeout and resume sweeps" in {
      val reconciler = new RecordingOrphanReconciler(testKit.system)
      val recoveryCounter = new FailureCounter(testKit.system, failureLimit = 3)
      // Fail the first three calls (enough to trip the breaker at
      // maxConsecutiveFailures=2). Subsequent calls succeed.
      reconciler.setReconcileResponse(() => recoveryCounter.next())

      val (sweeper, observer) = observingSweeper(
        reconciler = reconciler,
        // resetTimeout = interval here: the half-open trial fires on the next
        // tick after the reset window elapses.
        config = fastConfig(
          interval = 80.millis,
          initialDelay = 10.millis,
          maxSweepDuration = 5.seconds,
          maxConsecutiveFailures = 2
        )
      )

      try {
        sweeper.start().futureValue
        // Collect every outcome up to (and including) the first Succeeded.
        // The prefix MUST include at least two `Failed` outcomes — those
        // tripped the breaker; the eventual Succeeded outcome proves the
        // breaker reset. Using `fishForOutcome` alone here would discard
        // the failures before the count assertion.
        val outcomes = observer.collectUntil(5.seconds) {
          case (_, _: SweepOutcome.Succeeded) => true
          case _                              => false
        }
        outcomes.exists {
          case (_, _: SweepOutcome.Succeeded) => true
          case _                              => false
        } shouldBe true
        outcomes.count {
          case (_, _: SweepOutcome.Failed) => true
          case _                           => false
        } should be >= 2
      } finally {
        sweeper.stop().futureValue
      }
    }
  }

  // -- F48/F49 test infrastructure ------------------------------------------

  /** Subclass of the impl whose only addition is to hand every
    * `onSweepComplete` invocation to a [[TestProbe]]. The probe is the
    * single source of happens-before with the test thread; no atomic, no
    * `synchronized`. */
  private final class TestableSweeper(
      reconciler: OrphanReconciler,
      isActive: String => Future[Boolean],
      config: FlushSweeperConfig,
      system: ActorSystem[?],
      val outcomeProbe: TestProbe[(Long, SweepOutcome)]
  ) extends OrphanSpoolSweeperImpl(reconciler, isActive, config, system) {

    override protected def onSweepComplete(durationMs: Long, outcome: SweepOutcome): Unit =
      outcomeProbe.ref ! ((durationMs, outcome))
  }

  /** Adapter providing assertion-friendly views over the outcome probe.
    * Encapsulates the probe so tests do not reach into TestProbe directly
    * for the common "wait for a matching outcome" pattern. */
  private final class OutcomeObserver(probe: TestProbe[(Long, SweepOutcome)]) {

    /** Collect outcomes for up to `max`, stopping as soon as `predicate`
      * matches one. The returned sequence preserves arrival order and
      * INCLUDES every non-matching outcome the probe received in the
      * meantime — so tests can assert on both "the matching outcome
      * eventually arrived" AND "N non-matching outcomes preceded it".
      *
      * Implementation note: tail-recursive over a `Deadline`; each
      * iteration `receiveMessage`s with the remaining budget. No `var`,
      * no atomic — the actor mailbox provides the happens-before. */
    def collectUntil(max: FiniteDuration)(
        predicate: ((Long, SweepOutcome)) => Boolean
    ): Vector[(Long, SweepOutcome)] = {
      val deadline = max.fromNow

      @scala.annotation.tailrec
      def loop(acc: Vector[(Long, SweepOutcome)]): Vector[(Long, SweepOutcome)] = {
        val left = deadline.timeLeft
        if (left.toMillis <= 0L) acc
        else scala.util.Try(probe.receiveMessage(left)).toOption match {
          case Some(msg) =>
            val next = acc :+ msg
            if (predicate(msg)) next else loop(next)
          case None => acc
        }
      }
      loop(Vector.empty)
    }

    /** Wait up to `max` for an outcome that satisfies `predicate`. Earlier
      * non-matching outcomes are DISCARDED — use [[collectUntil]] when you
      * also need to count or inspect the prefix. */
    def fishForOutcome(max: FiniteDuration)(
        predicate: ((Long, SweepOutcome)) => Boolean
    ): (Long, SweepOutcome) =
      probe.fishForMessage(max) {
        case msg if predicate(msg) =>
          FishingOutcomes.complete
        case _ =>
          FishingOutcomes.continueAndIgnore
      }.last
  }

  private def observingSweeper(
      reconciler: OrphanReconciler,
      config: FlushSweeperConfig,
      isActive: String => Future[Boolean] = _ => Future.successful(false)
  ): (TestableSweeper, OutcomeObserver) = {
    val probe = testKit.createTestProbe[(Long, SweepOutcome)]("sweep-outcomes")
    val sweeper = new TestableSweeper(reconciler, isActive, config, testKit.system, probe)
    (sweeper, new OutcomeObserver(probe))
  }

  // -- Failure / first-call counters (actor-backed) ---------------------------
  //
  // Tiny helpers used by tests that need "the Nth call returns X, the rest
  // return Y" or "the first call does X, the rest do Y." Counter state lives
  // in a typed actor — same pattern as RecorderActor, scoped to the helper.

  private object CounterActor {
    sealed trait Command
    final case class IncrementAndReply(reply: Promise[Int]) extends Command

    def apply(): Behavior[Command] = active(0)
    private def active(count: Int): Behavior[Command] =
      Behaviors.receiveMessage {
        case IncrementAndReply(reply) =>
          val next = count + 1
          reply.trySuccess(next)
          active(next)
      }
  }

  /** "Fail the first `failureLimit` calls; succeed thereafter."
    * State is the call counter; the actor mailbox serialises increments. */
  private final class FailureCounter(system: ActorSystem[?], failureLimit: Int = 2) {
    private val ref = system.systemActorOf(
      CounterActor(),
      s"failure-counter-${UUID.randomUUID()}"
    )

    def next(): Future[RecoveryReport] = {
      val p = Promise[Int]()
      ref ! CounterActor.IncrementAndReply(p)
      p.future.flatMap { n =>
        if (n <= failureLimit)
          Future.failed(new RuntimeException(s"sick downstream attempt $n"))
        else
          Future.successful(RecoveryReport(sessionsRecovered = 1, 0, 0, 0, 0L))
      }(system.executionContext)
    }
  }

  /** "First call uses `first`; every other call uses `rest`."
    * Intended for the rapid-stop-then-start test: only the first call needs
    * to fence on a Promise, every subsequent call is a fast no-op. */
  private final class FirstCallCounter(system: ActorSystem[?]) {
    private val ref = system.systemActorOf(
      CounterActor(),
      s"first-call-counter-${UUID.randomUUID()}"
    )

    def fenceFirst(
        first: => Future[RecoveryReport],
        rest: => Future[RecoveryReport]
    ): Future[RecoveryReport] = {
      val p = Promise[Int]()
      ref ! CounterActor.IncrementAndReply(p)
      p.future.flatMap { n =>
        if (n == 1) first else rest
      }(system.executionContext)
    }
  }
}
