/*
 * Copyright 2026 Tomshley LLC.
 * All Rights Reserved.
 */

package com.tomshley.boilerplate.jvm.durablebufferedflush.internal

import com.tomshley.boilerplate.jvm.durablebufferedflush.{
  FlushSweeperConfig,
  OrphanReconciler,
  OrphanSpoolSweeper,
  RecoveryReport
}
import org.apache.pekko.actor.typed.scaladsl.{Behaviors, TimerScheduler}
import org.apache.pekko.actor.typed.{ActorSystem, Behavior}
import org.apache.pekko.pattern.{CircuitBreaker, CircuitBreakerOpenException}
import org.slf4j.Logger

import java.util.UUID
import scala.concurrent.{Future, Promise}
import scala.util.{Failure, Success, Try}
import scala.util.control.NonFatal

private object OrphanSpoolSweeperActor {

  sealed trait Command
  final case class Start(reply: Promise[Unit]) extends Command
  final case class Stop(reply: Promise[Unit]) extends Command
  private case object Tick extends Command
  private final case class SweepCompleted(
      durationMs: Long,
      result: Try[RecoveryReport]
  ) extends Command

  /** Timer key for the periodic sweep tick. Using a singleton key means
    * `timers.cancel(TickKey)` reliably cancels the active schedule
    * regardless of how many `Start`/`Stop` cycles have run. */
  private case object TickKey

  /** Behavior factory. The actor owns its lifecycle: state lives in the
    * behavior parameter, the timer is provided by `Behaviors.withTimers`,
    * and sweep completion arrives via `context.pipeToSelf` (no raw
    * Future callbacks inside the actor). */
  def apply(
      reconciler: OrphanReconciler,
      isActive: String => Future[Boolean],
      config: FlushSweeperConfig,
      breaker: CircuitBreaker,
      onSweepComplete: (Long, OrphanSpoolSweeperImpl.SweepOutcome) => Unit
  ): Behavior[Command] =
    Behaviors.setup { context =>
      Behaviors.withTimers { timers =>
        active(
          timerOn = false,
          inFlight = false,
          timers,
          reconciler,
          isActive,
          config,
          breaker,
          onSweepComplete,
          context.log
        )
      }
    }

  /** The actor's single behavior, parameterised by two orthogonal
    * boolean axes:
    *  - `timerOn`  — whether the periodic [[Tick]] timer is currently
    *                 scheduled. Flipped by `Start` / `Stop`.
    *  - `inFlight` — whether a sweep is currently in-flight via
    *                 `pipeToSelf`. Flipped by `Tick` (dispatch) and
    *                 `SweepCompleted` (terminal).
    *
    * The two are deliberately independent so that a rapid Stop → Start
    * across an in-flight sweep does not lose the in-flight discipline:
    * a fresh tick after restart still sees `inFlight = true` and skips
    * with a DEBUG log, exactly as if no Stop had ever happened. The
    * test seam contract is unaffected — every sweep that started gets
    * exactly one terminal log and seam invocation. */
  private def active(
      timerOn: Boolean,
      inFlight: Boolean,
      timers: TimerScheduler[Command],
      reconciler: OrphanReconciler,
      isActive: String => Future[Boolean],
      config: FlushSweeperConfig,
      breaker: CircuitBreaker,
      onSweepComplete: (Long, OrphanSpoolSweeperImpl.SweepOutcome) => Unit,
      log: Logger
  ): Behavior[Command] = {
    def next(nextTimerOn: Boolean, nextInFlight: Boolean): Behavior[Command] =
      active(
        nextTimerOn,
        nextInFlight,
        timers,
        reconciler,
        isActive,
        config,
        breaker,
        onSweepComplete,
        log
      )

    Behaviors.receive { (context, message) =>
      message match {
        case Start(reply) if timerOn =>
          log.debug("OrphanSpoolSweeper.start ignored — already running")
          reply.trySuccess(())
          Behaviors.same

        case Start(reply) =>
          reply.trySuccess(())
          if (!config.enabled) {
            log.info("OrphanSpoolSweeper disabled by config — start() is a no-op")
            Behaviors.same
          } else {
            timers.startTimerWithFixedDelay(
              TickKey,
              Tick,
              config.initialDelay,
              config.interval
            )
            log.info(
              "OrphanSpoolSweeper started: interval={}, initialDelay={}, maxSweepDuration={}, maxConsecutiveFailures={}",
              config.interval,
              config.initialDelay,
              config.maxSweepDuration,
              config.maxConsecutiveFailures
            )
            next(nextTimerOn = true, nextInFlight = inFlight)
          }

        case Stop(reply) =>
          reply.trySuccess(())
          if (timerOn) {
            timers.cancel(TickKey)
            log.info("OrphanSpoolSweeper stopped")
            next(nextTimerOn = false, nextInFlight = inFlight)
          } else {
            Behaviors.same
          }

        case Tick if !timerOn =>
          // Stale tick delivered from a cancelled schedule. Drop.
          Behaviors.same

        case Tick if inFlight =>
          log.debug("OrphanSpoolSweeper skipping tick — previous sweep still in flight")
          Behaviors.same

        case Tick =>
          val startNanos = System.nanoTime()
          // Materialise the sweep through the breaker. Lifting a thrown
          // exception into a failed Future ensures both async and sync
          // failure modes flow through the same pipeToSelf path.
          val sweep =
            try breaker.withCircuitBreaker(reconciler.reconcileOrphans(isActive))
            catch { case NonFatal(ex) => Future.failed[RecoveryReport](ex) }
          context.pipeToSelf(sweep) { result =>
            val durationMs = (System.nanoTime() - startNanos) / 1_000_000L
            SweepCompleted(durationMs, result)
          }
          next(nextTimerOn = timerOn, nextInFlight = true)

        case SweepCompleted(durationMs, result) =>
          handleSweepCompletion(durationMs, result, log, onSweepComplete)
          next(nextTimerOn = timerOn, nextInFlight = false)
      }
    }
  }

  /** Single source of truth for sweep terminal handling: logs the result
    * at the appropriate level, then invokes the test seam exactly once.
    * A sweep that was in-flight when `Stop` arrived still completes
    * here — only the timer is paused by `Stop`, not the pipeToSelf —
    * so every dispatched sweep is guaranteed one terminal log and one
    * seam invocation. */
  private def handleSweepCompletion(
      durationMs: Long,
      result: Try[RecoveryReport],
      log: Logger,
      onSweepComplete: (Long, OrphanSpoolSweeperImpl.SweepOutcome) => Unit
  ): Unit = result match {
    case Success(report) =>
      if (report.total > 0) {
        log.info(
          "OrphanSpoolSweeper sweep complete: durationMs={}, recovered={}, aborted={}, cleaned={}, failed={}, claimsResent={}",
          durationMs,
          report.sessionsRecovered,
          report.sessionsAborted,
          report.sessionsCleaned,
          report.sessionsFailed,
          report.totalClaimsResent
        )
      } else {
        log.debug(
          "OrphanSpoolSweeper sweep complete: durationMs={}, no orphans found",
          durationMs
        )
      }
      onSweepComplete(durationMs, OrphanSpoolSweeperImpl.SweepOutcome.Succeeded(report))

    case Failure(_: CircuitBreakerOpenException) =>
      // Breaker is OPEN — `withCircuitBreaker` short-circuited without
      // calling the reconciler. The OPEN transition itself is logged at
      // INFO by the breaker's `.onOpen` hook; per-tick suppression noise
      // stays at DEBUG.
      log.debug("OrphanSpoolSweeper tick fast-failed: circuit breaker OPEN")
      onSweepComplete(durationMs, OrphanSpoolSweeperImpl.SweepOutcome.FastFailed)

    case Failure(ex) =>
      log.warn(
        "OrphanSpoolSweeper sweep failed: durationMs={}, cause={}",
        durationMs,
        ex.getMessage
      )
      onSweepComplete(durationMs, OrphanSpoolSweeperImpl.SweepOutcome.Failed(ex))
  }
}

/** Default implementation of [[OrphanSpoolSweeper]].
  *
  * Schedules periodic ticks against an internal typed actor that owns
  * the sweeper's lifecycle and per-tick re-entry behaviour. The actor's
  * mailbox provides the single-threaded discipline that previously
  * required an [[java.util.concurrent.atomic.AtomicBoolean]] re-entry
  * guard, a `var` state field, and a `synchronized` lifecycle lock —
  * all three are gone.
  *
  * Observability: every completed sweep emits a structured log line
  * carrying `durationMs` so that operators can detect a sweep that is
  * approaching the configured `maxSweepDuration` budget without
  * correlating timestamps. The field is logged on both Success and
  * Failure paths.
  *
  * Failure-bounding: per-sweep work is wrapped in a Pekko
  * [[CircuitBreaker]]. Three classes of failure count against the
  * breaker: a thrown exception from `reconcileOrphans`, a failed
  * `Future`, and a sweep that exceeds `config.maxSweepDuration`. After
  * `config.maxConsecutiveFailures` consecutive failures the breaker
  * enters its OPEN state; subsequent ticks are fast-failed (logged at
  * DEBUG, no work dispatched). After the breaker's reset interval the
  * next tick triggers a single trial call (HALF-OPEN); a success closes
  * the breaker, a failure re-opens it. State transitions are logged at
  * INFO so that operators can see the breaker's behaviour in the audit
  * stream.
  *
  * Lifecycle: idempotent `start` / `stop` flow as messages through the
  * actor's mailbox; ordering is guaranteed by single-threaded delivery.
  * Naming: the actor is spawned with a UUID-suffixed system actor name
  * so that multiple sweepers (e.g. across parallel test suites) never
  * collide.
  *
  * Test seam: subclassing remains supported. Overriding
  * [[onSweepComplete]] is invoked from the actor for every terminal
  * sweep outcome via a closure passed at actor construction; any
  * throwable raised by an override is contained to a WARN log.
  */
class OrphanSpoolSweeperImpl(
    reconciler: OrphanReconciler,
    isActive: String => Future[Boolean],
    config: FlushSweeperConfig,
    system: ActorSystem[?]
) extends OrphanSpoolSweeper {

  /** Test seam: invoked once per terminal sweep outcome (success, failure, or
    * fast-failed by the breaker). Default no-op. Tests subclass and override
    * to observe sweep completion in a deterministic way that does not depend
    * on a particular SLF4J binding being on the classpath. The same data is
    * also written to the production logs — this seam never replaces logging,
    * it complements it. */
  protected def onSweepComplete(
      durationMs: Long,
      outcome: OrphanSpoolSweeperImpl.SweepOutcome
  ): Unit = ()

  private val log = system.log

  // Bounds an unbounded run: a wedged sweep would otherwise hold the
  // actor's in-flight state forever and silently starve subsequent
  // ticks. The breaker also prevents a known-sick downstream from
  // producing N busy ticks per cycle while it's unhealthy — once OPEN,
  // subsequent ticks fast-fail until the reset window elapses.
  // `resetTimeout = config.interval` aligns the half-open trial call
  // with the natural sweep cadence. Kept at the class level so that
  // it survives intra-process stop → start cycles (consumers expect
  // the failure count to persist).
  private val breaker: CircuitBreaker = new CircuitBreaker(
    scheduler = system.classicSystem.scheduler,
    maxFailures = config.maxConsecutiveFailures,
    callTimeout = config.maxSweepDuration,
    resetTimeout = config.interval
  )(system.executionContext)
    .onOpen(log.info(
      "OrphanSpoolSweeper circuit breaker OPEN: {} consecutive failures (callTimeout={}, will retry after resetTimeout={})",
      config.maxConsecutiveFailures, config.maxSweepDuration, config.interval
    ))
    .onHalfOpen(log.info("OrphanSpoolSweeper circuit breaker HALF-OPEN: trial sweep will run on next tick"))
    .onClose(log.info("OrphanSpoolSweeper circuit breaker CLOSED: sweeps resumed"))

  /** Containment shim around the test seam. Any throwable raised by an
    * overridden `onSweepComplete` is logged and swallowed — the test
    * seam must not be able to leak into the actor's dispatcher or break
    * a sweep's terminal handling. */
  private def invokeOnSweepComplete(
      durationMs: Long,
      outcome: OrphanSpoolSweeperImpl.SweepOutcome
  ): Unit =
    try onSweepComplete(durationMs, outcome)
    catch {
      case NonFatal(ex) =>
        log.warn("OrphanSpoolSweeper.onSweepComplete observer threw: {}", ex.getMessage)
    }

  private val sweeperActor = system.systemActorOf(
    OrphanSpoolSweeperActor(
      reconciler = reconciler,
      isActive = isActive,
      config = config,
      breaker = breaker,
      onSweepComplete = invokeOnSweepComplete
    ),
    s"orphan-spool-sweeper-${UUID.randomUUID()}"
  )

  override def start(): Future[Unit] = {
    val promise = Promise[Unit]()
    sweeperActor ! OrphanSpoolSweeperActor.Start(promise)
    promise.future
  }

  override def stop(): Future[Unit] = {
    val promise = Promise[Unit]()
    sweeperActor ! OrphanSpoolSweeperActor.Stop(promise)
    promise.future
  }
}

private[durablebufferedflush] object OrphanSpoolSweeperImpl {

  /** Terminal outcome of a single sweep iteration. Carried into the
    * [[OrphanSpoolSweeperImpl#onSweepComplete]] test seam.
    *
    * `Succeeded`   — the reconciler completed the pass and produced a
    *                 [[com.tomshley.boilerplate.jvm.durablebufferedflush.RecoveryReport]].
    * `Failed`      — the reconciler returned a failed Future or threw, or
    *                 the breaker's `callTimeout` fired against a wedged
    *                 sweep.
    * `FastFailed`  — the breaker was OPEN and short-circuited the call
    *                 without invoking the reconciler. */
  sealed trait SweepOutcome
  object SweepOutcome {
    final case class Succeeded(report: RecoveryReport) extends SweepOutcome
    final case class Failed(cause: Throwable) extends SweepOutcome
    case object FastFailed extends SweepOutcome
  }
}
