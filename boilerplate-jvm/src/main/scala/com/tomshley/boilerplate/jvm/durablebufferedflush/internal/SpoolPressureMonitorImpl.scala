/*
 * Copyright 2026 Tomshley LLC.
 * All Rights Reserved.
 */

package com.tomshley.boilerplate.jvm.durablebufferedflush.internal

import com.tomshley.boilerplate.jvm.durablebufferedflush.{
  SpoolPressureConfig,
  SpoolPressureLevel,
  SpoolPressureMonitor,
  SpoolSizeReporter
}
import org.apache.pekko.actor.typed.scaladsl.{Behaviors, TimerScheduler}
import org.apache.pekko.actor.typed.{ActorSystem, Behavior}
import org.slf4j.Logger

import java.util.UUID
import scala.concurrent.{Future, Promise}
import scala.util.{Failure, Success, Try}
import scala.util.control.NonFatal

private object SpoolPressureMonitorActor {

  sealed trait Command
  final case class Start(reply: Promise[Unit]) extends Command
  final case class Stop(reply: Promise[Unit]) extends Command
  final case class GetCurrentLevel(reply: Promise[SpoolPressureLevel]) extends Command
  final case class Subscribe(
      handler: SpoolPressureLevel => Unit,
      reply: Promise[Unit]
  ) extends Command
  private case object MonitorTick extends Command
  private case object ReconcileTick extends Command
  // Carries the capacity snapshot taken at MonitorTick dispatch alongside the
  // async size result. Capturing capacity at dispatch (not at completion)
  // keeps the percent computation consistent with the size value it is
  // paired with — a config swap that happens during the in-flight read
  // does not produce a half-old half-new evaluation.
  private final case class MonitorSizeReceived(
      capacity: Long,
      result: Try[Long]
  ) extends Command
  private final case class ReconcileCompleted(result: Try[Long]) extends Command

  private case object MonitorTickKey
  private case object ReconcileTickKey

  /** Behavior factory. The actor is the single source of truth for:
    *  - the current pressure level (no [[AtomicReference]] leak);
    *  - the subscriber registry (no [[AtomicReference]] leak);
    *  - the two timer schedules (provided by `Behaviors.withTimers`);
    *  - per-tick re-entry discipline (orthogonal `monitorInFlight` /
    *    `reconcileInFlight` flags on the behavior, no atomics).
    *
    * Asynchronous work (the recount) is bridged back into the mailbox
    * via `context.pipeToSelf` — no raw [[scala.concurrent.Future]]
    * callbacks inside the actor. */
  def apply(
      reporter: SpoolSizeReporter,
      config: SpoolPressureConfig,
      capacityProvider: () => Long
  ): Behavior[Command] =
    Behaviors.setup { context =>
      Behaviors.withTimers { timers =>
        active(
          timerOn = false,
          monitorInFlight = false,
          reconcileInFlight = false,
          level = SpoolPressureLevel.Low,
          handlers = Vector.empty,
          timers,
          reporter,
          config,
          capacityProvider,
          context.log
        )
      }
    }

  /** The actor's single behavior. State lives in the behavior parameters
    * (no `var`, no atomics) and is rolled forward through `next(...)`
    * convenience calls that vary one or more axes.
    *
    * Axes:
    *  - `timerOn`           — periodic ticks are scheduled. Flipped by Start/Stop.
    *  - `monitorInFlight`   — a fast tick has dispatched an async size read
    *                          to the reporter and is awaiting the reply.
    *                          Flipped by MonitorTick (dispatch) and
    *                          MonitorSizeReceived (terminal).
    *  - `reconcileInFlight` — a slow tick is currently awaiting recount.
    *                          Flipped by ReconcileTick (dispatch) and
    *                          ReconcileCompleted (terminal).
    *  - `level`             — the most recently observed level.
    *  - `handlers`          — registered transition subscribers, in
    *                          registration order. */
  private def active(
      timerOn: Boolean,
      monitorInFlight: Boolean,
      reconcileInFlight: Boolean,
      level: SpoolPressureLevel,
      handlers: Vector[SpoolPressureLevel => Unit],
      timers: TimerScheduler[Command],
      reporter: SpoolSizeReporter,
      config: SpoolPressureConfig,
      capacityProvider: () => Long,
      log: Logger
  ): Behavior[Command] = {
    def next(
        nextTimerOn: Boolean = timerOn,
        nextMonitorInFlight: Boolean = monitorInFlight,
        nextReconcileInFlight: Boolean = reconcileInFlight,
        nextLevel: SpoolPressureLevel = level,
        nextHandlers: Vector[SpoolPressureLevel => Unit] = handlers
    ): Behavior[Command] = active(
      nextTimerOn,
      nextMonitorInFlight,
      nextReconcileInFlight,
      nextLevel,
      nextHandlers,
      timers,
      reporter,
      config,
      capacityProvider,
      log
    )

    Behaviors.receive { (context, message) =>
      message match {
        case Start(reply) if timerOn =>
          log.debug("SpoolPressureMonitor.start ignored — already running")
          reply.trySuccess(())
          Behaviors.same

        case Start(reply) =>
          reply.trySuccess(())
          if (!config.enabled) {
            log.info("SpoolPressureMonitor disabled by config — start() is a no-op")
            Behaviors.same
          } else {
            timers.startTimerWithFixedDelay(
              MonitorTickKey,
              MonitorTick,
              config.monitorInterval,
              config.monitorInterval
            )
            timers.startTimerWithFixedDelay(
              ReconcileTickKey,
              ReconcileTick,
              config.reconciliationInterval,
              config.reconciliationInterval
            )
            log.info(
              "SpoolPressureMonitor started: monitorInterval={}, reconciliationInterval={}, alertThreshold={}%, criticalThreshold={}%",
              config.monitorInterval,
              config.reconciliationInterval,
              config.alertThresholdPercent,
              config.criticalThresholdPercent
            )
            next(nextTimerOn = true)
          }

        case Stop(reply) =>
          reply.trySuccess(())
          if (timerOn) {
            timers.cancel(MonitorTickKey)
            timers.cancel(ReconcileTickKey)
            log.info("SpoolPressureMonitor stopped")
            next(nextTimerOn = false)
          } else {
            Behaviors.same
          }

        case GetCurrentLevel(reply) =>
          reply.trySuccess(level)
          Behaviors.same

        case Subscribe(handler, reply) =>
          reply.trySuccess(())
          next(nextHandlers = handlers :+ handler)

        case MonitorTick if !timerOn =>
          // Stale tick from a cancelled schedule. Drop.
          Behaviors.same

        case MonitorTick if monitorInFlight =>
          // A previous async size read has not yet resolved. Skipping
          // here prevents the reporter from being asked twice for the
          // same logical tick — the in-flight read will arrive shortly
          // and trigger an evaluation; the next scheduled MonitorTick
          // proceeds normally once that completes.
          log.debug("SpoolPressureMonitor skipping monitor tick — previous size read still in flight")
          Behaviors.same

        case MonitorTick =>
          // Capture capacity at dispatch so the percent computation in
          // MonitorSizeReceived is paired with a consistent (capacity,
          // bytes) pair. A capacity provider that swaps mid-flight does
          // not produce a half-old half-new evaluation.
          val capacity = capacityProvider()
          if (capacity <= 0L) {
            log.debug("SpoolPressureMonitor: capacity <= 0; skipping level evaluation")
            Behaviors.same
          } else {
            // Lift a synchronous throw into a failed Future so both async
            // and sync failure modes flow through the same pipeToSelf
            // handler — single source of truth for logging and the
            // in-flight reset.
            val sizeFuture =
              try reporter.currentSizeBytes()
              catch { case NonFatal(ex) => Future.failed[Long](ex) }
            context.pipeToSelf(sizeFuture)(MonitorSizeReceived(capacity, _))
            next(nextMonitorInFlight = true)
          }

        case MonitorSizeReceived(capacity, Success(sizeBytes)) =>
          val percent = percentOf(sizeBytes, capacity)
          val newLevel = nextLevel(level, percent, config)
          val didTransition = newLevel != level
          if (didTransition) {
            log.info(
              "SpoolPressureMonitor level transition: {} -> {} (sizeBytes={}, capacityBytes={}, percent={})",
              level,
              newLevel,
              sizeBytes,
              capacity,
              percent
            )
            emitToHandlers(newLevel, handlers, log)
          }
          next(nextMonitorInFlight = false, nextLevel = newLevel)

        case MonitorSizeReceived(_, Failure(ex)) =>
          log.warn("SpoolPressureMonitor monitor tick failed: {}", ex.getMessage)
          next(nextMonitorInFlight = false)

        case ReconcileTick if !timerOn =>
          // Stale tick from a cancelled schedule. Drop.
          Behaviors.same

        case ReconcileTick if reconcileInFlight =>
          log.debug("SpoolPressureMonitor skipping reconciliation tick — previous tick still in flight")
          Behaviors.same

        case ReconcileTick =>
          // Lift a synchronous throw into a failed Future so both async and
          // sync failure modes flow through the same pipeToSelf handler —
          // single source of truth for logging and the in-flight reset.
          val recount =
            try reporter.recountFromFilesystem()
            catch { case NonFatal(ex) => Future.failed[Long](ex) }
          context.pipeToSelf(recount)(ReconcileCompleted(_))
          next(nextReconcileInFlight = true)

        case ReconcileCompleted(Success(bytes)) =>
          log.debug("SpoolPressureMonitor reconciliation complete: filesystemBytes={}", bytes)
          next(nextReconcileInFlight = false)

        case ReconcileCompleted(Failure(ex)) =>
          log.warn("SpoolPressureMonitor reconciliation failed: {}", ex.getMessage)
          next(nextReconcileInFlight = false)
      }
    }
  }

  private def emitToHandlers(
      next: SpoolPressureLevel,
      handlers: Vector[SpoolPressureLevel => Unit],
      log: Logger
  ): Unit =
    handlers.foreach { handler =>
      try handler(next)
      catch {
        case NonFatal(ex) =>
          log.warn("SpoolPressureMonitor onLevelChange handler threw: {}", ex.getMessage)
      }
    }

  private def percentOf(sizeBytes: Long, capacityBytes: Long): Int =
    // The two early-return branches prove `sizeBytes` is in `(0, capacity)`
    // here, so the ratio is in `(0.0, 1.0)` and `.toInt` truncates to `[0, 99]`
    // — no explicit clamp needed.
    if (sizeBytes <= 0L) 0
    else if (sizeBytes >= capacityBytes) 100
    else ((sizeBytes.toDouble / capacityBytes.toDouble) * 100.0).toInt

  /** Apply the threshold + hysteresis state machine. Idempotent — a tick at
    * the same percent as the previous tick returns the previous level.
    *
    * Multi-level jumps are permitted in both directions when a single tick
    * crosses both threshold edges: a counter recount can take Low → Critical
    * in one step, and a sudden drain can take Critical → Low in one step.
    * Both jumps satisfy the relevant hysteresis clear edge, so neither
    * risks flapping; routing Critical through High over two ticks would
    * be slower without being safer. */
  private def nextLevel(
      previous: SpoolPressureLevel,
      percent: Int,
      config: SpoolPressureConfig
  ): SpoolPressureLevel = {
    import SpoolPressureLevel.*
    previous match {
      case Low =>
        if (percent >= config.criticalThresholdPercent) Critical
        else if (percent >= config.alertThresholdPercent) High
        else Low

      case High =>
        if (percent >= config.criticalThresholdPercent) Critical
        else if (percent < config.alertClearPercent) Low
        else High

      case Critical =>
        if (percent < config.alertClearPercent) Low
        else if (percent < config.criticalClearPercent) High
        else Critical
    }
  }
}

/** Default implementation of [[SpoolPressureMonitor]].
  *
  * The implementation is a thin facade over a typed actor that owns
  * every piece of mutable state — the current pressure level, the
  * subscriber registry, the schedule lifecycle, and per-tick re-entry
  * discipline. No [[java.util.concurrent.atomic]], no `var`, no
  * `synchronized` block, no [[scala.concurrent.Future#onComplete]]
  * callback inside the actor.
  *
  * Two schedules, both timer-driven from inside the actor via
  * `Behaviors.withTimers`:
  *  - the fast tick (`config.monitorInterval`) dispatches an async read
  *    via [[SpoolSizeReporter.currentSizeBytes]] (returns `Future[Long]`),
  *    pipes the result back to the actor via `pipeToSelf`, and then
  *    computes the current level using `config`'s thresholds with
  *    hysteresis applied against the previously-observed level, emitting
  *    a transition event if the level has changed. Capacity is captured
  *    at dispatch (not at completion) so the (capacity, bytes) pair
  *    used for the percent computation is internally consistent;
  *  - the slow tick (`config.reconciliationInterval`) invokes
  *    [[SpoolSizeReporter.recountFromFilesystem]] to correct accounting
  *    drift. The recount runs on the reporter's blocking dispatcher
  *    (see `FilesystemChunkSpool`); the monitor pipes the result back
  *    to itself via `pipeToSelf` and then permits the next slow tick.
  *
  * Lifecycle: idempotent `start` / `stop` flow as messages through the
  * actor's mailbox; ordering is guaranteed by single-threaded delivery.
  * The actor is spawned with a UUID-suffixed system actor name so that
  * multiple monitor instances (e.g. across parallel test suites) never
  * collide.
  */
final class SpoolPressureMonitorImpl(
    reporter: SpoolSizeReporter,
    config: SpoolPressureConfig,
    system: ActorSystem[?],
    capacityProvider: () => Long
) extends SpoolPressureMonitor {

  private val monitorActor = system.systemActorOf(
    SpoolPressureMonitorActor(
      reporter = reporter,
      config = config,
      capacityProvider = capacityProvider
    ),
    s"spool-pressure-monitor-${UUID.randomUUID()}"
  )

  override def start(): Future[Unit] = {
    val promise = Promise[Unit]()
    monitorActor ! SpoolPressureMonitorActor.Start(promise)
    promise.future
  }

  override def stop(): Future[Unit] = {
    val promise = Promise[Unit]()
    monitorActor ! SpoolPressureMonitorActor.Stop(promise)
    promise.future
  }

  override def currentLevel(): Future[SpoolPressureLevel] = {
    val promise = Promise[SpoolPressureLevel]()
    monitorActor ! SpoolPressureMonitorActor.GetCurrentLevel(promise)
    promise.future
  }

  override def onLevelChange(handler: SpoolPressureLevel => Unit): Future[Unit] = {
    val promise = Promise[Unit]()
    monitorActor ! SpoolPressureMonitorActor.Subscribe(handler, promise)
    promise.future
  }
}
