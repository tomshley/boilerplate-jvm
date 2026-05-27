/*
 * Copyright 2026 Tomshley LLC.
 * All Rights Reserved.
 */

package com.tomshley.boilerplate.jvm.durablebufferedflush

import com.tomshley.boilerplate.jvm.durablebufferedflush.internal.SpoolPressureMonitorImpl
import org.apache.pekko.actor.typed.ActorSystem

import scala.concurrent.Future

/** Periodically samples the spool size and emits level transitions.
  *
  * Decouples measurement from action: the monitor owns the threshold and
  * hysteresis logic; subscribers (typically an [[AdmissionController]])
  * react to level transitions. Multiple subscribers are permitted and
  * each is called from the monitor's actor on the actor system's
  * dispatcher, so handlers MUST be cheap and side-effect-bounded —
  * they MUST NOT block, MUST NOT perform I/O, and MUST NOT throw (a
  * thrown handler is contained to a WARN log and the remaining
  * subscribers are still called).
  *
  * Owns:
  *   - the periodic schedule (fast tick + slow filesystem reconciliation);
  *   - the threshold / hysteresis state machine;
  *   - the level-change observer registry.
  *
  * Does NOT own:
  *   - the act of refusing new sessions — that belongs to
  *     [[AdmissionController]];
  *   - the byte-level measurement — that belongs to [[SpoolSizeReporter]].
  *
  * Lifecycle mirrors [[OrphanSpoolSweeper]]: idempotent `start` / `stop`,
  * actor-mailbox-serialized lifecycle transitions, no-op when the
  * supplied config is disabled.
  *
  * Bonér note: every public method returns a [[Future]] because the
  * implementation owns its state inside a typed actor; there is no
  * `@volatile` or [[java.util.concurrent.atomic]] cache leaking out
  * of the trait surface. Callers that want the "most recent" level
  * should subscribe via [[onLevelChange]] and react to the transition
  * directly; [[currentLevel]] is a coarse-grained out-of-band query
  * intended for diagnostics, health endpoints, and tests.
  */
trait SpoolPressureMonitor {

  /** Start the periodic monitor. Idempotent. Returns a completed future
    * when the underlying config is disabled — `start` does not schedule
    * in that case and subsequent observer registrations remain valid but
    * are never invoked. */
  def start(): Future[Unit]

  /** Stop the monitor. Idempotent. Cancels any scheduled tick; an
    * in-flight reconciliation is allowed to complete naturally. */
  def stop(): Future[Unit]

  /** The most recently observed level. The returned future completes
    * once the monitor's actor has processed the query — typically
    * sub-millisecond. Before the first tick fires this returns
    * [[SpoolPressureLevel.Low]]. */
  def currentLevel(): Future[SpoolPressureLevel]

  /** Register a callback invoked on every Low ↔ High ↔ Critical transition
    * (after hysteresis is applied). The handler is invoked from the
    * monitor's actor; see the trait scaladoc for the handler contract.
    * Multiple subscribers are permitted; each runs in registration order.
    * There is no `unregister` — handlers live for the lifetime of the
    * monitor instance.
    *
    * The returned future completes once the actor has accepted the
    * registration. After that point, any subsequent transition will
    * invoke this handler.
    *
    * Bonér note: the level-change event is the single source of truth.
    * A subscriber that wants to know the current level out of band may
    * call [[currentLevel]] at any time; it MUST NOT register a handler
    * solely to capture the most recent value. */
  def onLevelChange(handler: SpoolPressureLevel => Unit): Future[Unit]
}

object SpoolPressureMonitor {

  /** Build a pressure monitor that reads from the supplied size reporter
    * on the schedule defined by `config`.
    *
    * The capacity denominator is taken from
    * `config.configuredCapacityBytes`. If that is `None`, capacity falls
    * back to `Long.MaxValue` — effectively disabling level transitions
    * (every measurement maps to 0% occupancy, so the level stays Low).
    *
    * Consumers that want the canonical "min(configured ceiling, filesystem
    * total space)" capacity policy should use the four-argument overload
    * and pass the computed minimum as a `capacityProvider`. The decoupling
    * is deliberate: the monitor stays a measurement-and-policy component,
    * agnostic of where the storage lives. */
  def apply(
      reporter: SpoolSizeReporter,
      config: SpoolPressureConfig,
      system: ActorSystem[?]
  ): SpoolPressureMonitor =
    new SpoolPressureMonitorImpl(
      reporter = reporter,
      config = config,
      system = system,
      capacityProvider = () => config.configuredCapacityBytes.getOrElse(Long.MaxValue)
    )

  /** Build a pressure monitor with a caller-supplied capacity provider so
    * that the denominator used for percent occupancy can reflect a
    * filesystem-aware computation (typically `min(config.configuredCapacityBytes,
    * Files.getFileStore(spoolRoot).getTotalSpace)`). The provider is
    * called on every monitor tick so that filesystem capacity changes
    * (e.g. PVC online resize) are picked up without restart. */
  def apply(
      reporter: SpoolSizeReporter,
      config: SpoolPressureConfig,
      system: ActorSystem[?],
      capacityProvider: () => Long
  ): SpoolPressureMonitor =
    new SpoolPressureMonitorImpl(
      reporter = reporter,
      config = config,
      system = system,
      capacityProvider = capacityProvider
    )
}
