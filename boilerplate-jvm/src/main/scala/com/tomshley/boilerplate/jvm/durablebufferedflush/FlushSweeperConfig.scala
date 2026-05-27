/*
 * Copyright 2026 Tomshley LLC.
 * All Rights Reserved.
 */

package com.tomshley.boilerplate.jvm.durablebufferedflush

import com.typesafe.config.Config

import scala.concurrent.duration.*
import scala.jdk.DurationConverters.*

/** Configuration for the [[OrphanSpoolSweeper]] — periodic background
  * reconciliation of orphan spool entities to durable storage.
  *
  * Defaults are conservative: `enabled = false`. Consumers must opt in
  * explicitly via configuration; when disabled the sweeper is never scheduled
  * and the caller pays no runtime cost.
  *
  * Knobs:
  *   - `interval`               cadence between sweep ticks. For deployments
  *                              backed by ephemeral spool storage this is the
  *                              upper bound on the orphan-recovery window —
  *                              the longer the interval, the wider the window
  *                              during which a process restart can lose
  *                              in-flight buffered data.
  *   - `initialDelay`           delay between sweeper start and the first
  *                              tick. Lets a startup
  *                              `RecoveryManager.recover()` pass settle
  *                              before the sweeper begins re-listing spool
  *                              entities.
  *   - `maxSweepDuration`       upper bound on a single sweep's wall-clock
  *                              duration. Backs the [[OrphanSpoolSweeper]]'s
  *                              circuit breaker `callTimeout`: a sweep that
  *                              fails to complete within this window is
  *                              counted as a failure for the breaker. Default
  *                              is `interval × 10` — a loose bound for
  *                              background reconciliation, set so that a
  *                              transient slow downstream does not flap the
  *                              breaker. Operators tighten this after a soak
  *                              window with metric data.
  *   - `maxConsecutiveFailures` how many consecutive sweep failures (timeouts
  *                              or thrown exceptions) trip the breaker into
  *                              its OPEN state, fast-failing subsequent ticks
  *                              until the breaker's reset interval elapses.
  */
final case class FlushSweeperConfig(
    enabled: Boolean,
    interval: FiniteDuration,
    initialDelay: FiniteDuration,
    maxSweepDuration: FiniteDuration,
    maxConsecutiveFailures: Int
) {
  require(maxConsecutiveFailures > 0,
    s"maxConsecutiveFailures must be > 0, got $maxConsecutiveFailures")
  require(maxSweepDuration > Duration.Zero,
    s"maxSweepDuration must be positive, got $maxSweepDuration")
}

object FlushSweeperConfig {

  /** Sentinel "disabled" config. The sweeper's `start()` is a no-op when this
    * is supplied. */
  val Disabled: FlushSweeperConfig =
    FlushSweeperConfig(
      enabled = false,
      interval = 15.minutes,
      initialDelay = 2.minutes,
      maxSweepDuration = 150.minutes,
      maxConsecutiveFailures = 3
    )

  /** Read sweeper config from a `sweeper { ... }` HOCON block under the parent
    * config (typically the same parent as `recovery`, `backpressure`, `close`).
    *
    * If the `sweeper` block is missing the sweeper is treated as disabled —
    * this preserves backward compatibility for consumers that have not yet
    * added the block to their `application.conf`.
    *
    * Expected keys (all optional, defaults shown):
    *   - `enabled`                  boolean,  default false
    *   - `interval`                 duration, default 15 minutes
    *   - `initial-delay`            duration, default 2 minutes
    *   - `max-sweep-duration`       duration, default `interval × 10`
    *   - `max-consecutive-failures` int,      default 3 */
  def fromConfig(parent: Config): FlushSweeperConfig = {
    if (!parent.hasPath("sweeper")) Disabled
    else {
      val cfg = parent.getConfig("sweeper")
      val interval =
        if (cfg.hasPath("interval")) cfg.getDuration("interval").toScala else 15.minutes
      FlushSweeperConfig(
        enabled = if (cfg.hasPath("enabled")) cfg.getBoolean("enabled") else false,
        interval = interval,
        initialDelay =
          if (cfg.hasPath("initial-delay")) cfg.getDuration("initial-delay").toScala else 2.minutes,
        maxSweepDuration =
          if (cfg.hasPath("max-sweep-duration")) cfg.getDuration("max-sweep-duration").toScala
          else interval * 10,
        maxConsecutiveFailures =
          if (cfg.hasPath("max-consecutive-failures")) cfg.getInt("max-consecutive-failures") else 3
      )
    }
  }
}
