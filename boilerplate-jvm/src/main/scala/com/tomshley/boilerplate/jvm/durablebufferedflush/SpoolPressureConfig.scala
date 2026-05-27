/*
 * Copyright 2026 Tomshley LLC.
 * All Rights Reserved.
 */

package com.tomshley.boilerplate.jvm.durablebufferedflush

import com.typesafe.config.Config

import scala.concurrent.duration.*
import scala.jdk.DurationConverters.*

/** Configuration for spool-pressure monitoring and admission control.
  *
  * Designed around four observations:
  *  1. The hot path (per-chunk write) must remain free of policy. Pressure
  *     is sampled on a separate cadence by [[SpoolPressureMonitor]] and the
  *     policy gate is read once per session at admission.
  *  2. Two thresholds are sufficient: an alert level (operators are paged,
  *     no behavior change) and a critical level (admission closes for new
  *     sessions; in-flight sessions are never gated).
  *  3. Hysteresis prevents flapping at the boundary. Each threshold has a
  *     companion "clear" percentage that must be crossed downward before
  *     the level transitions back.
  *  4. Capacity is bounded by `min(configuredCapacityBytes,
  *     filesystem.totalSpace)`. The configured ceiling is a policy choice;
  *     the filesystem is a physical ceiling. The smaller wins.
  *
  * Knobs:
  *   - `enabled`                  master switch. When false the monitor is
  *                                a no-op and the admission controller
  *                                stays open.
  *   - `monitorInterval`          fast cadence — reads the hot-path counter
  *                                and re-evaluates the level. Default 5m.
  *   - `reconciliationInterval`   slow cadence — invokes
  *                                [[SpoolSizeReporter.recountFromFilesystem]]
  *                                to correct hot-path drift. Default 15m.
  *   - `alertThresholdPercent`    percent occupancy at which the level
  *                                transitions Low → High (operators
  *                                paged). Default 70.
  *   - `criticalThresholdPercent` percent occupancy at which the level
  *                                transitions High → Critical (admission
  *                                closes). Default 90.
  *   - `alertClearPercent`        Low-side hysteresis edge — High → Low
  *                                only after occupancy drops below this.
  *                                Default 65.
  *   - `criticalClearPercent`     Critical-side hysteresis edge —
  *                                Critical → High only after occupancy
  *                                drops below this. Default 85.
  *   - `configuredCapacityBytes`  policy ceiling. `None` = "use filesystem
  *                                truth only". When set, the effective
  *                                capacity is `min(this, filesystem.totalSpace)`.
  *   - `suggestedRetryAfter`      operator-suggested delay carried by
  *                                [[SpoolPressureCriticalException]] when
  *                                admission is closed. Default 30s.
  */
final case class SpoolPressureConfig(
    enabled: Boolean,
    monitorInterval: FiniteDuration,
    reconciliationInterval: FiniteDuration,
    alertThresholdPercent: Int,
    criticalThresholdPercent: Int,
    alertClearPercent: Int,
    criticalClearPercent: Int,
    configuredCapacityBytes: Option[Long],
    suggestedRetryAfter: FiniteDuration
) {
  require(0 < alertThresholdPercent && alertThresholdPercent < criticalThresholdPercent && criticalThresholdPercent <= 100,
    s"thresholds must satisfy 0 < alert ($alertThresholdPercent) < critical ($criticalThresholdPercent) <= 100")
  require(0 < alertClearPercent && alertClearPercent < alertThresholdPercent,
    s"alertClearPercent ($alertClearPercent) must be in (0, alertThresholdPercent ($alertThresholdPercent))")
  require(alertClearPercent < criticalClearPercent && criticalClearPercent < criticalThresholdPercent,
    s"criticalClearPercent ($criticalClearPercent) must be in (alertClearPercent ($alertClearPercent), criticalThresholdPercent ($criticalThresholdPercent))")
  require(monitorInterval > Duration.Zero, s"monitorInterval must be positive, got $monitorInterval")
  require(reconciliationInterval > Duration.Zero, s"reconciliationInterval must be positive, got $reconciliationInterval")
  require(suggestedRetryAfter > Duration.Zero, s"suggestedRetryAfter must be positive, got $suggestedRetryAfter")
  configuredCapacityBytes.foreach { c =>
    require(c > 0L, s"configuredCapacityBytes must be > 0 when present, got $c")
  }
}

object SpoolPressureConfig {

  /** Sentinel "disabled" config. The monitor's `start()` is a no-op when
    * this is supplied; [[Workflow]] callers that wire pressure may pass
    * this to keep admission permanently open while satisfying the
    * pressure-aware factory shape. Mirrors the
    * [[FlushSweeperConfig.Disabled]] pattern.
    *
    * Defaults reflect §F.5 of the durability/pressure plan: 70% alert,
    * 90% critical, 5% hysteresis on both bands, 5m monitor / 15m
    * reconciliation, 30s retry-after. */
  val Disabled: SpoolPressureConfig = SpoolPressureConfig(
    enabled = false,
    monitorInterval = 5.minutes,
    reconciliationInterval = 15.minutes,
    alertThresholdPercent = 70,
    criticalThresholdPercent = 90,
    alertClearPercent = 65,
    criticalClearPercent = 85,
    configuredCapacityBytes = None,
    suggestedRetryAfter = 30.seconds
  )

  /** Read pressure config from a `pressure { ... }` HOCON block under the
    * parent config (typically the same parent as `recovery`,
    * `backpressure`, `close`, `sweeper`).
    *
    * If the `pressure` block is missing the pressure architecture is
    * treated as disabled — preserves backward compatibility for consumers
    * that have not yet added the block.
    *
    * Expected keys (all optional, defaults shown):
    *   - `enabled`                    boolean,  default false
    *   - `monitor-interval`           duration, default 5 minutes
    *   - `reconciliation-interval`    duration, default 15 minutes
    *   - `alert-threshold-percent`    int,      default 70
    *   - `critical-threshold-percent` int,      default 90
    *   - `alert-clear-percent`        int,      default 65
    *   - `critical-clear-percent`     int,      default 85
    *   - `configured-capacity-bytes`  long,     default unset (filesystem-only)
    *   - `suggested-retry-after`      duration, default 30 seconds */
  def fromConfig(parent: Config): SpoolPressureConfig = {
    if (!parent.hasPath("pressure")) Disabled
    else {
      val cfg = parent.getConfig("pressure")
      SpoolPressureConfig(
        enabled =
          if (cfg.hasPath("enabled")) cfg.getBoolean("enabled") else false,
        monitorInterval =
          if (cfg.hasPath("monitor-interval")) cfg.getDuration("monitor-interval").toScala else 5.minutes,
        reconciliationInterval =
          if (cfg.hasPath("reconciliation-interval")) cfg.getDuration("reconciliation-interval").toScala else 15.minutes,
        alertThresholdPercent =
          if (cfg.hasPath("alert-threshold-percent")) cfg.getInt("alert-threshold-percent") else 70,
        criticalThresholdPercent =
          if (cfg.hasPath("critical-threshold-percent")) cfg.getInt("critical-threshold-percent") else 90,
        alertClearPercent =
          if (cfg.hasPath("alert-clear-percent")) cfg.getInt("alert-clear-percent") else 65,
        criticalClearPercent =
          if (cfg.hasPath("critical-clear-percent")) cfg.getInt("critical-clear-percent") else 85,
        configuredCapacityBytes =
          if (cfg.hasPath("configured-capacity-bytes"))
            Some(cfg.getBytes("configured-capacity-bytes").longValue())
          else None,
        suggestedRetryAfter =
          if (cfg.hasPath("suggested-retry-after")) cfg.getDuration("suggested-retry-after").toScala else 30.seconds
      )
    }
  }
}
