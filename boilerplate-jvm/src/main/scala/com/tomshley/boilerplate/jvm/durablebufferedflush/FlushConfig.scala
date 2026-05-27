/*
 * Copyright 2026 Tomshley LLC.
 * All Rights Reserved.
 */

package com.tomshley.boilerplate.jvm.durablebufferedflush

import com.typesafe.config.Config
import org.apache.pekko.actor.typed.ActorSystem
import org.apache.pekko.util.Timeout

import scala.concurrent.duration.*
import scala.jdk.DurationConverters.*

final case class FlushBackpressureConfig(
    claimLagSoft: Long,
    claimLagHard: Long,
    pauseTimeout: FiniteDuration
)

final case class FlushCloseConfig(
    askTimeout: Timeout,
    inspectTimeout: Timeout,
    retryDelay: FiniteDuration,
    maxRetries: Int
)

/** Configuration for the recovery / steady-state-reconciliation pass.
  *
  * Knobs:
  *   - `parallelism`        upper bound on concurrent per-entity work (e.g.
  *                          `isActive` predicate evaluations and per-orphan
  *                          recovery). The recovery path batches work in
  *                          chunks of this size so that an expensive
  *                          predicate or slow downstream cannot fan out
  *                          unboundedly.
  *   - `inspectTimeout`     ask-timeout for `SessionPort.inspect`-shaped
  *                          calls during recovery.
  *   - `perEntityTimeout`   upper bound on the wall-clock time any single
  *                          per-entity reconciliation step (the `isActive`
  *                          predicate evaluation, or the per-entity
  *                          recovery work) is allowed to take. Applied
  *                          symmetrically to both the startup
  *                          `RecoveryManager.recover()` pass and the
  *                          steady-state `OrphanReconciler.reconcileOrphans`
  *                          pass. A timeout on `isActive` is treated as
  *                          "active" (skip this pass — the conservative
  *                          choice); a timeout on per-entity recovery is
  *                          contained as a per-entity failure and the rest
  *                          of the batch continues.
  */
final case class FlushRecoveryConfig(
    parallelism: Int,
    inspectTimeout: Timeout,
    perEntityTimeout: FiniteDuration
) {
  require(parallelism > 0, s"parallelism must be > 0, got $parallelism")
  require(perEntityTimeout > Duration.Zero,
    s"perEntityTimeout must be positive, got $perEntityTimeout")
}

final case class FlushConfig(
    backpressure: FlushBackpressureConfig,
    close: FlushCloseConfig,
    recovery: FlushRecoveryConfig
)

object FlushConfig {

  /** Default per-entity recovery timeout. Sized to absorb realistic minute-scale
    * tail latencies on backing object-storage / messaging round-trips. */
  private val DefaultPerEntityTimeout: FiniteDuration = 120.seconds

  def fromConfig(config: Config, system: ActorSystem[?]): FlushConfig = {
    val backpressureConfig = config.getConfig("backpressure")
    val closeConfig = config.getConfig("close")
    val recoveryConfig = config.getConfig("recovery")

    FlushConfig(
      backpressure = FlushBackpressureConfig(
        claimLagSoft = backpressureConfig.getLong("claim-lag-soft"),
        claimLagHard = backpressureConfig.getLong("claim-lag-hard"),
        pauseTimeout = backpressureConfig.getDuration("pause-timeout").toScala
      ),
      close = FlushCloseConfig(
        askTimeout = Timeout.create(closeConfig.getDuration("ask-timeout")),
        inspectTimeout = Timeout.create(closeConfig.getDuration("inspect-timeout")),
        retryDelay = closeConfig.getDuration("retry-delay").toScala,
        maxRetries = closeConfig.getInt("max-retries")
      ),
      recovery = FlushRecoveryConfig(
        parallelism = math.max(1, recoveryConfig.getInt("parallelism")),
        inspectTimeout = Timeout.create(recoveryConfig.getDuration("inspect-timeout")),
        perEntityTimeout =
          if (recoveryConfig.hasPath("per-entity-timeout"))
            recoveryConfig.getDuration("per-entity-timeout").toScala
          else DefaultPerEntityTimeout
      )
    )
  }
}
