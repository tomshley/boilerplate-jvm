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

final case class FlushRecoveryConfig(
    parallelism: Int,
    inspectTimeout: Timeout
)

final case class FlushConfig(
    backpressure: FlushBackpressureConfig,
    close: FlushCloseConfig,
    recovery: FlushRecoveryConfig
)

object FlushConfig {
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
        inspectTimeout = Timeout.create(recoveryConfig.getDuration("inspect-timeout"))
      )
    )
  }
}
