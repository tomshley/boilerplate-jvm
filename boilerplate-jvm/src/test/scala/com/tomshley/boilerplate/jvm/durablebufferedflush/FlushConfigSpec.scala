/*
 * Copyright 2026 Tomshley LLC.
 * All Rights Reserved.
 */

package com.tomshley.boilerplate.jvm.durablebufferedflush

import com.typesafe.config.ConfigFactory
import org.apache.pekko.actor.testkit.typed.scaladsl.ActorTestKit
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.duration.*

final class FlushConfigSpec extends AnyWordSpec with Matchers with BeforeAndAfterAll {

  private val testKit = ActorTestKit("FlushConfigSpec")

  override def afterAll(): Unit = {
    testKit.shutdownTestKit()
    super.afterAll()
  }

  "FlushConfig.fromConfig" should {
    "parse all fields from config including the explicit per-entity-timeout" in {
      val config = ConfigFactory.parseString(
        """
          |backpressure {
          |  claim-lag-soft = 12
          |  claim-lag-hard = 34
          |  pause-timeout = 1500 ms
          |}
          |close {
          |  ask-timeout = 2 s
          |  inspect-timeout = 3 s
          |  retry-delay = 250 ms
          |  max-retries = 7
          |}
          |recovery {
          |  parallelism = 5
          |  inspect-timeout = 4 s
          |  per-entity-timeout = 90 s
          |}
          |""".stripMargin
      )

      val parsed = FlushConfig.fromConfig(config, testKit.system)

      parsed.backpressure.claimLagSoft shouldBe 12L
      parsed.backpressure.claimLagHard shouldBe 34L
      parsed.backpressure.pauseTimeout shouldBe 1500.millis
      parsed.close.askTimeout.duration shouldBe 2.seconds
      parsed.close.inspectTimeout.duration shouldBe 3.seconds
      parsed.close.retryDelay shouldBe 250.millis
      parsed.close.maxRetries shouldBe 7
      parsed.recovery.parallelism shouldBe 5
      parsed.recovery.inspectTimeout.duration shouldBe 4.seconds
      parsed.recovery.perEntityTimeout shouldBe 90.seconds
    }

    "default per-entity-timeout to 120s when not specified" in {
      val config = ConfigFactory.parseString(
        """
          |backpressure {
          |  claim-lag-soft = 1
          |  claim-lag-hard = 2
          |  pause-timeout = 100 ms
          |}
          |close {
          |  ask-timeout = 1 s
          |  inspect-timeout = 1 s
          |  retry-delay = 10 ms
          |  max-retries = 1
          |}
          |recovery {
          |  parallelism = 1
          |  inspect-timeout = 1 s
          |}
          |""".stripMargin
      )

      val parsed = FlushConfig.fromConfig(config, testKit.system)

      parsed.recovery.perEntityTimeout shouldBe 120.seconds
    }
  }
}
