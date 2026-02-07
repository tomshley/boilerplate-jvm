package com.tomshley.boilerplate.jvm.health

import org.apache.pekko.actor.ActorSystem
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

final class HealthCheckSpec extends AnyWordSpec with Matchers with ScalaFutures {

  "HealthCheck" should {
    "return Future(true)" in {
      val system = ActorSystem("healthcheck")
      try {
        val hc = new HealthCheck(system)
        hc().futureValue shouldBe true
      } finally {
        system.terminate()
      }
    }
  }
}
