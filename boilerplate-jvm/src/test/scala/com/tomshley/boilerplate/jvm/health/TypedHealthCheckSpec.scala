package com.tomshley.boilerplate.jvm.health

import org.apache.pekko.actor.testkit.typed.scaladsl.ActorTestKit
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

final class TypedHealthCheckSpec extends AnyWordSpec with Matchers with ScalaFutures with BeforeAndAfterAll {
  private val testKit = ActorTestKit("TypedHealthCheckSpec")
  private given org.apache.pekko.actor.typed.ActorSystem[?] = testKit.system

  override def afterAll(): Unit = {
    testKit.shutdownTestKit()
    super.afterAll()
  }

  "TypedHealthCheck" should {
    "return Future(true)" in {
      val hc = new TypedHealthCheck
      hc().futureValue shouldBe true
    }
  }
}
