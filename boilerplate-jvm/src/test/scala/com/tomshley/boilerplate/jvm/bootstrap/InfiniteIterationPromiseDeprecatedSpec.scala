package com.tomshley.boilerplate.jvm.bootstrap

import org.apache.pekko.actor.testkit.typed.scaladsl.ActorTestKit
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.Eventually
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.time.{Milliseconds, Seconds, Span}

import java.util.concurrent.atomic.AtomicInteger

final class InfiniteIterationPromiseDeprecatedSpec
    extends AnyWordSpec
    with Matchers
    with ScalaFutures
    with Eventually
    with BeforeAndAfterAll {

  override implicit val patienceConfig: PatienceConfig =
    PatienceConfig(timeout = Span(3, Seconds), interval = Span(25, Milliseconds))

  private val testKit = ActorTestKit("InfiniteIterationPromiseDeprecatedSpec")

  override def afterAll(): Unit = {
    testKit.shutdownTestKit()
    super.afterAll()
  }

  "InfiniteIterationPromise" should {
    "keep looping until interrupted by a failure" in {
      val counter = new AtomicInteger(0)

      val result = InfiniteIterationPromise(testKit.system, {
        val current = counter.incrementAndGet()
        if (current >= 5) throw new RuntimeException("stop")
      })

      eventually {
        counter.get() should be >= 5
      }

      result.failed.futureValue.getMessage shouldBe "stop"
    }
  }
}
