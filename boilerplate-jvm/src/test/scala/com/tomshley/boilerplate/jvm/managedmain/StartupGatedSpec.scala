package com.tomshley.boilerplate.jvm.managedmain

import org.apache.pekko.actor.typed.ActorSystem
import org.apache.pekko.actor.typed.scaladsl.Behaviors
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{Milliseconds, Seconds, Span}
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.{Await, ExecutionContext, Future, Promise}
import scala.concurrent.duration.*

final class StartupGatedSpec
    extends AnyWordSpec
    with Matchers
    with ScalaFutures {

  override implicit val patienceConfig: PatienceConfig =
    PatienceConfig(timeout = Span(3, Seconds), interval = Span(25, Milliseconds))

  private given ExecutionContext = ExecutionContext.global

  "Startup.gated" should {
    "invoke onReady when the prerequisite succeeds" in {
      val system = ActorSystem(Behaviors.empty, "StartupGatedSpecSuccess")
      val ready = Promise[Int]()

      try {
        val startup = Startup.gated("success", Future.successful(42), 1.second) { value =>
          ready.success(value)
        }

        startup.run(using system)

        ready.future.futureValue shouldBe 42
      } finally {
        shutdown(system)
      }
    }

    "terminate the ActorSystem when the prerequisite fails" in {
      val system = ActorSystem(Behaviors.empty, "StartupGatedSpecFailure")

      try {
        val startup = Startup.gated[Int]("failure", Future.failed(new RuntimeException("boom")), 1.second) {
          _ => fail("onReady should not be called for a failed prerequisite")
        }

        startup.run(using system)

        system.whenTerminated.futureValue
        succeed
      } finally {
        shutdown(system)
      }
    }

    "terminate the ActorSystem when the prerequisite times out" in {
      val system = ActorSystem(Behaviors.empty, "StartupGatedSpecTimeout")
      val never = Promise[Int]()

      try {
        val startup = Startup.gated("timeout", never.future, 100.millis) { _ =>
          fail("onReady should not be called for a timed out prerequisite")
        }

        startup.run(using system)

        system.whenTerminated.futureValue
        succeed
      } finally {
        shutdown(system)
      }
    }
  }

  private def shutdown(system: ActorSystem[?]): Unit = {
    if (!system.whenTerminated.isCompleted) {
      system.terminate()
    }
    Await.ready(system.whenTerminated, 3.seconds)
    ()
  }
}
