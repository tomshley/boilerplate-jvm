package com.tomshley.boilerplate.jvm.bootstrap

import org.scalatest.concurrent.Eventually
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.time.{Milliseconds, Seconds, Span}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Promise
import scala.concurrent.Future

final class LoadStatusStartupSpec
    extends AnyWordSpec
    with Matchers
    with ScalaFutures
    with Eventually {

  override implicit val patienceConfig: PatienceConfig =
    PatienceConfig(timeout = Span(3, Seconds), interval = Span(25, Milliseconds))

  "LoadStatusStartup.start" should {
    "block until the provided Future completes" in {
      val p = Promise[Boolean]()

      val startF: Future[Unit] = Future {
        LoadStatusStartup.start(p.future)
      }

      Thread.sleep(150)
      startF.isCompleted shouldBe false

      p.success(true)

      eventually {
        startF.isCompleted shouldBe true
      }
    }
  }
}
