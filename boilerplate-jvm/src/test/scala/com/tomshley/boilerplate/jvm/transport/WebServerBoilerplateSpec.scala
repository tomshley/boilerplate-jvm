package com.tomshley.boilerplate.jvm.transport

import org.apache.pekko.actor.typed.ActorSystem
import org.apache.pekko.actor.typed.scaladsl.Behaviors
import org.apache.pekko.http.scaladsl.Http
import org.apache.pekko.http.scaladsl.model.StatusCodes
import org.apache.pekko.http.scaladsl.model.Uri
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.ExecutionContext

final class WebServerBoilerplateSpec extends AnyWordSpec with Matchers with ScalaFutures {

  "WebServerBoilerplate" should {
    "bind and serve /heartbeat" in {
      val system: ActorSystem[Nothing] = ActorSystem(Behaviors.empty, "WebServerBoilerplateSpec")
      given ExecutionContext = system.executionContext

      import org.apache.pekko.actor.typed.scaladsl.adapter.*
      given classicSystem: org.apache.pekko.actor.ActorSystem = system.toClassic

      val binding = WebServerBoilerplate.start("127.0.0.1", 0, system, Seq.empty).futureValue
      val port = binding.localAddress.getPort

      val response = Http().singleRequest(
        org.apache.pekko.http.scaladsl.model.HttpRequest(uri = Uri(s"http://127.0.0.1:$port/heartbeat"))
      ).futureValue

      response.status shouldBe StatusCodes.OK

      binding.unbind().futureValue
      system.terminate()
    }
  }
}
