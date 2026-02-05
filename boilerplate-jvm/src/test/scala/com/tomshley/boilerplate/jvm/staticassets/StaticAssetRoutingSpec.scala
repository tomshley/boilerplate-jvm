package com.tomshley.boilerplate.jvm.staticassets

import com.tomshley.boilerplate.jvm.staticassets.exceptions.StaticAssetRoutingRejection
import org.apache.pekko.http.scaladsl.model.StatusCodes
import org.apache.pekko.http.scaladsl.testkit.ScalatestRouteTest
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

final class StaticAssetRoutingSpec extends AnyWordSpec with Matchers with ScalatestRouteTest {

  import org.apache.pekko.actor.typed.scaladsl.adapter.*

  private object UnderTest extends StaticAssetRouting

  private val routes = UnderTest.getStaticAssetRoute(system.toTyped)

  "StaticAssetRouting" should {
    "serve a JS asset with a javascript content type" in {
      Get("/static/test.js") ~> routes ~> check {
        status shouldBe StatusCodes.OK
        contentType.mediaType.value shouldBe "text/javascript"
        responseAs[String].trim should include("console.log")
      }
    }

    "serve a CSS asset with a css content type" in {
      Get("/static/test.css") ~> routes ~> check {
        status shouldBe StatusCodes.OK
        contentType.mediaType.value shouldBe "text/css"
        responseAs[String].trim should include("body")
      }
    }

    "reject unsupported extensions" in {
      Get("/static/test.xyz") ~> routes ~> check {
        rejections.length should be > 0
        rejections.exists(_.isInstanceOf[StaticAssetRoutingRejection]) shouldBe true
      }
    }

    "reject when the resource does not exist" in {
      Get("/static/does-not-exist.js") ~> routes ~> check {
        rejections.exists(_.isInstanceOf[StaticAssetRoutingRejection]) shouldBe true
      }
    }
  }
}
