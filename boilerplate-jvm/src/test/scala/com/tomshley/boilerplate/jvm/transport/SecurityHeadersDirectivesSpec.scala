/*
 * copyright 2017–present tomshley llc
 *
 * licensed under the apache license, version 2.0 (the "license");
 * you may not use this file except in compliance with the license.
 * you may obtain a copy of the license at
 *
 * http://www.apache.org/licenses/license-2.0
 *
 * unless required by applicable law or agreed to in writing, software
 * distributed under the license is distributed on an "as is" basis,
 * without warranties or conditions of any kind, either express or implied.
 * see the license for the specific language governing permissions and
 * limitations under the license.
 *
 * @author thomas schena @sgoggles <https://github.com/sgoggles> | <https://gitlab.com/sgoggles>
 *
 */

package com.tomshley.boilerplate.jvm.transport

import org.apache.pekko.http.scaladsl.model.StatusCodes
import org.apache.pekko.http.scaladsl.model.headers.{RawHeader, `Strict-Transport-Security`}
import org.apache.pekko.http.scaladsl.server.Directives.*
import org.apache.pekko.http.scaladsl.server.Route
import org.apache.pekko.http.scaladsl.testkit.ScalatestRouteTest
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

final class SecurityHeadersDirectivesSpec
    extends AnyWordSpec
    with Matchers
    with ScalatestRouteTest {

  private object Fixture extends SecurityHeadersDirectives

  "respondWithSecurityHeaders" should {
    "emit the baseline set and no CSP when none is supplied" in {
      Get("/probe") ~> Fixture.respondWithSecurityHeaders() {
        complete(StatusCodes.OK)
      } ~> check {
        header("X-Content-Type-Options").map(_.value()) shouldBe Some("nosniff")
        header("X-Frame-Options").map(_.value()) shouldBe Some("DENY")
        header("Referrer-Policy").map(_.value()) shouldBe Some("strict-origin-when-cross-origin")
        header("Permissions-Policy").map(_.value()) shouldBe
          Some("camera=(), microphone=(), geolocation=()")
        header("Strict-Transport-Security").map(_.value()) shouldBe
          Some("max-age=31536000; includeSubDomains")
        header("Content-Security-Policy") shouldBe None
      }
    }

    "emit the consumer's CSP verbatim and drop HSTS when disabled" in {
      val csp = "default-src 'self'; object-src 'none'"
      Get("/probe") ~> Fixture.respondWithSecurityHeaders(
        contentSecurityPolicy = Some(csp),
        transportSecurity = None
      ) {
        complete(StatusCodes.OK)
      } ~> check {
        header("Content-Security-Policy").map(_.value()) shouldBe Some(csp)
        header("Strict-Transport-Security") shouldBe None
      }
    }

    "render an HSTS header without includeSubDomains when not requested" in {
      Get("/probe") ~> Fixture.respondWithSecurityHeaders(
        transportSecurity = Some(`Strict-Transport-Security`(600L, includeSubDomains = false))
      ) {
        complete(StatusCodes.OK)
      } ~> check {
        header("Strict-Transport-Security").map(_.value()) shouldBe Some("max-age=600")
      }
    }

    "drop frame options when disabled" in {
      Get("/probe") ~> Fixture.respondWithSecurityHeaders(frameOptions = None) {
        complete(StatusCodes.OK)
      } ~> check {
        header("X-Frame-Options") shouldBe None
      }
    }

    "let an inner route's own header win over the default" in {
      Get("/probe") ~> Fixture.respondWithSecurityHeaders() {
        respondWithHeader(RawHeader("X-Frame-Options", "SAMEORIGIN")) {
          complete(StatusCodes.OK)
        }
      } ~> check {
        headers.count(_.is("x-frame-options")) shouldBe 1
        header("X-Frame-Options").map(_.value()) shouldBe Some("SAMEORIGIN")
      }
    }

    "cover error responses when the wrapped route is sealed" in {
      Get("/nope") ~> Fixture.respondWithSecurityHeaders() {
        Route.seal(path("probe")(get(complete(StatusCodes.OK))))
      } ~> check {
        status shouldBe StatusCodes.NotFound
        header("X-Content-Type-Options").map(_.value()) shouldBe Some("nosniff")
        header("X-Frame-Options").map(_.value()) shouldBe Some("DENY")
      }
    }
  }
}
