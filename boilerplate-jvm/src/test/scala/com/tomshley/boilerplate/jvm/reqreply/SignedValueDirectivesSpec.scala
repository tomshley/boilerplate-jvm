/*
 * copyright 2023 tomshley llc
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
package com.tomshley.boilerplate.jvm.reqreply

import com.tomshley.boilerplate.jvm.reqreply.exceptions.{ExpiredExpiringValueRejection, ExpiringValueRejection}
import com.tomshley.boilerplate.jvm.security.tokens.{ExpiringSignedValue, Keyring, TokenProfile}
import org.apache.pekko.http.scaladsl.server.Directives.*
import org.apache.pekko.http.scaladsl.server.Route
import org.apache.pekko.http.scaladsl.testkit.ScalatestRouteTest
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import java.time.Instant
import scala.concurrent.duration.*

final class SignedValueDirectivesSpec extends AnyWordSpec with Matchers with ScalatestRouteTest {

  private val keyHex = "00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff"

  private val keyring = Keyring.fromHex(Map(1 -> keyHex))
  private val profile =
    TokenProfile("directive-spec-v1\n", version = 1, payloadLength = 0, grace = Duration.Zero)

  private object Signing extends SignedValueDirectives {
    override protected val signedValueKeyring: Keyring = keyring
    override protected val signedValueProfile: TokenProfile = profile
  }

  private object Unconfigured extends SignedValueDirectives {
    override protected val signedValueKeyring: Keyring = Keyring.empty
    override protected val signedValueProfile: TokenProfile = profile
  }

  private def route(directives: SignedValueDirectives): Route =
    path("open" / directives.signedPathMatcher) { encoded =>
      directives.signedValue(encoded) { verified => complete(verified.value) }
    }

  "the signed-value directive" should {

    "provide the value of a genuine encoding" in {
      val encoded = Signing.signValue("order-4711", 5.minutes)

      Get(s"/open/$encoded") ~> route(Signing) ~> check {
        responseAs[String] shouldBe "order-4711"
      }
    }

    "reject an expired value as expired, so the caller knows to refresh" in {
      val expired = ExpiringSignedValue.sign(keyring, profile, "stale", Instant.now().minusSeconds(60))

      Get(s"/open/$expired") ~> route(Signing) ~> check {
        rejection shouldBe ExpiredExpiringValueRejection("The value has expired")
      }
    }

    "reject tampering, an unknown key, and garbage identically — no verification oracle" in {
      val encoded = Signing.signValue("target", 5.minutes)
      val bytes = java.util.Base64.getUrlDecoder.decode(encoded)
      bytes(bytes.length - 1) = (bytes(bytes.length - 1) ^ 0x01).toByte
      val tampered = java.util.Base64.getUrlEncoder.withoutPadding.encodeToString(bytes)

      val underAnotherKeyring = keyring.retire(1).withKey(2, keyHex)
      val unknownKey =
        ExpiringSignedValue.sign(underAnotherKeyring, profile, "elsewhere", Instant.now().plusSeconds(600))

      val generic = ExpiringValueRejection("Unable to validate the signed value")

      Seq(tampered, unknownKey, "notavalidvalue").foreach { candidate =>
        Get(s"/open/$candidate") ~> route(Signing) ~> check {
          rejection shouldBe generic
        }
      }
    }

    "verify nothing when no key is configured" in {
      val encoded = Signing.signValue("unverifiable-here", 5.minutes)

      Get(s"/open/$encoded") ~> route(Unconfigured) ~> check {
        rejection shouldBe ExpiringValueRejection("Unable to validate the signed value")
      }
    }

    "refuse to sign when no key is configured, naming the setting" in {
      val failure = the[IllegalStateException] thrownBy Unconfigured.signValue("anything", 5.minutes)
      failure.getMessage should include("tomshley-boilerplate-reqreply-signing.keys")
    }
  }
}