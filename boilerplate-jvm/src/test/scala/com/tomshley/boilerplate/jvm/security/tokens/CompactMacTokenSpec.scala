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
package com.tomshley.boilerplate.jvm.security.tokens

import com.tomshley.boilerplate.jvm.basics.MintedPimpedBytes
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import java.time.Instant
import scala.concurrent.duration.*
import scala.util.Random

final class CompactMacTokenSpec extends AnyWordSpec with Matchers {

  private val keyOne = "00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff"
  private val keyTwo = "ffeeddccbbaa99887766554433221100ffeeddccbbaa99887766554433221100"

  private val keyring = Keyring.fromHex(Map(1 -> keyOne, 2 -> keyTwo))
  private val profile = TokenProfile("round-trip-family-v1\n", version = 3, payloadLength = 12, grace = 72.hours)

  private val millisPerDay = 86_400_000L

  private def payloadOf(bytes: Byte*): MintedPimpedBytes = MintedPimpedBytes(bytes.toArray)

  "mint and verify" should {

    "round-trip payload, kid, and expiry on the active slot" in {
      val payload = MintedPimpedBytes(Array.tabulate[Byte](12)(_.toByte))
      val token = CompactMacToken.mint(keyring, profile, payload, 20_000)

      token should fullyMatch regex "^[A-Za-z0-9_-]+$"
      token should have length CompactMacToken.encodedLength(profile)

      CompactMacToken.verify(keyring, profile, token, Instant.EPOCH) shouldBe
        Right(VerifiedToken(kid = 2, payload = payload, expiryDaysSinceEpoch = 20_000))
    }

    "round-trip random payloads across kids and expiries (seeded property sweep)" in {
      val random = new Random(20260804L)
      (1 to 100).foreach { _ =>
        val payload = MintedPimpedBytes(Array.fill[Byte](12)(random.nextInt().toByte))
        val kid = if (random.nextBoolean()) 1 else 2
        val expiryDays = random.nextInt(0x10000)
        val token = CompactMacToken.mint(keyring, profile, payload, expiryDays, kid)

        CompactMacToken.verify(keyring, profile, token, Instant.EPOCH) shouldBe
          Right(VerifiedToken(kid, payload, expiryDays))
      }
    }

    "treat exactly the grace boundary as valid and one millisecond past it as expired" in {
      val token = CompactMacToken.mint(keyring, profile, MintedPimpedBytes(new Array[Byte](12)), 10_000)
      val boundary = 10_000L * millisPerDay + profile.grace.toMillis

      CompactMacToken.verify(keyring, profile, token, Instant.ofEpochMilli(boundary)).isRight shouldBe true
      CompactMacToken.verify(keyring, profile, token, Instant.ofEpochMilli(boundary + 1L)) shouldBe
        Left(RejectionReason.Expired)
    }

    "honor zero grace" in {
      val zeroGrace = profile.copy(grace = Duration.Zero)
      val token = CompactMacToken.mint(keyring, zeroGrace, MintedPimpedBytes(new Array[Byte](12)), 100)

      CompactMacToken.verify(keyring, zeroGrace, token, Instant.ofEpochMilli(100L * millisPerDay)).isRight shouldBe true
      CompactMacToken.verify(keyring, zeroGrace, token, Instant.ofEpochMilli(100L * millisPerDay + 1L)) shouldBe
        Left(RejectionReason.Expired)
    }

    "reject minting with a wrong payload length, out-of-range expiry, or unknown kid" in {
      an[IllegalArgumentException] should be thrownBy
        CompactMacToken.mint(keyring, profile, MintedPimpedBytes(new Array[Byte](11)), 1)
      an[IllegalArgumentException] should be thrownBy
        CompactMacToken.mint(keyring, profile, MintedPimpedBytes(new Array[Byte](13)), 1)
      an[IllegalArgumentException] should be thrownBy
        CompactMacToken.mint(keyring, profile, MintedPimpedBytes(new Array[Byte](12)), -1)
      an[IllegalArgumentException] should be thrownBy
        CompactMacToken.mint(keyring, profile, MintedPimpedBytes(new Array[Byte](12)), 0x10000)
      an[IllegalArgumentException] should be thrownBy
        CompactMacToken.mint(keyring, profile, MintedPimpedBytes(new Array[Byte](12)), 1, kid = 7)
    }

    "refuse to mint from an empty keyring" in {
      an[IllegalStateException] should be thrownBy
        CompactMacToken.mint(Keyring.empty, profile, MintedPimpedBytes(new Array[Byte](12)), 1)
    }

    "support a zero-length payload profile" in {
      val slim = TokenProfile("slim-family-v1\n", version = 1, payloadLength = 0, grace = Duration.Zero)
      val token = CompactMacToken.mint(keyring, slim, MintedPimpedBytes.empty, 42)

      CompactMacToken.verify(keyring, slim, token, Instant.EPOCH) shouldBe
        Right(VerifiedToken(2, MintedPimpedBytes.empty, 42))
    }

    "compose as a ladder via Either" in {
      val payload = payloadOf(9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9)
      val token = CompactMacToken.mint(keyring, profile, payload, 5)
      val fallback = TokenProfile("other-family-v1\n", version = 1, payloadLength = 12, grace = Duration.Zero)

      val ladder = CompactMacToken
        .verify(keyring, fallback, token, Instant.EPOCH)
        .orElse(CompactMacToken.verify(keyring, profile, token, Instant.EPOCH))

      ladder.map(_.payload) shouldBe Right(payload)
    }
  }
}