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

/**
 * Every gate in here makes verification FAIL on purpose. A verifier is only
 * trusted after each rejection path has been demonstrated, distinctly.
 */
final class CompactMacTokenNegativeSpec extends AnyWordSpec with Matchers {

  private val keyOne = "00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff"
  private val keyTwo = "ffeeddccbbaa99887766554433221100ffeeddccbbaa99887766554433221100"

  private val keyring = Keyring.fromHex(Map(1 -> keyOne, 2 -> keyTwo))
  private val profile = TokenProfile("negative-family-v1\n", version = 3, payloadLength = 12, grace = 72.hours)

  private val freshToken =
    CompactMacToken.mint(keyring, profile, MintedPimpedBytes(Array.tabulate[Byte](12)(_.toByte)), 20_000)

  private def rejectionOf(token: String): RejectionReason =
    CompactMacToken.verify(keyring, profile, token, Instant.EPOCH) match {
      case Left(reason)    => reason
      case Right(verified) => fail(s"expected rejection, got $verified")
    }

  private def withFlippedBit(token: String, byteIndex: Int): String = {
    val bytes = java.util.Base64.getUrlDecoder.decode(token)
    bytes(byteIndex) = (bytes(byteIndex) ^ 0x01).toByte
    java.util.Base64.getUrlEncoder.withoutPadding.encodeToString(bytes)
  }

  "a bit-flipped token" should {

    "reject with SignatureMismatch for a flip in every MAC byte" in {
      (15 until 27).foreach { index =>
        rejectionOf(withFlippedBit(freshToken, index)) shouldBe RejectionReason.SignatureMismatch
      }
    }

    "reject with SignatureMismatch for a flip in the payload" in {
      rejectionOf(withFlippedBit(freshToken, 5)) shouldBe RejectionReason.SignatureMismatch
    }

    "reject with SignatureMismatch for a flip in the expiry bytes — never Expired" in {
      rejectionOf(withFlippedBit(freshToken, 13)) shouldBe RejectionReason.SignatureMismatch
      rejectionOf(withFlippedBit(freshToken, 14)) shouldBe RejectionReason.SignatureMismatch
    }
  }

  "an unknown key id" should {

    "reject with UnknownKey, distinctly from a bad signature" in {
      val token = CompactMacToken.mint(keyring, profile, MintedPimpedBytes(new Array[Byte](12)), 1, kid = 2)
      CompactMacToken.verify(keyring.retire(2), profile, token, Instant.EPOCH) shouldBe
        Left(RejectionReason.UnknownKey)
    }

    "reject everything against an empty keyring" in {
      CompactMacToken.verify(Keyring.empty, profile, freshToken, Instant.EPOCH) shouldBe
        Left(RejectionReason.UnknownKey)
    }
  }

  "rotation" should {

    "stop verifying after retire and resume after the key is re-added" in {
      val token = CompactMacToken.mint(keyring, profile, MintedPimpedBytes(new Array[Byte](12)), 1, kid = 2)

      val retired = keyring.retire(2)
      CompactMacToken.verify(retired, profile, token, Instant.EPOCH) shouldBe Left(RejectionReason.UnknownKey)

      val restored = retired.withKey(2, keyTwo)
      CompactMacToken.verify(restored, profile, token, Instant.EPOCH).isRight shouldBe true
    }

    "reject a token minted with a different key in the same slot" in {
      val swapped = keyring.withKey(2, keyOne)
      val token = CompactMacToken.mint(keyring, profile, MintedPimpedBytes(new Array[Byte](12)), 1, kid = 2)
      CompactMacToken.verify(swapped, profile, token, Instant.EPOCH) shouldBe
        Left(RejectionReason.SignatureMismatch)
    }
  }

  "version bits" should {

    "reject a token from another version with UnknownVersion" in {
      val otherVersion = profile.copy(version = 2)
      val otherToken = CompactMacToken.mint(keyring, otherVersion, MintedPimpedBytes(new Array[Byte](12)), 1)
      rejectionOf(otherToken) shouldBe RejectionReason.UnknownVersion
    }
  }

  "malformed input" should {

    "reject a token one byte short or long as Malformed" in {
      val bytes = java.util.Base64.getUrlDecoder.decode(freshToken)
      val short = java.util.Base64.getUrlEncoder.withoutPadding.encodeToString(bytes.dropRight(1))
      val long = java.util.Base64.getUrlEncoder.withoutPadding.encodeToString(bytes :+ 0.toByte)
      rejectionOf(short) shouldBe RejectionReason.Malformed
      rejectionOf(long) shouldBe RejectionReason.Malformed
    }

    "reject padding and the standard-alphabet characters as Malformed" in {
      rejectionOf(freshToken + "==") shouldBe RejectionReason.Malformed
      rejectionOf("+" + freshToken.drop(1)) shouldBe RejectionReason.Malformed
      rejectionOf("/" + freshToken.drop(1)) shouldBe RejectionReason.Malformed
    }

    "reject the empty string, whitespace, and garbage as Malformed" in {
      rejectionOf("") shouldBe RejectionReason.Malformed
      rejectionOf(" " * 36) shouldBe RejectionReason.Malformed
      rejectionOf("not a token") shouldBe RejectionReason.Malformed
    }

    "never throw on adversarial input" in {
      noException should be thrownBy CompactMacToken.verify(keyring, profile, "\u0000" * 36, Instant.EPOCH)
      noException should be thrownBy CompactMacToken.verify(keyring, profile, "A" * 10_000, Instant.EPOCH)
      noException should be thrownBy CompactMacToken.verify(keyring, profile, null, Instant.EPOCH)
    }
  }

  "a signed value fed to the compact verifier" should {

    "reject as Malformed (different length family)" in {
      val encoded = ExpiringSignedValue.sign(keyring, profile, "cross-format", Instant.ofEpochMilli(1_000_000L))
      rejectionOf(encoded) shouldBe RejectionReason.Malformed
    }
  }
}