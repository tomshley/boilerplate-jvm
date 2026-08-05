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

final class ExpiringSignedValueSpec extends AnyWordSpec with Matchers {

  private val keyOne = "00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff"
  private val keyring = Keyring.fromHex(Map(1 -> keyOne))
  private val profile = TokenProfile("value-family-v1\n", version = 1, payloadLength = 12, grace = Duration.Zero)

  private val expiry = Instant.ofEpochMilli(1_700_000_000_000L)

  "sign and verify" should {

    "round-trip value, expiry, and kid" in {
      val encoded = ExpiringSignedValue.sign(keyring, profile, "a-plain-value", expiry)
      encoded should fullyMatch regex "^[A-Za-z0-9_-]+$"

      ExpiringSignedValue.verify(keyring, profile, encoded, expiry.minusSeconds(60)) shouldBe
        Right(ExpiringSignedValue("a-plain-value", expiry, kid = 1))
    }

    "round-trip an empty value and a multi-byte unicode value" in {
      val empty = ExpiringSignedValue.sign(keyring, profile, "", expiry)
      ExpiringSignedValue.verify(keyring, profile, empty, Instant.EPOCH).map(_.value) shouldBe Right("")

      val unicode = "überschrift-\u00e9\u4e16\u754c"
      val encoded = ExpiringSignedValue.sign(keyring, profile, unicode, expiry)
      ExpiringSignedValue.verify(keyring, profile, encoded, Instant.EPOCH).map(_.value) shouldBe Right(unicode)
    }

    "treat exactly the expiry as valid and one millisecond past it as expired (zero grace)" in {
      val encoded = ExpiringSignedValue.sign(keyring, profile, "boundary", expiry)
      ExpiringSignedValue.verify(keyring, profile, encoded, expiry).isRight shouldBe true
      ExpiringSignedValue.verify(keyring, profile, encoded, expiry.plusMillis(1)) shouldBe
        Left(RejectionReason.Expired)
    }

    "honor grace" in {
      val graceful = profile.copy(grace = 5.minutes)
      val encoded = ExpiringSignedValue.sign(keyring, graceful, "graceful", expiry)
      val boundary = expiry.plusMillis(5.minutes.toMillis)

      ExpiringSignedValue.verify(keyring, graceful, encoded, boundary).isRight shouldBe true
      ExpiringSignedValue.verify(keyring, graceful, encoded, boundary.plusMillis(1)) shouldBe
        Left(RejectionReason.Expired)
    }

    "refuse to sign from an empty keyring" in {
      an[IllegalStateException] should be thrownBy
        ExpiringSignedValue.sign(Keyring.empty, profile, "anything", expiry)
    }
  }

  "tampering" should {

    "reject a value-byte flip with SignatureMismatch" in {
      val encoded = ExpiringSignedValue.sign(keyring, profile, "tamper-target", expiry)
      val bytes = java.util.Base64.getUrlDecoder.decode(encoded)
      bytes(10) = (bytes(10) ^ 0x01).toByte // inside the value region
      val tampered = java.util.Base64.getUrlEncoder.withoutPadding.encodeToString(bytes)

      ExpiringSignedValue.verify(keyring, profile, tampered, Instant.EPOCH) shouldBe
        Left(RejectionReason.SignatureMismatch)
    }

    "reject an expiry rollback with SignatureMismatch — expiry is authenticated" in {
      val encoded = ExpiringSignedValue.sign(keyring, profile, "rollback-target", expiry)
      val bytes = java.util.Base64.getUrlDecoder.decode(encoded)
      bytes(4) = (bytes(4) ^ 0x40).toByte // inside the expiry region
      val tampered = java.util.Base64.getUrlEncoder.withoutPadding.encodeToString(bytes)

      ExpiringSignedValue.verify(keyring, profile, tampered, Instant.EPOCH) shouldBe
        Left(RejectionReason.SignatureMismatch)
    }

    "reject a non-canonical encoding of the very same bytes as Malformed" in {
      // A 1-byte value makes the decoded length 22 bytes = 176 bits, which
      // 30 base64 characters over-covers by 4 bits. Those slack bits give an
      // attacker a second wire form of an authentic value unless the decoder
      // insists on the canonical one. (Compact tokens have no slack: 27 bytes
      // is exactly 36 characters.)
      val encoded = ExpiringSignedValue.sign(keyring, profile, "x", expiry)
      val decoded = java.util.Base64.getUrlDecoder.decode(encoded)
      val alphabet = (('A' to 'Z') ++ ('a' to 'z') ++ ('0' to '9') ++ Seq('-', '_')).map(_.toString)

      val nonCanonical = alphabet
        .map(encoded.dropRight(1) + _)
        .filter(_ != encoded)
        .find(candidate => java.util.Base64.getUrlDecoder.decode(candidate).sameElements(decoded))

      withClue("the slack-bit seam must exist for this test to mean anything: ") {
        nonCanonical should not be empty
      }
      ExpiringSignedValue.verify(keyring, profile, nonCanonical.get, Instant.EPOCH) shouldBe
        Left(RejectionReason.Malformed)
    }

    "reject truncation, padding, and garbage as Malformed" in {
      val encoded = ExpiringSignedValue.sign(keyring, profile, "malformed-target", expiry)
      ExpiringSignedValue.verify(keyring, profile, encoded.take(10), Instant.EPOCH) shouldBe
        Left(RejectionReason.Malformed)
      ExpiringSignedValue.verify(keyring, profile, encoded + "=", Instant.EPOCH) shouldBe
        Left(RejectionReason.Malformed)
      ExpiringSignedValue.verify(keyring, profile, "", Instant.EPOCH) shouldBe
        Left(RejectionReason.Malformed)
    }
  }

  "key lifecycle" should {

    "reject after the signing key is retired and resume when re-added" in {
      val encoded = ExpiringSignedValue.sign(keyring, profile, "lifecycle", expiry)

      ExpiringSignedValue.verify(keyring.retire(1), profile, encoded, Instant.EPOCH) shouldBe
        Left(RejectionReason.UnknownKey)

      ExpiringSignedValue.verify(keyring.retire(1).withKey(1, keyOne), profile, encoded, Instant.EPOCH)
        .isRight shouldBe true
    }
  }

  "cross-format separation" should {

    "never verify a compact token as a signed value, even with a colliding length" in {
      // A compact token with payloadLength 12 decodes to 27 bytes; a signed value
      // with a 6-byte value also decodes to 27 bytes. Same profile, same keyring,
      // same length — the family label in the MAC domain must still separate them.
      val compact = CompactMacToken.mint(keyring, profile, MintedPimpedBytes(new Array[Byte](12)), 1)
      ExpiringSignedValue.verify(keyring, profile, compact, Instant.EPOCH) shouldBe
        Left(RejectionReason.SignatureMismatch)
    }
  }
}