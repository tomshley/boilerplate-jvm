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

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

final class KeyringSpec extends AnyWordSpec with Matchers {

  private val keyOne = "00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff"
  private val keyTwo = "ffeeddccbbaa99887766554433221100ffeeddccbbaa99887766554433221100"

  "a keyring" should {

    "expose slot metadata only" in {
      val keyring = Keyring.fromHex(Map(1 -> keyOne, 3 -> keyTwo))
      keyring.kids shouldBe Set(1, 3)
      keyring.activeKid shouldBe Some(3)
      keyring.nonEmpty shouldBe true
    }

    "treat the highest slot as active and follow retirement" in {
      val keyring = Keyring.fromHex(Map(1 -> keyOne, 3 -> keyTwo))
      keyring.retire(3).activeKid shouldBe Some(1)
      keyring.retire(3).retire(1).activeKid shouldBe None
    }

    "be immutable: withKey and retire return new keyrings" in {
      val original = Keyring.fromHex(Map(1 -> keyOne))
      val grown = original.withKey(2, keyTwo)
      val shrunk = grown.retire(1)

      original.kids shouldBe Set(1)
      grown.kids shouldBe Set(1, 2)
      shrunk.kids shouldBe Set(2)
    }

    "reject slots outside 0..31" in {
      an[IllegalArgumentException] should be thrownBy Keyring.fromHex(Map(32 -> keyOne))
      an[IllegalArgumentException] should be thrownBy Keyring.fromHex(Map(-1 -> keyOne))
      an[IllegalArgumentException] should be thrownBy Keyring.empty.withKey(32, keyOne)
    }

    "reject empty, odd-length, and non-hex key material" in {
      an[IllegalArgumentException] should be thrownBy Keyring.fromHex(Map(1 -> ""))
      an[IllegalArgumentException] should be thrownBy Keyring.fromHex(Map(1 -> "abc"))
      an[IllegalArgumentException] should be thrownBy Keyring.fromHex(Map(1 -> "zz11"))
    }

    "never print key material" in {
      val keyring = Keyring.fromHex(Map(1 -> keyOne, 2 -> keyTwo))
      keyring.toString shouldBe "Keyring(kids=1,2)"
      keyring.toString should not include "0011"
      keyring.toString should not include "ffee"
    }
  }

  "the key-material container" should {

    "redact its contents in toString — a stack trace or log line can never carry a key" in {
      val secret = SecretBytes.fromHex(keyOne)
      secret.toString shouldBe "SecretBytes(length=32)"
      secret.toString should not include "0011"
    }

    "hash by length only, so keys cannot be probed through hash collisions" in {
      SecretBytes.fromHex(keyOne).hashCode shouldBe SecretBytes.fromHex(keyTwo).hashCode
      SecretBytes.fromHex(keyOne) should not equal SecretBytes.fromHex(keyTwo)
      SecretBytes.fromHex(keyOne) shouldEqual SecretBytes.fromHex(keyOne.toUpperCase)
    }
  }
}