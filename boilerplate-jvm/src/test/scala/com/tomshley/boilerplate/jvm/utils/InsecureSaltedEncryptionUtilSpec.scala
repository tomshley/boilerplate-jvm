package com.tomshley.boilerplate.jvm.utils

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

final class InsecureSaltedEncryptionUtilSpec extends AnyWordSpec with Matchers {

  private object UnderTest extends InsecureSaltedEncryptionUtil

  "InsecureSaltedEncryptionUtil" should {
    "encrypt and decrypt round-trip" in {
      val plaintext = "hello"
      val encrypted = UnderTest.encrypt(plaintext)
      UnderTest.decrypt(encrypted) shouldBe plaintext
    }

    "encryptBase64Hmac produce URL-safe string" in {
      val encoded = UnderTest.encryptBase64Hmac("hello")
      UnderTest.base64URLSafeRegex.matches(encoded) shouldBe true
    }

    "encryptBase64Hmac and decryptBase64Hmac round-trip" in {
      val plaintext = "hello"
      val encoded = UnderTest.encryptBase64Hmac(plaintext)
      UnderTest.decryptBase64Hmac(encoded) shouldBe plaintext
    }
  }
}
