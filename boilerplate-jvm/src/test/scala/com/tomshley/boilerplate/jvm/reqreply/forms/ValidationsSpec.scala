package com.tomshley.boilerplate.jvm.reqreply.forms

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

final class ValidationsSpec extends AnyWordSpec with Matchers with Validations {

  "Validations.isValidEmailFormat" should {
    "return true for valid email" in {
      isValidEmailFormat("test@example.com") shouldBe true
    }
    "return true for email with subdomain" in {
      isValidEmailFormat("user@mail.example.co.uk") shouldBe true
    }
    "return false for email without @" in {
      isValidEmailFormat("testexample.com") shouldBe false
    }
    "return false for email without domain" in {
      isValidEmailFormat("test@") shouldBe false
    }
    "return false for empty string" in {
      isValidEmailFormat("") shouldBe false
    }
  }

  "Validations.isValidPhoneFormat" should {
    "return true for 10-digit phone" in {
      isValidPhoneFormat("1234567890") shouldBe true
    }
    "return true for phone with dashes" in {
      isValidPhoneFormat("123-456-7890") shouldBe true
    }
    "return true for phone with parentheses" in {
      isValidPhoneFormat("(123) 456-7890") shouldBe true
    }
    "return true for phone with country code" in {
      isValidPhoneFormat("+1 123-456-7890") shouldBe true
    }
    "return false for short number" in {
      isValidPhoneFormat("12345") shouldBe false
    }
    "return false for letters" in {
      isValidPhoneFormat("abcdefghij") shouldBe false
    }
  }

  "Validations.isShortEnough" should {
    "return true when message is shorter than limit" in {
      isShortEnough("hello", 10) shouldBe true
    }
    "return true when message equals limit" in {
      isShortEnough("hello", 5) shouldBe true
    }
    "return false when message exceeds limit" in {
      isShortEnough("hello world", 5) shouldBe false
    }
  }
}
