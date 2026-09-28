package com.tomshley.boilerplate.jvm.reqreply.forms

import java.nio.charset.StandardCharsets.UTF_8
import org.scalatest.concurrent.TimeLimits
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{Seconds, Span}
import org.scalatest.wordspec.AnyWordSpec

final class ValidationsSpec extends AnyWordSpec with Matchers with Validations with TimeLimits {

  "Validations.isValidEmailFormat" should {
    "return true for valid email" in {
      isValidEmailFormat("test@example.com") shouldBe true
    }
    "return true for email with subdomain" in {
      isValidEmailFormat("user@mail.example.co.uk") shouldBe true
    }
    "return true for a tagged local part" in {
      isValidEmailFormat("user+orders@example.com") shouldBe true
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
    "return false for whitespace inside the address" in {
      isValidEmailFormat("test user@example.com") shouldBe false
      isValidEmailFormat("test@exa mple.com") shouldBe false
    }
    "return false beyond the RFC 5321 254-octet ASCII bound" in {
      val local = "a" * 64 // the 64-char local-part bound — see below
      val domain = Seq("a" * 63, "b" * 63, "c" * 58, "de").mkString(".")
      isValidEmailFormat(s"$local@$domain") shouldBe true // 254 — at the bound
      val over = Seq("a" * 63, "b" * 63, "c" * 59, "de").mkString(".")
      isValidEmailFormat(s"$local@$over") shouldBe false // 255 — over
    }
    "return false beyond the 64-octet ASCII local-part bound" in {
      isValidEmailFormat(s"${"a" * 65}@b.cd") shouldBe false
    }
    "apply the 254 bound in UTF-8 octets, not characters" in {
      val address = ("é" * 32) + "@" + Seq("a" * 63, "b" * 63, "c" * 58, "de").mkString(".")
      address.getBytes(UTF_8).length shouldBe 254
      isValidEmailFormat(address) shouldBe true
      val over = ("é" * 32) + "@" + Seq("a" * 63, "b" * 63, "c" * 58, "ded").mkString(".")
      over.getBytes(UTF_8).length shouldBe 255
      isValidEmailFormat(over) shouldBe false
    }
    "apply the 64 local-part bound in UTF-8 octets, not characters" in {
      isValidEmailFormat(("é" * 32) + "@b.cd") shouldBe true // 64 octets
      isValidEmailFormat(("é" * 33) + "@b.cd") shouldBe false // 66 octets
    }
    "return false for null" in {
      isValidEmailFormat(null) shouldBe false
    }
    "return false for a trailing newline — the match covers the whole input" in {
      isValidEmailFormat("user@example.com\n") shouldBe false
    }
    "return false for address-list delimiters and control characters" in {
      isValidEmailFormat("a,b@c.d") shouldBe false
      isValidEmailFormat("a@b.c>") shouldBe false
      isValidEmailFormat("a\u00A0b@c.d") shouldBe false
      isValidEmailFormat("a\u0000b@c.d") shouldBe false
      isValidEmailFormat("a\uFEFFb@c.d") shouldBe false
    }
    "return true for an internationalized address" in {
      isValidEmailFormat("用户@例子.广告") shouldBe true
    }
  }

  "Validations.isValidPhoneFormat" should {
    "return true for phone with dashes" in {
      isValidPhoneFormat("123-456-7890") shouldBe true
    }
    "return true for phone with parentheses" in {
      isValidPhoneFormat("(123) 456-7890") shouldBe true
    }
    "return true for phone with country code" in {
      isValidPhoneFormat("+1 123-456-7890") shouldBe true
    }
    "return true for E.164" in {
      isValidPhoneFormat("+15555551212") shouldBe true
    }
    "return true for international formats" in {
      isValidPhoneFormat("+44 20 7946 0958") shouldBe true
    }
    "return true with an extension" in {
      isValidPhoneFormat("555-1212 x89") shouldBe true
      isValidPhoneFormat("+1 555-1212 ext.89") shouldBe true
      isValidPhoneFormat("555-1212 x 89") shouldBe true
    }
    "return false for non-ASCII-space whitespace before the extension digits" in {
      List("\u0085", "\uFEFF", "\n", "\t", "\u00A0").foreach { separator =>
        isValidPhoneFormat(s"555-1212 x${separator}89") shouldBe false
      }
    }
    "return false for null" in {
      isValidPhoneFormat(null) shouldBe false
    }
    "return true for a digits-only run at NANP length" in {
      isValidPhoneFormat("5558675309") shouldBe true
      isValidPhoneFormat("1234567890") shouldBe true
    }
    "return true for dotted and unspaced parenthesized runs" in {
      isValidPhoneFormat("123.456.7890") shouldBe true
      isValidPhoneFormat("(123)4567890") shouldBe true
    }
    "return false for a trailing newline — the match covers the whole input" in {
      isValidPhoneFormat("5558675309\n") shouldBe false
    }
    "return false when the digit floor leans on extension digits" in {
      isValidPhoneFormat("555-12 x89") shouldBe false
    }
    "return false for a trailing space — the body must end in a digit" in {
      isValidPhoneFormat("5551212 ") shouldBe false
    }
    "return false when the only separator precedes the extension" in {
      isValidPhoneFormat("5551212 x89") shouldBe false
    }
    "return false beyond the 15-digit ceiling on the formatted branch" in {
      isValidPhoneFormat("1234-5678-9012-3456-7890") shouldBe false
    }
    "return false for a structureless run below NANP length" in {
      isValidPhoneFormat("5551212") shouldBe false
      isValidPhoneFormat("555121") shouldBe false
    }
    "return false for a digits-only run beyond E.164" in {
      isValidPhoneFormat("0" * 16) shouldBe false
    }
    "return false for short number" in {
      isValidPhoneFormat("12345") shouldBe false
    }
    "return false for letters" in {
      isValidPhoneFormat("abcdefghij") shouldBe false
    }
    "return false for punctuation without digits" in {
      isValidPhoneFormat("(((((") shouldBe false
      isValidPhoneFormat("() --- .") shouldBe false
      isValidPhoneFormat("- ( ) .") shouldBe false
    }
    "return false for a formatted run below the digit floor" in {
      isValidPhoneFormat("555-12") shouldBe false
    }
    "return false when a leading paren stands in for a separator" in {
      isValidPhoneFormat("(5551212") shouldBe false
    }
    "return true at the pattern's longest accepted length" in {
      val longest = "+(1-1-1-1-1-1-1-1-1-1-1-1-1-1--1 ext. 123456"
      longest.length shouldBe phoneMaxLength
      isValidPhoneFormat(longest) shouldBe true
    }
    "return false beyond the pattern's longest accepted length" in {
      val over = "+((1-1-1-1-1-1-1-1-1-1-1-1-1-1--1 ext. 123456"
      over.length shouldBe phoneMaxLength + 1
      isValidPhoneFormat(over) shouldBe false
    }
    "return false quickly on hostile input — the length check bounds matching" in {
      failAfter(Span(1, Seconds)) {
        isValidPhoneFormat("1234567" + " " * 1_000_000) shouldBe false
      }
    }
  }

  "Validations.isNonBlank" should {
    "return true for content" in {
      isNonBlank("hello") shouldBe true
    }
    "return false for empty and whitespace-only" in {
      isNonBlank("") shouldBe false
      isNonBlank("   ") shouldBe false
    }
    "return false for null" in {
      isNonBlank(null) shouldBe false
    }
  }

  "Validations.isLengthWithin" should {
    "respect inclusive bounds" in {
      isLengthWithin("ab", 2, 4) shouldBe true
      isLengthWithin("abcd", 2, 4) shouldBe true
      isLengthWithin("a", 2, 4) shouldBe false
      isLengthWithin("abcde", 2, 4) shouldBe false
    }
    "return false for null" in {
      isLengthWithin(null, 0, 4) shouldBe false
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
    "return false for null" in {
      isShortEnough(null, 5) shouldBe false
    }
  }
}
