package com.tomshley.boilerplate.jvm.utils

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

final class ChecksumUtilSpec extends AnyWordSpec with Matchers {

  private object UnderTest extends ChecksumUtil

  "ChecksumUtil.toMD5" should {
    "produce a 32-char lowercase hex string" in {
      val md5 = UnderTest.toMD5("hello")
      md5.length shouldBe 32
      md5.matches("[0-9a-f]{32}") shouldBe true
    }

    "be deterministic" in {
      UnderTest.toMD5("hello") shouldBe UnderTest.toMD5("hello")
    }

    "match a known hash" in {
      UnderTest.toMD5("hello") shouldBe "5d41402abc4b2a76b9719d911017c592"
    }
  }
}
