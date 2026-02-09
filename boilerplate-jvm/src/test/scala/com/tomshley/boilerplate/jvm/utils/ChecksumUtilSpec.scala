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

  "ChecksumUtil.computeCrc32" should {
    "return a non-negative Long" in {
      val crc = UnderTest.computeCrc32("hello".getBytes("UTF-8"))
      crc should be >= 0L
    }

    "be deterministic" in {
      val data = "hello".getBytes("UTF-8")
      UnderTest.computeCrc32(data) shouldBe UnderTest.computeCrc32(data)
    }

    "match a known CRC32 value" in {
      // CRC32 of "hello" is 907060870 (0x3610A686)
      UnderTest.computeCrc32("hello".getBytes("UTF-8")) shouldBe 907060870L
    }

    "handle empty input" in {
      UnderTest.computeCrc32(Array.emptyByteArray) shouldBe 0L
    }
  }
}
