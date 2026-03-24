package com.tomshley.boilerplate.jvm.utils

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

final class TimeUtilsDeprecatedSpec extends AnyWordSpec with Matchers {
  private object UnderTest extends TimeUtils

  "TimeUtils" should {
    "resolve deprecated type aliases" in {
      val now: UnderTest.DateTime = UnderTest.DateTime.now()
      now.getYear should be > 2000
    }

    "resolve deprecated forwarders" in {
      val formatted = UnderTest.DateTimeFormat.forPattern("yyyy-MM-dd").print(UnderTest.DateTime.now())
      formatted.length shouldBe 10
    }
  }
}
