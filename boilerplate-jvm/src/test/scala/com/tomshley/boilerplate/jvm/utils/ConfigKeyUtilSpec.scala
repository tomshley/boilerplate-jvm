package com.tomshley.boilerplate.jvm.utils

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

final class ConfigKeyUtilSpec extends AnyWordSpec with Matchers {

  private object UnderTest extends ConfigKeyUtil

  "ConfigKeyUtil" should {
    "getValueWithDefault return None when key missing" in {
      UnderTest.getValueWithDefault[String]("definitely.missing.key", None) shouldBe None
    }

    "getValueWithDefault return default when key is null" in {
      UnderTest.getValueWithDefault[String]("tomshley-boilerplate-reqreply-idempotency.insecure-salt", Some("x")) shouldBe Some("0123456789abcdef")
    }
  }
}
