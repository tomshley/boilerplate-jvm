package com.tomshley.boilerplate.jvm.utils

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

final class ConfigKeyUtilSpec extends AnyWordSpec with Matchers {

  private object UnderTest extends ConfigKeyUtil

  "ConfigKeyUtil" should {
    "getValueWithDefault return None when key missing" in {
      UnderTest.getValueWithDefault[String]("definitely.missing.key", None) shouldBe None
    }

    "getValueWithDefault return the configured value when key is present" in {
      UnderTest.getValueWithDefault[String]("config-key-util-fixture.present", Some("x")) shouldBe Some("0123456789abcdef")
    }

    "getValueWithDefault return default when key is null" in {
      UnderTest.getValueWithDefault[String]("config-key-util-fixture.explicitly-null", Some("x")) shouldBe Some("x")
    }
  }
}
