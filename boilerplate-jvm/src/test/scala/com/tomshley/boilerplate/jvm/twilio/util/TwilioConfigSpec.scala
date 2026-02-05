package com.tomshley.boilerplate.jvm.twilio.util

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

final class TwilioConfigSpec extends AnyWordSpec with Matchers {

  "TwilioConfig" should {
    "store accountSid, authToken, and from" in {
      val config = TwilioConfig("AC123", "token456", "+15551234567")
      config.accountSid shouldBe "AC123"
      config.authToken shouldBe "token456"
      config.from shouldBe "+15551234567"
    }

    "create PhoneNumber from 'from' field" in {
      val config = TwilioConfig("AC123", "token456", "+15551234567")
      config.twilioFrom.toString should include("+15551234567")
    }
  }
}
