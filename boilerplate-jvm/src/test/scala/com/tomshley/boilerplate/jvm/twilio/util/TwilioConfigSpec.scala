/*
 * copyright 2017–present tomshley llc
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
