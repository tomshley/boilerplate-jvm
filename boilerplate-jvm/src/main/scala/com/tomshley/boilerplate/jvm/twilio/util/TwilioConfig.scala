package com.tomshley.boilerplate.jvm.twilio.util

import com.twilio.`type`.PhoneNumber

case class TwilioConfig(accountSid:String, authToken:String, from:String) {
  val twilioFrom = PhoneNumber(from)
}
