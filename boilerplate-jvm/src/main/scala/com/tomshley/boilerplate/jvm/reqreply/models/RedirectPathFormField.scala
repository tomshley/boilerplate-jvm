package com.tomshley.boilerplate.jvm.reqreply.models

trait RedirectPathFormField {
  lazy val expiringFormFieldRedirectPathMaybe: Option[ExpiringValue] = {
    ExpiringValue.fromBase64Hmac(redirectPathFormFieldHmacString)
  }
  val redirectPathFormFieldHmacString: String
}
