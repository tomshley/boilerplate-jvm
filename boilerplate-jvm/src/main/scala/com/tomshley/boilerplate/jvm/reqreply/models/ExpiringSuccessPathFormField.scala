package com.tomshley.boilerplate.jvm.reqreply.models

trait ExpiringSuccessPathFormField {
  lazy val expiringSuccessPathMaybe: Option[ExpiringValue] = {
    ExpiringValue.fromBase64Hmac(successPathHmacString)
  }
  val successPathHmacString: String
}
