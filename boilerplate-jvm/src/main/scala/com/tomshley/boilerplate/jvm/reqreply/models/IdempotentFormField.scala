package com.tomshley.boilerplate.jvm.reqreply.models

trait IdempotentFormField {
  lazy val requestIdExpiringValueMaybe: Option[ExpiringValue] = {
    ExpiringValue.fromBase64Hmac(requestIdHmacString)
  }
  val requestIdHmacString: String
}
