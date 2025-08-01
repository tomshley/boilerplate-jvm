package com.tomshley.boilerplate.jvm.reqreply.models

trait IdempotentView {
  lazy val requestId: ExpiringValue = ExpiringValue()
  lazy val requestIdHmac: String = requestId.toBase64Hmac
}
