package com.tomshley.boilerplate.jvm.reqreply.models

import com.tomshley.boilerplate.jvm.reqreply.SignedValueDirectives
import com.tomshley.boilerplate.jvm.security.tokens.ExpiringSignedValue

trait IdempotentFormField {
  lazy val requestIdSignedValueMaybe: Option[ExpiringSignedValue] = {
    SignedValueDirectives.verifiedValue(requestIdHmacString)
  }
  val requestIdHmacString: String
}
