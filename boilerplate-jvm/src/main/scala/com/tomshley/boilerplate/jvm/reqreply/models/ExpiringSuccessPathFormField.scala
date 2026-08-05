package com.tomshley.boilerplate.jvm.reqreply.models

import com.tomshley.boilerplate.jvm.reqreply.SignedValueDirectives
import com.tomshley.boilerplate.jvm.security.tokens.ExpiringSignedValue

trait ExpiringSuccessPathFormField {
  lazy val expiringSuccessPathMaybe: Option[ExpiringSignedValue] = {
    SignedValueDirectives.verifiedValue(successPathHmacString)
  }
  val successPathHmacString: String
}
