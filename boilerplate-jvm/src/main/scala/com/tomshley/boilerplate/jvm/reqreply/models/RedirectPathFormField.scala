package com.tomshley.boilerplate.jvm.reqreply.models

import com.tomshley.boilerplate.jvm.reqreply.SignedValueDirectives
import com.tomshley.boilerplate.jvm.security.tokens.ExpiringSignedValue

trait RedirectPathFormField {
  lazy val expiringFormFieldRedirectPathMaybe: Option[ExpiringSignedValue] = {
    SignedValueDirectives.verifiedValue(redirectPathFormFieldHmacString)
  }
  val redirectPathFormFieldHmacString: String
}
