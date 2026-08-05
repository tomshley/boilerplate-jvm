package com.tomshley.boilerplate.jvm.reqreply.models

import com.tomshley.boilerplate.jvm.reqreply.SignedValueDirectives

import java.util.UUID
import scala.concurrent.duration.*

trait IdempotentView {
  lazy val requestIdHmac: String = SignedValueDirectives.signValue(UUID.randomUUID().toString, 5.minutes)
}
