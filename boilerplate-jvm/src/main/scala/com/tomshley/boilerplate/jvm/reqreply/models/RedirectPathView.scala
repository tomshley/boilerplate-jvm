package com.tomshley.boilerplate.jvm.reqreply.models

import com.tomshley.boilerplate.jvm.reqreply.SignedValueDirectives

import scala.concurrent.duration.*
trait RedirectPathView {
  lazy val redirectPathHmac: String = {
    SignedValueDirectives.signValue(redirectPath.getOrElse(""), (360 * 10).days)
  }
  def redirectPath:Option[String]
}
