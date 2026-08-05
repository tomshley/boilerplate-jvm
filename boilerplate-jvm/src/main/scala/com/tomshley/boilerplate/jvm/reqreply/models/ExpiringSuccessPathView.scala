package com.tomshley.boilerplate.jvm.reqreply.models

import com.tomshley.boilerplate.jvm.reqreply.SignedValueDirectives

import scala.concurrent.duration.*
trait ExpiringSuccessPathView {
  def successValue: Option[String]
  lazy val successPathHmac: String = SignedValueDirectives.signValue(successValue.getOrElse(""), 1.day)
}
