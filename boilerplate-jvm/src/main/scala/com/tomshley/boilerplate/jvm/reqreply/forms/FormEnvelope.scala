package com.tomshley.boilerplate.jvm.reqreply.forms

import com.tomshley.boilerplate.jvm.reqreply.models.{ExpiringSuccessPathView, IdempotentView, RedirectPathView, ViewModel}

trait FormEnvelope[T <: FormFieldNameMap] extends ViewModel with IdempotentView with ExpiringSuccessPathView with RedirectPathView {
  val getRenderPath: String = "/"
  val successPathPrefix: String = "/"
  val postPathPrefix: String = "/"
  val formFieldModelMap: Option[T] = Option.empty
  val messages: List[String] = List.empty
  val errors: List[String] = List.empty
  val fieldErrors: Map[String, List[String]] = Map.empty

  override def successValue: Option[String] =
    Some(s"$successPathPrefix$requestIdHmac")

  override def redirectPath: Option[String] =
    Some(getRenderPath)
}

