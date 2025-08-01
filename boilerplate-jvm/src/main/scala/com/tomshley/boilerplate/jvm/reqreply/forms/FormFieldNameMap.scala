package com.tomshley.boilerplate.jvm.reqreply.forms

import com.tomshley.boilerplate.jvm.reqreply.models.ViewModel

trait FormFieldNameMap extends ViewModel {
  def toMap: Map[String, String]
}

