package com.tomshley.boilerplate.lib.reqreply.forms

import com.tomshley.boilerplate.lib.reqreply.models.ViewModel

trait FormFieldNameMap extends ViewModel {
  def toMap: Map[String, String]
}

