package com.tomshley.boilerplate.jvm.reqreply.forms.models

import com.tomshley.boilerplate.jvm.marshalling.models.MarshallModel

final case class NamedValidation(fieldName: String, message: String) extends MarshallModel[NamedValidation]
