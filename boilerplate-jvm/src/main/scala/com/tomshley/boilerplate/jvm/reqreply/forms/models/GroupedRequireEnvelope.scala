package com.tomshley.boilerplate.jvm.reqreply.forms.models

import com.tomshley.boilerplate.jvm.marshalling.models.MarshallModel

final case class GroupedRequireEnvelope(condition: Boolean, errorValidation: NamedValidation) extends MarshallModel[GroupedRequireEnvelope]

