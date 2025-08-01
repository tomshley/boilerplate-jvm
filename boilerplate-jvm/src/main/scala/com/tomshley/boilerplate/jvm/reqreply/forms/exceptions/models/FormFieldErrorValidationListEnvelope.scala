package com.tomshley.boilerplate.jvm.reqreply.forms.exceptions.models

import com.tomshley.boilerplate.jvm.marshalling.models.MarshallModel
import com.tomshley.boilerplate.jvm.reqreply.forms.models.NamedValidation

final case class FormFieldErrorValidationListEnvelope(errors: List[NamedValidation]) extends MarshallModel[FormFieldErrorValidationListEnvelope]
