package com.tomshley.boilerplate.jvm.reqreply.forms

import com.tomshley.boilerplate.jvm.reqreply.forms.exceptions.FormFieldException
import com.tomshley.boilerplate.jvm.reqreply.forms.exceptions.models.FormFieldErrorValidationListEnvelope
import com.tomshley.boilerplate.jvm.reqreply.forms.models.GroupedRequireEnvelope

trait GroupedRequirements {
  @inline final def require(requirements: List[GroupedRequireEnvelope]): Unit = {
    val errorsEnvelope = FormFieldErrorValidationListEnvelope(requirements.filter(!_.condition).map(_.errorValidation))
    if (errorsEnvelope.errors.nonEmpty) {
      throw FormFieldException(errorsEnvelope)
    }
  }
}