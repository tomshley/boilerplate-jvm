package com.tomshley.boilerplate.jvm.reqreply.forms.exceptions

import com.tomshley.boilerplate.jvm.marshalling.JsonMarshaller
import com.tomshley.boilerplate.jvm.reqreply.forms.exceptions.models.FormFieldErrorValidationListEnvelope

final case class FormFieldException(formFieldErrorValidationListEnvelope: FormFieldErrorValidationListEnvelope,
                                    cause: Throwable = None.orNull)
  extends IllegalArgumentException(JsonMarshaller.serializeWithDefaults[FormFieldErrorValidationListEnvelope](formFieldErrorValidationListEnvelope), cause)