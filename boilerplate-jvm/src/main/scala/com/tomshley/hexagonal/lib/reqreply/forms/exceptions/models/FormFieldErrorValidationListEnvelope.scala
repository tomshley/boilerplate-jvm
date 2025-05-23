package  com.tomshley.boilerplate.lib.reqreply.forms.exceptions.models

import com.tomshley.boilerplate.lib.marshalling.models.MarshallModel
import com.tomshley.boilerplate.lib.reqreply.forms.models.NamedValidation

final case class FormFieldErrorValidationListEnvelope(errors: List[NamedValidation]) extends MarshallModel[FormFieldErrorValidationListEnvelope]
