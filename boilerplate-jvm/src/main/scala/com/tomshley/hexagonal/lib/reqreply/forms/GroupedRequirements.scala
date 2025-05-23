package  com.tomshley.boilerplate.lib.reqreply.forms

import com.tomshley.boilerplate.lib.reqreply.forms.exceptions.FormFieldException
import com.tomshley.boilerplate.lib.reqreply.forms.exceptions.models.FormFieldErrorValidationListEnvelope
import com.tomshley.boilerplate.lib.reqreply.forms.models.GroupedRequireEnvelope

trait GroupedRequirements {
  @inline final def require(requirements: List[GroupedRequireEnvelope]): Unit = {
    val errorsEnvelope = FormFieldErrorValidationListEnvelope(requirements.filter(!_.condition).map(_.errorValidation))
    if (errorsEnvelope.errors.nonEmpty) {
      throw FormFieldException(errorsEnvelope)
    }
  }
}