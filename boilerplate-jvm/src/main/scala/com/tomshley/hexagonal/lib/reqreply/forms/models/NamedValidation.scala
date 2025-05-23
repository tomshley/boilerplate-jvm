package  com.tomshley.boilerplate.lib.reqreply.forms.models

import com.tomshley.boilerplate.lib.marshalling.models.MarshallModel

final case class NamedValidation(fieldName: String, message: String) extends MarshallModel[NamedValidation]
