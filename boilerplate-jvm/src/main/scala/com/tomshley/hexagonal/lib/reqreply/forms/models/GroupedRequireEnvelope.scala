package  com.tomshley.boilerplate.lib.reqreply.forms.models

import com.tomshley.boilerplate.lib.marshalling.models.MarshallModel

final case class GroupedRequireEnvelope(condition: Boolean, errorValidation: NamedValidation) extends MarshallModel[GroupedRequireEnvelope]

