package com.tomshley.boilerplate.jvm.reqreply.forms.exceptions

import com.tomshley.boilerplate.jvm.reqreply.forms.exceptions.models.FormFieldErrorValidationListEnvelope
import com.tomshley.boilerplate.jvm.reqreply.forms.models.NamedValidation
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

final class FormFieldExceptionSpec extends AnyWordSpec with Matchers {

  "FormFieldException" should {
    "serialize error envelope to message" in {
      val envelope = FormFieldErrorValidationListEnvelope(
        List(
          NamedValidation("email", "Invalid email format")
        )
      )
      val ex = FormFieldException(envelope)
      ex.getMessage should include("email")
      ex.getMessage should include("Invalid email format")
    }

    "handle multiple errors in envelope" in {
      val envelope = FormFieldErrorValidationListEnvelope(
        List(
          NamedValidation("email", "Required"),
          NamedValidation("phone", "Invalid format")
        )
      )
      val ex = FormFieldException(envelope)
      ex.getMessage should include("email")
      ex.getMessage should include("phone")
    }

    "handle empty error list" in {
      val envelope = FormFieldErrorValidationListEnvelope(List.empty)
      val ex = FormFieldException(envelope)
      ex.getMessage shouldBe a[String]
      ex.getMessage should not be null
    }
  }
}
