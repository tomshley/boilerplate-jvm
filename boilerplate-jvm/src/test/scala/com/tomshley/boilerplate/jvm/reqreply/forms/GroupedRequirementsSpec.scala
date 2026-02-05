package com.tomshley.boilerplate.jvm.reqreply.forms

import com.tomshley.boilerplate.jvm.reqreply.forms.exceptions.FormFieldException
import com.tomshley.boilerplate.jvm.reqreply.forms.models.{GroupedRequireEnvelope, NamedValidation}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

final class GroupedRequirementsSpec extends AnyWordSpec with Matchers with GroupedRequirements {

  "GroupedRequirements.require" should {
    "not throw when all conditions pass" in {
      noException shouldBe thrownBy {
        require(
          List(
            GroupedRequireEnvelope(condition = true, NamedValidation("field1", "error1")),
            GroupedRequireEnvelope(condition = true, NamedValidation("field2", "error2"))
          )
        )
      }
    }

    "throw FormFieldException when one condition fails" in {
      val ex = intercept[FormFieldException] {
        require(
          List(
            GroupedRequireEnvelope(condition = true, NamedValidation("field1", "error1")),
            GroupedRequireEnvelope(condition = false, NamedValidation("field2", "error2"))
          )
        )
      }
      ex.getMessage should include("field2")
      ex.getMessage should include("error2")
    }

    "throw FormFieldException with multiple errors when multiple conditions fail" in {
      val ex = intercept[FormFieldException] {
        require(
          List(
            GroupedRequireEnvelope(condition = false, NamedValidation("field1", "error1")),
            GroupedRequireEnvelope(condition = false, NamedValidation("field2", "error2"))
          )
        )
      }
      ex.getMessage should include("field1")
      ex.getMessage should include("field2")
    }

    "not throw for empty list" in {
      noException shouldBe thrownBy {
        require(List.empty)
      }
    }
  }
}
