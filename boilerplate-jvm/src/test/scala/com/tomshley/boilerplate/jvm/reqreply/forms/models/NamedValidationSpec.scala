package com.tomshley.boilerplate.jvm.reqreply.forms.models

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

final class NamedValidationSpec extends AnyWordSpec with Matchers {

  "NamedValidation" should {
    "store fieldName and message" in {
      val v = NamedValidation("email", "is required")
      v.fieldName shouldBe "email"
      v.message shouldBe "is required"
    }
  }
}
