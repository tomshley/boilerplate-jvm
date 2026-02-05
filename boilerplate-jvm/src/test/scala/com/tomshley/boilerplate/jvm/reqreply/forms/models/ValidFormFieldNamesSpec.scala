package com.tomshley.boilerplate.jvm.reqreply.forms.models

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

final class ValidFormFieldNamesSpec extends AnyWordSpec with Matchers {

  "ValidFormFieldNames" should {
    "return empty Seq by default" in {
      val names = new ValidFormFieldNames {}
      names.validFields shouldBe Seq.empty
    }

    "allow override of validFields" in {
      val names = new ValidFormFieldNames {
        override def validFields: Seq[String] = Seq("email", "phone")
      }
      names.validFields should contain allOf ("email", "phone")
    }
  }
}
