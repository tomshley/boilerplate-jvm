/*
 * copyright 2017–present tomshley llc
 *
 * licensed under the apache license, version 2.0 (the "license");
 * you may not use this file except in compliance with the license.
 * you may obtain a copy of the license at
 *
 * http://www.apache.org/licenses/license-2.0
 *
 * unless required by applicable law or agreed to in writing, software
 * distributed under the license is distributed on an "as is" basis,
 * without warranties or conditions of any kind, either express or implied.
 * see the license for the specific language governing permissions and
 * limitations under the license.
 *
 * @author thomas schena @sgoggles <https://github.com/sgoggles> | <https://gitlab.com/sgoggles>
 *
 */

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
