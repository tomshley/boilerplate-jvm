package com.tomshley.boilerplate.jvm.durablebufferedflush

import com.tomshley.boilerplate.jvm.durablebufferedflush.CloseValidationFailure
import com.tomshley.boilerplate.jvm.durablebufferedflush.CloseValidationFailure.Classification
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

final class CloseValidationFailureSpec extends AnyWordSpec with Matchers {

  "CloseValidationFailure" should {

    "classify stable code-prefixed recoverable resend fallbacks" in {
      val message = CloseValidationFailure.ClaimsCountMismatch(entity = 3L, expected = 4L).getMessage
      val sequenceMessage = CloseValidationFailure.SequenceMismatch(entity = 3L, expected = 4L).getMessage

      CloseValidationFailure.classify(new RuntimeException(message)) shouldBe Classification.RecoverableResend
      CloseValidationFailure.classify(new RuntimeException(sequenceMessage)) shouldBe Classification.RecoverableResend
      CloseValidationFailure.isFatal(new RuntimeException(message)) shouldBe false
    }

    "classify stable code-prefixed fatal fallbacks" in {
      val message = CloseValidationFailure.BytesMismatch(entity = 7L, expected = 8L).getMessage

      CloseValidationFailure.classify(new RuntimeException(message)) shouldBe Classification.Fatal
      CloseValidationFailure.classify(new RuntimeException(CloseValidationFailure.SessionNotOpen.getMessage)) shouldBe Classification.Fatal
      CloseValidationFailure.classify(new RuntimeException(CloseValidationFailure.RequiredSessionFieldsMissing.getMessage)) shouldBe Classification.Fatal
      CloseValidationFailure.isFatal(new RuntimeException(message)) shouldBe true
    }

    "retain legacy message compatibility for older fallbacks" in {
      CloseValidationFailure.classify(new RuntimeException("Claims count mismatch: entity=3, expected=4")) shouldBe Classification.RecoverableResend
      CloseValidationFailure.classify(new RuntimeException("Bytes mismatch: entity=7, expected=8")) shouldBe Classification.Fatal
    }

    "leave unknown messages as unknown" in {
      CloseValidationFailure.classify(new RuntimeException("completely different failure")) shouldBe Classification.Unknown
      CloseValidationFailure.classify(new RuntimeException(null: String)) shouldBe Classification.Unknown
      CloseValidationFailure.isFatal(new RuntimeException("completely different failure")) shouldBe false
    }
  }
}
