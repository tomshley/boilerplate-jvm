package com.tomshley.boilerplate.jvm.reqreply.models

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import java.time.Instant
import java.util.UUID
import scala.concurrent.duration.*

final class ExpiringValueSpec extends AnyWordSpec with Matchers {

  "ExpiringValue" should {
    "create with a default expiration roughly 5 minutes in the future" in {
      val before = Instant.now()
      val ev = ExpiringValue()
      val after = Instant.now()

      ev.expiration.isAfter(before.plusSeconds(4 * 60)) shouldBe true
      ev.expiration.isBefore(after.plusSeconds(6 * 60)) shouldBe true
    }

    "round-trip through toBase64Hmac/fromBase64Hmac with a value" in {
      val ev = ExpiringValue(expirationDuration = Some(1.minute), value = Some("hello"))
      val encoded = ev.toBase64Hmac

      encoded.trim shouldBe encoded
      encoded.nonEmpty shouldBe true

      val decoded = ExpiringValue.fromBase64Hmac(encoded)
      decoded.isDefined shouldBe true

      val decodedEv = decoded.get
      decodedEv.uuid shouldBe ev.uuid
      decodedEv.expiration shouldBe ev.expiration
      decodedEv.value shouldBe ev.value
    }

    "round-trip through toBase64Hmac/fromBase64Hmac without a value" in {
      val ev = ExpiringValue(expirationDuration = Some(1.minute), value = None)
      val encoded = ev.toBase64Hmac

      val decoded = ExpiringValue.fromBase64Hmac(encoded)
      decoded.isDefined shouldBe true
      decoded.get.value shouldBe None
    }

    "return None from fromBase64Hmac for invalid input" in {
      ExpiringValue.fromBase64Hmac("not-a-valid-hmac") shouldBe None
    }

    "report validity and expiry correctly" in {
      val future = ExpiringValue(UUID.randomUUID(), Instant.now().plusSeconds(60), None)
      future.isExpired shouldBe false
      future.isValid shouldBe true

      val past = ExpiringValue(UUID.randomUUID(), Instant.now().minusSeconds(60), None)
      past.isExpired shouldBe true
      past.isValid shouldBe false
    }
  }
}
