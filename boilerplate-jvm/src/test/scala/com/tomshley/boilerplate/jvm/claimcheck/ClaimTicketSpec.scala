package com.tomshley.boilerplate.jvm.claimcheck

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

final class ClaimTicketSpec extends AnyWordSpec with Matchers {

  "SimpleClaimTicket" should {
    "store all fields" in {
      val ticket = SimpleClaimTicket(
        number = "n",
        location = "loc",
        storeType = "type",
        checkedAt = None,
        expiresAt = None,
        sizeBytes = Some(123L)
      )

      ticket.number shouldBe "n"
      ticket.location shouldBe "loc"
      ticket.storeType shouldBe "type"
      ticket.sizeBytes shouldBe Some(123L)
    }
  }

  "ClaimTicket" should {
    "apply creates a simple ticket" in {
      val ticket = ClaimTicket("n", "loc", "type")
      ticket.number shouldBe "n"
      ticket.location shouldBe "loc"
      ticket.storeType shouldBe "type"
    }

    "withTimestamp sets checkedAt" in {
      val ticket = ClaimTicket.withTimestamp("n", "loc", "type")
      ticket.checkedAt.isDefined shouldBe true
    }
  }
}
