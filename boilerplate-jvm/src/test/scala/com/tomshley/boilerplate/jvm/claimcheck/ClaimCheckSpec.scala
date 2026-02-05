package com.tomshley.boilerplate.jvm.claimcheck

import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.ExecutionContext

final class ClaimCheckSpec extends AnyWordSpec with Matchers with ScalaFutures {

  given ExecutionContext = ExecutionContext.global

  "ClaimCheck" should {
    "check returns a ticket with correct fields" in {
      val enricher = new InMemoryContentEnricher()
      val claimCheck = ClaimCheck(enricher, location = "bucket-a")

      val ticket = claimCheck.check("hello".getBytes("UTF-8"), tag = "tag-1").futureValue
      ticket.number shouldBe "tag-1"
      ticket.location shouldBe "bucket-a"
      ticket.storeType shouldBe "in-memory"
      ticket.sizeBytes shouldBe Some(5L)
    }

    "claim retrieves original data" in {
      val enricher = new InMemoryContentEnricher()
      val claimCheck = ClaimCheck(enricher, location = "bucket-a")

      val payload = "hello".getBytes("UTF-8")
      val ticket = claimCheck.check(payload, tag = "tag-1").futureValue

      val claimed = claimCheck.claim(ticket).futureValue
      claimed.isDefined shouldBe true
      new String(claimed.get, "UTF-8") shouldBe "hello"
    }

    "claim returns None for unknown ticket" in {
      val enricher = new InMemoryContentEnricher()
      val claimCheck = ClaimCheck(enricher, location = "bucket-a")

      val ticket: ClaimTicket = SimpleClaimTicket(number = "missing", location = "bucket-a", storeType = enricher.storeType)
      claimCheck.claim(ticket).futureValue shouldBe None
    }

    "discard removes data" in {
      val enricher = new InMemoryContentEnricher()
      val claimCheck = ClaimCheck(enricher, location = "bucket-a")

      val payload = "hello".getBytes("UTF-8")
      val ticket = claimCheck.check(payload, tag = "tag-1").futureValue

      claimCheck.discard(ticket).futureValue shouldBe true
      claimCheck.claim(ticket).futureValue shouldBe None
    }

    "exists returns true for checked data and false after discard" in {
      val enricher = new InMemoryContentEnricher()
      val claimCheck = ClaimCheck(enricher, location = "bucket-a")

      val payload = "hello".getBytes("UTF-8")
      val ticket = claimCheck.check(payload, tag = "tag-1").futureValue

      claimCheck.exists(ticket).futureValue shouldBe true
      claimCheck.discard(ticket).futureValue shouldBe true
      claimCheck.exists(ticket).futureValue shouldBe false
    }
  }
}
