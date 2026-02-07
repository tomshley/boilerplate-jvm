package com.tomshley.boilerplate.jvm.claimcheck

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import java.time.Instant

final class ClaimCheckBoilerplateSpec extends AnyWordSpec with Matchers {

  object TestBoilerplate extends ClaimCheckBoilerplate

  // Key conversion: String -> Int
  given stringToInt: (String => Int) = _.toInt

  private def ticket(key: Int): ClaimTicket =
    ClaimTicket(
      number = s"ticket-$key",
      location = s"s3://bucket/key-$key",
      storeType = "test",
      sizeBytes = Some(100L)
    )

  private def applyEvent(
      state: TestClaimCheckState,
      event: ClaimCheckLifecycleEvent
  ): TestClaimCheckState = {
    TestBoilerplate.applyClaimCheckEvent[Int, TestClaimCheckState](state, event)
  }

  "ClaimCheckBoilerplate" should {
    "apply ItemReceived — add key to pending" in {
      val s0 = TestClaimCheckState()
      val event = ClaimCheckLifecycleEvent.ItemReceived("1", 100L, 12345L, Instant.now())
      val s1 = applyEvent(s0, event)

      s1.pendingKeys shouldBe Set(1)
      s1.claims shouldBe empty
    }

    "apply ItemChecked — move from pending to claims" in {
      val s0 = TestClaimCheckState(pendingKeys = Set(1))
      val t = ticket(1)
      val event = ClaimCheckLifecycleEvent.ItemChecked("1", t, Instant.now())
      val s1 = applyEvent(s0, event)

      s1.pendingKeys shouldBe empty
      s1.claims shouldBe Map(1 -> t)
    }

    "apply ItemClaimed — no state change by default" in {
      val t = ticket(1)
      val s0 = TestClaimCheckState(claims = Map(1 -> t))
      val event = ClaimCheckLifecycleEvent.ItemClaimed("1", Instant.now())
      val s1 = applyEvent(s0, event)

      s1.pendingKeys shouldBe empty
      s1.claims shouldBe Map(1 -> t)
    }

    "apply ItemDiscarded — remove from pending and claims" in {
      val t = ticket(1)
      val s0 = TestClaimCheckState(pendingKeys = Set(1, 2), claims = Map(1 -> t))
      val event = ClaimCheckLifecycleEvent.ItemDiscarded("1", Instant.now())
      val s1 = applyEvent(s0, event)

      s1.pendingKeys shouldBe Set(2)
      s1.claims shouldBe empty
    }

    "factory methods produce correct events" in {
      val received = TestBoilerplate.itemReceivedEvent("1", 100L, 12345L)
      received.key shouldBe "1"
      received.sizeBytes shouldBe 100L
      received.crc32 shouldBe 12345L

      val t = ticket(1)
      val checked = TestBoilerplate.itemCheckedEvent("1", t)
      checked.key shouldBe "1"
      checked.claimTicket shouldBe t

      val claimed = TestBoilerplate.itemClaimedEvent("1")
      claimed.key shouldBe "1"

      val discarded = TestBoilerplate.itemDiscardedEvent("1")
      discarded.key shouldBe "1"
    }
  }
}
