package com.tomshley.boilerplate.jvm.claimcheck

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import java.time.Instant

/** Test implementation of ClaimCheckState for unit testing */
case class TestClaimCheckState(
    pendingKeys: Set[Int] = Set.empty,
    claims: Map[Int, ClaimTicket] = Map.empty
) extends ClaimCheckState[Int, TestClaimCheckState] {

  override def withPendingKey(key: Int): TestClaimCheckState =
    copy(pendingKeys = pendingKeys + key)

  override def withClaim(key: Int, ticket: ClaimTicket): TestClaimCheckState =
    copy(
      pendingKeys = pendingKeys - key,
      claims = claims + (key -> ticket)
    )

  override def withDiscarded(key: Int): TestClaimCheckState =
    copy(pendingKeys = pendingKeys - key, claims = claims - key)
}

final class ClaimCheckStateSpec extends AnyWordSpec with Matchers {

  private def ticket(key: Int): ClaimTicket =
    ClaimTicket(
      number = s"ticket-$key",
      location = s"s3://bucket/key-$key",
      storeType = "test",
      sizeBytes = Some(100L)
    )

  "ClaimCheckState" should {
    "start empty" in {
      val s = TestClaimCheckState()
      s.pendingKeys shouldBe empty
      s.claims shouldBe empty
      s.checkedKeys shouldBe empty
      s.claimsCount shouldBe 0
      s.totalClaimedBytes shouldBe 0L
    }

    "track pending keys" in {
      val s0 = TestClaimCheckState()
      val s1 = s0.withPendingKey(1)
      val s2 = s1.withPendingKey(2)

      s2.pendingKeys shouldBe Set(1, 2)
      s2.isPending(1) shouldBe true
      s2.isPending(3) shouldBe false
      s2.hasClaim(1) shouldBe false
    }

    "track claims and remove from pending" in {
      val s0 = TestClaimCheckState()
        .withPendingKey(1)
        .withPendingKey(2)

      val t1 = ticket(1)
      val s1 = s0.withClaim(1, t1)

      s1.pendingKeys shouldBe Set(2)
      s1.hasClaim(1) shouldBe true
      s1.hasClaim(2) shouldBe false
      s1.checkedKeys shouldBe Set(1)
      s1.claimsCount shouldBe 1
      s1.claims(1) shouldBe t1
    }

    "compute receivedButNotStored" in {
      val s = TestClaimCheckState(
        pendingKeys = Set(1, 2, 3),
        claims = Map(1 -> ticket(1))
      )
      s.receivedButNotStored shouldBe Set(2, 3)
    }

    "compute progress" in {
      val s0 = TestClaimCheckState()
      s0.progress(10) shouldBe 0.0

      val s1 = TestClaimCheckState(claims = Map(1 -> ticket(1), 2 -> ticket(2)))
      s1.progress(4) shouldBe 0.5
      s1.progress(0) shouldBe 0.0
    }

    "compute totalClaimedBytes" in {
      val s = TestClaimCheckState(
        claims = Map(1 -> ticket(1), 2 -> ticket(2))
      )
      s.totalClaimedBytes shouldBe 200L
    }

    "check allChecked" in {
      val s = TestClaimCheckState(
        claims = Map(1 -> ticket(1), 2 -> ticket(2), 3 -> ticket(3))
      )
      s.allChecked(Set(1, 2)) shouldBe true
      s.allChecked(Set(1, 2, 3)) shouldBe true
      s.allChecked(Set(1, 4)) shouldBe false
    }
  }
}
