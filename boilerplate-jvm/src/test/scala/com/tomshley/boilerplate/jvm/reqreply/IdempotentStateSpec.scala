package com.tomshley.boilerplate.jvm.reqreply

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

final class IdempotentStateSpec extends AnyWordSpec with Matchers {

  "Idempotent.State" should {
    "start empty" in {
      val s = Idempotent.State.empty
      s.idempotencyKey shouldBe None
      s.requestCount shouldBe 0
      s.replyCount shouldBe 0
      s.isIdempotent shouldBe false
    }

    "increment requestCount and set request fields on newRequest" in {
      val s0 = Idempotent.State.empty
      val s1 = s0.newRequest("k1", Some(Map("h" -> "v")), Some("body"))

      s1.idempotencyKey shouldBe Some("k1")
      s1.requestCount shouldBe 1
      s1.replyCount shouldBe 0
      s1.requestHeaders shouldBe Some(Map("h" -> "v"))
      s1.requestBody shouldBe Some("body")
      s1.isIdempotent shouldBe false

      val s2 = s1.newRequest("k1", None, None)
      s2.requestCount shouldBe 2
      s2.isIdempotentRequest shouldBe true
      s2.isIdempotent shouldBe true
    }

    "increment replyCount and set reply fields on newReply" in {
      val s0 = Idempotent.State.empty.newRequest("k1", None, None)
      val s1 = s0.newReply(Some(Map("rh" -> "rv")), Some("reply"))

      s1.requestCount shouldBe 1
      s1.replyCount shouldBe 1
      s1.replyHeaders shouldBe Some(Map("rh" -> "rv"))
      s1.replyBody shouldBe Some("reply")
      s1.isIdempotent shouldBe false

      val s2 = s1.newReply(None, None)
      s2.replyCount shouldBe 2
      s2.isIdempotentReply shouldBe true
      s2.isIdempotent shouldBe true
    }

    "increment requestCount without changing stored request on existingRequest" in {
      val s0 = Idempotent.State.empty.newRequest("k1", Some(Map("h" -> "v")), Some("body"))
      val s1 = s0.existingRequest()

      s1.requestCount shouldBe 2
      s1.requestHeaders shouldBe Some(Map("h" -> "v"))
      s1.requestBody shouldBe Some("body")
    }

    "increment replyCount without changing stored reply on existingReply" in {
      val s0 = Idempotent.State.empty
        .newRequest("k1", None, None)
        .newReply(Some(Map("rh" -> "rv")), Some("reply"))

      val s1 = s0.existingReply()
      s1.replyCount shouldBe 2
      s1.replyHeaders shouldBe Some(Map("rh" -> "rv"))
      s1.replyBody shouldBe Some("reply")
    }

    "convert to Summary" in {
      val s0 = Idempotent.State.empty
        .newRequest("k1", None, Some("req"))
        .newReply(None, Some("rep"))

      val summary = s0.toSummary
      summary.idempotencyKey shouldBe "k1"
      summary.replyBody shouldBe Some("rep")
      summary.isIdempotent shouldBe false
    }
  }
}
