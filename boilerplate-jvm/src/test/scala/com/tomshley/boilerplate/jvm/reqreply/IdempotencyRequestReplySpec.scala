package com.tomshley.boilerplate.jvm.reqreply

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

final class IdempotencyRequestReplySpec extends AnyWordSpec with Matchers {

  "Idempotency.RequestReply" should {
    "store headers and body" in {
      val rr = Idempotency.RequestReply(
        Some(Map("Content-Type" -> "application/json")),
        Some("{\"status\":\"ok\"}")
      )
      rr.headers shouldBe Some(Map("Content-Type" -> "application/json"))
      rr.body shouldBe Some("{\"status\":\"ok\"}")
    }

    "handle None values" in {
      val rr = Idempotency.RequestReply(None, None)
      rr.headers shouldBe None
      rr.body shouldBe None
    }
  }
}
