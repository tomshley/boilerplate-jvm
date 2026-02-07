package com.tomshley.boilerplate.jvm.reqreply

import org.apache.pekko.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import org.apache.pekko.pattern.StatusReply
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

final class IdempotentBehaviorSpec
    extends ScalaTestWithActorTestKit
    with AnyWordSpecLike
    with Matchers {

  "Idempotent" should {
    "accept a first request and mark it as non-idempotent" in {
      val probe = createTestProbe[StatusReply[Idempotent.Summary]]()
      val ref = spawn(Idempotent("k1"))

      ref ! Idempotent.SingleRequest(Some(Map("h" -> "v")), Some("req"), probe.ref)

      val reply = probe.receiveMessage()
      reply.isSuccess shouldBe true
      val summary = reply.getValue
      summary.idempotencyKey shouldBe "k1"
      summary.isIdempotent shouldBe false
      summary.replyBody shouldBe None
    }

    "return idempotent on repeated request" in {
      val probe = createTestProbe[StatusReply[Idempotent.Summary]]()
      val ref = spawn(Idempotent("k1"))

      ref ! Idempotent.SingleRequest(None, Some("req"), probe.ref)
      probe.receiveMessage().isSuccess shouldBe true

      ref ! Idempotent.SingleRequest(None, Some("req"), probe.ref)
      val reply2 = probe.receiveMessage()
      reply2.isSuccess shouldBe true
      reply2.getValue.isIdempotent shouldBe true
    }

    "store a reply and return idempotent on repeated reply" in {
      val probe = createTestProbe[StatusReply[Idempotent.Summary]]()
      val ref = spawn(Idempotent("k1"))

      ref ! Idempotent.SingleRequest(None, None, probe.ref)
      probe.receiveMessage().isSuccess shouldBe true

      ref ! Idempotent.SingleReply(None, Some("rep"), probe.ref)
      val reply1 = probe.receiveMessage()
      reply1.isSuccess shouldBe true
      reply1.getValue.replyBody shouldBe Some("rep")
      reply1.getValue.isIdempotent shouldBe false

      ref ! Idempotent.SingleReply(None, Some("rep"), probe.ref)
      val reply2 = probe.receiveMessage()
      reply2.isSuccess shouldBe true
      reply2.getValue.isIdempotent shouldBe true
      reply2.getValue.replyBody shouldBe Some("rep")
    }

    "reject a request after a reply has been observed" in {
      val probe = createTestProbe[StatusReply[Idempotent.Summary]]()
      val ref = spawn(Idempotent("k1"))

      ref ! Idempotent.SingleRequest(None, None, probe.ref)
      probe.receiveMessage().isSuccess shouldBe true

      ref ! Idempotent.SingleReply(None, Some("rep"), probe.ref)
      probe.receiveMessage().isSuccess shouldBe true

      ref ! Idempotent.SingleRequest(None, None, probe.ref)
      val reply = probe.receiveMessage()
      reply.isError shouldBe true
    }

    "reject an invalid reply state (reply without request)" in {
      val probe = createTestProbe[StatusReply[Idempotent.Summary]]()
      val ref = spawn(Idempotent("k1"))

      ref ! Idempotent.SingleReply(None, Some("rep"), probe.ref)
      val reply = probe.receiveMessage()
      reply.isError shouldBe true
    }
  }
}
