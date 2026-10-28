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

package com.tomshley.boilerplate.jvm.reqreply

import org.apache.pekko.actor.testkit.typed.scaladsl.{ScalaTestWithActorTestKit, SerializationTestKit}
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

    "return the stored reply after a request has been completed" in {
      val probe = createTestProbe[StatusReply[Idempotent.Summary]]()
      val ref = spawn(Idempotent("k1"))

      ref ! Idempotent.SingleRequest(None, None, probe.ref)
      probe.receiveMessage().isSuccess shouldBe true

      ref ! Idempotent.SingleReply(None, Some("rep"), probe.ref)
      probe.receiveMessage().isSuccess shouldBe true

      ref ! Idempotent.SingleRequest(None, None, probe.ref)
      val reply = probe.receiveMessage()
      reply.isSuccess shouldBe true
      reply.getValue.isIdempotent shouldBe true
      reply.getValue.replyBody shouldBe Some("rep")
    }

    "abandon a request before a reply so the next request is fresh" in {
      val probe = createTestProbe[StatusReply[Idempotent.Summary]]()
      val ref = spawn(Idempotent("k1"))

      ref ! Idempotent.SingleRequest(None, None, probe.ref)
      probe.receiveMessage().isSuccess shouldBe true
      ref ! Idempotent.AbandonRequest(probe.ref)
      probe.receiveMessage().isSuccess shouldBe true
      ref ! Idempotent.SingleRequest(None, None, probe.ref)
      val reply = probe.receiveMessage()
      reply.isSuccess shouldBe true
      reply.getValue.isIdempotent shouldBe false
    }

    "answer repeated empty abandons without claiming, and keep a released claim fresh" in {
      val probe = createTestProbe[StatusReply[Idempotent.Summary]]()
      val ref = spawn(Idempotent("k1"))

      ref ! Idempotent.AbandonRequest(probe.ref)
      probe.receiveMessage().getValue shouldBe Idempotent.State.empty.toSummary
      ref ! Idempotent.AbandonRequest(probe.ref)
      probe.receiveMessage().getValue shouldBe Idempotent.State.empty.toSummary

      ref ! Idempotent.SingleRequest(None, None, probe.ref)
      probe.receiveMessage().getValue.isIdempotent shouldBe false

      ref ! Idempotent.AbandonRequest(probe.ref)
      probe.receiveMessage().getValue shouldBe Idempotent.State.empty.toSummary
      ref ! Idempotent.AbandonRequest(probe.ref)
      probe.receiveMessage().getValue shouldBe Idempotent.State.empty.toSummary

      ref ! Idempotent.SingleRequest(None, None, probe.ref)
      val reply = probe.receiveMessage()
      reply.isSuccess shouldBe true
      reply.getValue.isIdempotent shouldBe false
    }

    "retain a completed reply when abandonment is requested" in {
      val probe = createTestProbe[StatusReply[Idempotent.Summary]]()
      val ref = spawn(Idempotent("k1"))

      ref ! Idempotent.SingleRequest(None, None, probe.ref)
      probe.receiveMessage().isSuccess shouldBe true
      ref ! Idempotent.SingleReply(None, Some("rep"), probe.ref)
      probe.receiveMessage().isSuccess shouldBe true
      ref ! Idempotent.AbandonRequest(probe.ref)
      val reply = probe.receiveMessage()
      reply.isSuccess shouldBe true
      reply.getValue.replyBody shouldBe Some("rep")
    }

    "reject an invalid reply state (reply without request)" in {
      val probe = createTestProbe[StatusReply[Idempotent.Summary]]()
      val ref = spawn(Idempotent("k1"))

      ref ! Idempotent.SingleReply(None, Some("rep"), probe.ref)
      val reply = probe.receiveMessage()
      reply.isError shouldBe true
    }

    "answer a status query without claiming the key or counting a request" in {
      val probe = createTestProbe[StatusReply[Idempotent.Summary]]()
      val ref = spawn(Idempotent("k1"))

      ref ! Idempotent.StatusRequest(probe.ref)
      val first = probe.receiveMessage()
      first.isSuccess shouldBe true
      first.getValue.isIdempotent shouldBe false
      first.getValue.replyBody shouldBe None

      ref ! Idempotent.StatusRequest(probe.ref)
      probe.receiveMessage().getValue.isIdempotent shouldBe false

      ref ! Idempotent.SingleRequest(None, None, probe.ref)
      val claimed = probe.receiveMessage()
      claimed.isSuccess shouldBe true
      claimed.getValue.isIdempotent shouldBe false
    }

    "report a stored reply on a status query without counting it" in {
      val probe = createTestProbe[StatusReply[Idempotent.Summary]]()
      val ref = spawn(Idempotent("k1"))

      ref ! Idempotent.SingleRequest(None, None, probe.ref)
      probe.receiveMessage().isSuccess shouldBe true
      ref ! Idempotent.SingleReply(None, Some("rep"), probe.ref)
      probe.receiveMessage().isSuccess shouldBe true

      ref ! Idempotent.StatusRequest(probe.ref)
      val status = probe.receiveMessage()
      status.isSuccess shouldBe true
      status.getValue.replyBody shouldBe Some("rep")
      status.getValue.isIdempotent shouldBe false
    }

    "serialize every command through a consumer-declared jackson-cbor binding" in {
      val probe = createTestProbe[StatusReply[Idempotent.Summary]]()
      val serializationTestKit = SerializationTestKit(system)

      serializationTestKit.verifySerialization(Idempotent.SingleRequest(Some(Map("h" -> "v")), Some("req"), probe.ref))
      serializationTestKit.verifySerialization(Idempotent.SingleReply(None, Some("rep"), probe.ref))
      serializationTestKit.verifySerialization(Idempotent.AbandonRequest(probe.ref))
      serializationTestKit.verifySerialization(Idempotent.StatusRequest(probe.ref))
      serializationTestKit.verifySerialization(Idempotent.State.empty.toSummary)
    }
  }
}
