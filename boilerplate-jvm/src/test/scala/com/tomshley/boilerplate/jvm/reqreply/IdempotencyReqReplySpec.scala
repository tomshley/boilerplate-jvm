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

import com.tomshley.boilerplate.jvm.security.tokens.{ExpiringSignedValue, Keyring, TokenProfile}
import com.typesafe.config.ConfigFactory
import org.apache.pekko.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import org.apache.pekko.cluster.typed.{Cluster, Join}
import org.apache.pekko.pattern.StatusReply
import org.scalatest.wordspec.AnyWordSpecLike

import java.time.Instant
import java.util.UUID
import scala.concurrent.{Future, Promise}
import scala.concurrent.duration.*

final class IdempotencyReqReplySpec
    extends ScalaTestWithActorTestKit(ConfigFactory.parseString(
      """
        |pekko.actor.provider = cluster
        |pekko.remote.artery.canonical.hostname = "127.0.0.1"
        |pekko.remote.artery.canonical.port = 0
        |pekko.cluster.jmx.enabled = off
        |""".stripMargin))
    with AnyWordSpecLike {

  private val keyring = Keyring.fromHex(Map(1 -> "00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff"))
  private val profile = TokenProfile("reqreply-spec\n", version = 1, payloadLength = 0, grace = Duration.Zero)

  private def freshRequestId(): ExpiringSignedValue = {
    val expiresAt = Instant.now().plusSeconds(60)
    val encoded = ExpiringSignedValue.sign(keyring, profile, UUID.randomUUID().toString, expiresAt)
    ExpiringSignedValue.verify(keyring, profile, encoded, Instant.now()).toOption.get
  }

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    Cluster(system).manager ! Join(Cluster(system).selfMember.address)
    Idempotent.init(system)
  }

  "Idempotency.reqReply" should {
    "run the callback once and replay the stored reply, headers included, on a repeat" in {
      val idempotency = Idempotency(system)
      val requestId = freshRequestId()
      val reply = Idempotency.RequestReply(Some(Map("h" -> "v")), Some("body"))

      idempotency.reqReply(requestId, Future.successful(reply)).futureValue shouldBe reply
      idempotency.reqReply(requestId, Future.failed(new IllegalStateException("must not run twice"))).futureValue shouldBe reply
    }

    "abandon on a failed callback and keep the caller's failure, so the next attempt is fresh" in {
      val idempotency = Idempotency(system)
      val requestId = freshRequestId()
      val reply = Idempotency.RequestReply(None, Some("body"))

      val failed = idempotency.reqReply(requestId, Future.failed(new IllegalStateException("downstream")))
      failed.failed.futureValue.getMessage shouldBe "downstream"
      idempotency.reqReply(requestId, Future.successful(reply)).futureValue shouldBe reply
    }

    "abandon on a callback that throws instead of returning a failed future" in {
      val idempotency = Idempotency(system)
      val requestId = freshRequestId()
      val reply = Idempotency.RequestReply(None, Some("body"))

      val failed = idempotency.reqReply(requestId, throw new IllegalStateException("sync"))
      failed.failed.futureValue.getMessage shouldBe "sync"
      idempotency.reqReply(requestId, Future.successful(reply)).futureValue shouldBe reply
    }

    "answer status without claiming, so a later reqReply still runs the callback" in {
      val idempotency = Idempotency(system)
      val requestId = freshRequestId()

      val status = idempotency.idempotencyStatus(requestId).futureValue
      status.replyBody shouldBe None
      status.isIdempotent shouldBe false

      val reply = Idempotency.RequestReply(None, Some("body"))
      idempotency.reqReply(requestId, Future.successful(reply)).futureValue shouldBe reply
    }

    "reflect a stored reply on status without counting it as a request" in {
      val idempotency = Idempotency(system)
      val requestId = freshRequestId()
      val reply = Idempotency.RequestReply(None, Some("body"))

      idempotency.reqReply(requestId, Future.successful(reply)).futureValue shouldBe reply

      val status = idempotency.idempotencyStatus(requestId).futureValue
      status.replyBody shouldBe Some("body")
      status.isIdempotent shouldBe false
    }

    "release an incomplete claim so a later reqReply runs fresh" in {
      val idempotency = Idempotency(system)
      val requestId = freshRequestId()

      idempotency.idempotencyResult(requestId).futureValue
      idempotency.release(requestId).futureValue.replyBody shouldBe None
      idempotency.idempotencyStatus(requestId).futureValue.replyBody shouldBe None

      val reply = Idempotency.RequestReply(None, Some("body"))
      idempotency.reqReply(requestId, Future.successful(reply)).futureValue shouldBe reply
    }

    "keep the stored reply when a completed claim is released" in {
      val idempotency = Idempotency(system)
      val requestId = freshRequestId()
      val reply = Idempotency.RequestReply(None, Some("body"))

      idempotency.reqReply(requestId, Future.successful(reply)).futureValue shouldBe reply
      idempotency.release(requestId).futureValue.replyBody shouldBe Some("body")
      idempotency.reqReply(requestId, Future.failed(new IllegalStateException("must not run twice"))).futureValue shouldBe reply
    }

    "record and replay a deliberately empty reply" in {
      val idempotency = Idempotency(system)
      val requestId = freshRequestId()
      val empty = Idempotency.RequestReply(None, None)

      idempotency.reqReply(requestId, Future.successful(empty)).futureValue shouldBe empty
      idempotency.reqReply(requestId, Future.failed(new IllegalStateException("must not run twice"))).futureValue shouldBe empty
      idempotency.idempotencyStatus(requestId).futureValue.repliedOn shouldBe defined
    }

    "return the acknowledged stored reply when another record wins first" in {
      val idempotency = Idempotency(system)
      val requestId = freshRequestId()
      val started = Promise[Unit]()
      val gate = Promise[Idempotency.RequestReply]()
      val replyProbe = createTestProbe[StatusReply[Idempotent.Summary]]()

      val result = idempotency.reqReply(requestId, {
        started.success(())
        gate.future
      })
      started.future.futureValue

      val stored = Idempotency.RequestReply(Some(Map("source" -> "stored")), Some("winner"))
      val entityRef = idempotency.idempotencyCluster.entityRefFor(Idempotent.EntityKey, requestId.value)
      entityRef ! Idempotent.SingleReply(stored.headers, stored.body, replyProbe.ref)
      replyProbe.receiveMessage().isSuccess shouldBe true

      gate.success(Idempotency.RequestReply(Some(Map("source" -> "callback")), Some("later")))

      result.futureValue shouldBe stored
      idempotency
        .reqReply(requestId, Future.failed(new IllegalStateException("must not run twice")))
        .futureValue shouldBe stored
    }
  }

  "Idempotency.reqReplyOutcome" should {

    "complete a fresh claim and replay the stored reply on a typed repeat" in {
      val idempotency = Idempotency(system)
      val requestId = freshRequestId()
      val reply = Idempotency.RequestReply(Some(Map("h" -> "v")), Some("body"))

      idempotency.reqReplyOutcome(requestId, Future.successful(reply)).futureValue shouldBe
        Idempotency.Outcome.Completed(reply)
      idempotency
        .reqReplyOutcome(requestId, Future.failed(new IllegalStateException("must not run twice")))
        .futureValue shouldBe Idempotency.Outcome.Completed(reply)
    }

    "distinguish a pending duplicate from a deliberately empty completed reply" in {
      val idempotency = Idempotency(system)
      val requestId = freshRequestId()
      val started = Promise[Unit]()
      val gate = Promise[Idempotency.RequestReply]()

      val first = idempotency.reqReplyOutcome(requestId, {
        started.success(())
        gate.future
      })
      started.future.futureValue

      idempotency
        .reqReplyOutcome(requestId, Future.failed(new IllegalStateException("must not run twice")))
        .futureValue shouldBe Idempotency.Outcome.Pending
      idempotency
        .reqReply(requestId, Future.failed(new IllegalStateException("must not run twice")))
        .futureValue shouldBe Idempotency.RequestReply(None, None)

      gate.success(Idempotency.RequestReply(None, None))
      first.futureValue shouldBe
        Idempotency.Outcome.Completed(Idempotency.RequestReply(None, None))

      idempotency
        .reqReplyOutcome(requestId, Future.failed(new IllegalStateException("must not run twice")))
        .futureValue shouldBe
        Idempotency.Outcome.Completed(Idempotency.RequestReply(None, None))
      idempotency.idempotencyStatus(requestId).futureValue.repliedOn shouldBe defined
    }

    "release the claim on a failed typed callback so the next attempt is fresh" in {
      val idempotency = Idempotency(system)
      val requestId = freshRequestId()
      val failure = new IllegalStateException("downstream")

      idempotency.reqReplyOutcome(requestId, Future.failed(failure)).failed.futureValue should
        be theSameInstanceAs failure

      val reply = Idempotency.RequestReply(None, Some("body"))
      idempotency.reqReplyOutcome(requestId, Future.successful(reply)).futureValue shouldBe
        Idempotency.Outcome.Completed(reply)
    }

    "release the claim on a typed callback that throws" in {
      val idempotency = Idempotency(system)
      val requestId = freshRequestId()
      val failure = new IllegalStateException("sync")

      idempotency.reqReplyOutcome(requestId, throw failure).failed.futureValue should
        be theSameInstanceAs failure

      val reply = Idempotency.RequestReply(None, Some("body"))
      idempotency.reqReplyOutcome(requestId, Future.successful(reply)).futureValue shouldBe
        Idempotency.Outcome.Completed(reply)
    }

    "return the acknowledged stored reply when another record wins first" in {
      val idempotency = Idempotency(system)
      val requestId = freshRequestId()
      val started = Promise[Unit]()
      val gate = Promise[Idempotency.RequestReply]()
      val replyProbe = createTestProbe[StatusReply[Idempotent.Summary]]()

      val result = idempotency.reqReplyOutcome(requestId, {
        started.success(())
        gate.future
      })
      started.future.futureValue

      val stored = Idempotency.RequestReply(Some(Map("source" -> "stored")), Some("winner"))
      val entityRef = idempotency.idempotencyCluster.entityRefFor(Idempotent.EntityKey, requestId.value)
      entityRef ! Idempotent.SingleReply(stored.headers, stored.body, replyProbe.ref)
      replyProbe.receiveMessage().isSuccess shouldBe true

      gate.success(Idempotency.RequestReply(Some(Map("source" -> "callback")), Some("later")))

      result.futureValue shouldBe Idempotency.Outcome.Completed(stored)
      idempotency
        .reqReplyOutcome(requestId, Future.failed(new IllegalStateException("must not run twice")))
        .futureValue shouldBe Idempotency.Outcome.Completed(stored)
    }
  }
}
