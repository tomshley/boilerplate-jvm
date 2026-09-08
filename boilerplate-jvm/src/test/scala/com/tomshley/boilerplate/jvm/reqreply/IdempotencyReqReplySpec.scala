package com.tomshley.boilerplate.jvm.reqreply

import com.tomshley.boilerplate.jvm.security.tokens.{ExpiringSignedValue, Keyring, TokenProfile}
import com.typesafe.config.ConfigFactory
import org.apache.pekko.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import org.apache.pekko.cluster.typed.{Cluster, Join}
import org.scalatest.wordspec.AnyWordSpecLike

import java.time.Instant
import java.util.UUID
import scala.concurrent.Future
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
  }
}
