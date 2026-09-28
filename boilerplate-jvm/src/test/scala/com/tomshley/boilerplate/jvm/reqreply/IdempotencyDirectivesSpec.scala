/*
 * copyright 2023 tomshley llc
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

import com.tomshley.boilerplate.jvm.reqreply.exceptions.UnknownRejection
import com.tomshley.boilerplate.jvm.security.tokens.{ExpiringSignedValue, Keyring, TokenProfile}
import org.apache.pekko.actor.typed.scaladsl.adapter.*
import org.apache.pekko.cluster.sharding.typed.scaladsl.EntityRef
import org.apache.pekko.http.scaladsl.server.Directives.*
import org.apache.pekko.http.scaladsl.testkit.ScalatestRouteTest
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import java.time.Instant
import java.util.UUID
import scala.concurrent.Future
import scala.concurrent.duration.*

final class IdempotencyDirectivesSpec extends AnyWordSpec with Matchers with ScalatestRouteTest {

  private object Fixture extends IdempotencyDirectives

  private val keyring = Keyring.fromHex(Map(1 -> "00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff"))
  private val profile =
    TokenProfile("idempotency-directives-spec\n", version = 1, payloadLength = 0, grace = Duration.Zero)

  private def requestId: ExpiringSignedValue = {
    val encoded = ExpiringSignedValue.sign(
      keyring, profile, UUID.randomUUID().toString, Instant.now().plusSeconds(60))
    ExpiringSignedValue.verify(keyring, profile, encoded, Instant.now()).toOption.get
  }

  /** The directives consume only the three members below, so the stub fails
    * the moment a read path claims a key — no cluster or sharding needed. */
  private final class StubIdempotency(
      status: => Future[Idempotent.Summary],
      result: => Future[Idempotent.Summary],
      reply: => Future[Idempotency.RequestReply]
  ) extends Idempotency(system.toTyped) {
    override def idempotencyStatus(
        requestId: ExpiringSignedValue,
        resultEntityRef: Option[EntityRef[Idempotent.Command]] = None
    ): Future[Idempotent.Summary] = status

    override def idempotencyResult(
        requestId: ExpiringSignedValue,
        resultEntityRef: Option[EntityRef[Idempotent.Command]] = None
    ): Future[Idempotent.Summary] = result

    override def reqReply(
        requestId: ExpiringSignedValue,
        responseBodyCallback: => Future[Idempotency.RequestReply]
    ): Future[Idempotency.RequestReply] = reply
  }

  private def mustNotClaim: Future[Nothing] =
    Future.failed(new AssertionError("a read path must not claim the request"))

  private def summary(body: Option[String]): Idempotent.Summary =
    Idempotent.Summary(
      idempotencyKey = "key-1",
      isIdempotent = false,
      repliedOn = Some(Instant.now()),
      replyHeaders = None,
      replyBody = body
    )

  "IdempotencyDirectives.getRequestId" should {

    "provide a stored reply through the read-only status lookup" in {
      val store = new StubIdempotency(
        status = Future.successful(summary(Some("stored-body"))),
        result = mustNotClaim,
        reply = mustNotClaim
      )
      val route = Fixture.getRequestId(store, requestId) { found =>
        complete(found.replyBody.getOrElse(""))
      }
      Get("/") ~> route ~> check {
        responseAs[String] shouldBe "stored-body"
      }
    }

    "reject generically while no reply is stored yet" in {
      val store = new StubIdempotency(
        status = Future.successful(summary(None)),
        result = mustNotClaim,
        reply = mustNotClaim
      )
      val route = Fixture.getRequestId(store, requestId) { found =>
        complete(found.replyBody.getOrElse(""))
      }
      Get("/") ~> route ~> check {
        rejection shouldBe UnknownRejection("Unknown error occurred")
      }
    }
  }

  "IdempotencyDirectives.idempotentRequestReply" should {

    "provide the stored request reply" in {
      val stored = Idempotency.RequestReply(Some(Map("h" -> "v")), Some("fresh-body"))
      val store = new StubIdempotency(
        status = mustNotClaim,
        result = mustNotClaim,
        reply = Future.successful(stored)
      )
      val route = Fixture.idempotentRequestReply(store, requestId, mustNotClaim) { found =>
        complete(found.body.getOrElse(""))
      }
      Get("/") ~> route ~> check {
        responseAs[String] shouldBe "fresh-body"
      }
    }

    "reject generically while the reply body is absent" in {
      val pending = Idempotency.RequestReply(None, None)
      val store = new StubIdempotency(
        status = mustNotClaim,
        result = mustNotClaim,
        reply = Future.successful(pending)
      )
      val route = Fixture.idempotentRequestReply(store, requestId, mustNotClaim) { found =>
        complete(found.body.getOrElse(""))
      }
      Get("/") ~> route ~> check {
        rejection shouldBe UnknownRejection("Unknown error occurred")
      }
    }

    "reject a failed lookup without leaking its message or cause" in {
      val store = new StubIdempotency(
        status = mustNotClaim,
        result = mustNotClaim,
        reply = Future.failed(new RuntimeException("SECRET-SENTINEL"))
      )
      val route = Fixture.idempotentRequestReply(store, requestId, mustNotClaim) { found =>
        complete(found.body.getOrElse(""))
      }
      Get("/") ~> route ~> check {
        rejection match {
          case UnknownRejection(message, cause) =>
            message shouldBe "Unknown error occurred"
            cause shouldBe None
          case other => fail(s"unexpected rejection: $other")
        }
      }
    }
  }
}
