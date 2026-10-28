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

import com.tomshley.boilerplate.jvm.security.tokens.ExpiringSignedValue
import org.apache.pekko.actor.typed.ActorSystem
import org.apache.pekko.cluster.sharding.typed.scaladsl.{ClusterSharding, EntityRef}
import org.apache.pekko.util.Timeout
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.Future
import scala.util.{Failure, Success}

class Idempotency(system: ActorSystem[?]) {

  import system.executionContext

  private val logger: Logger = LoggerFactory.getLogger(getClass)

  final lazy val idempotencyCluster = ClusterSharding(system)

  private given timeout: Timeout =
    Timeout.create(
      system.settings.config
        .getDuration("tomshley-boilerplate-reqreply-idempotency.ask-timeout")
    )

  private def idempotencyEntityRef(requestId: ExpiringSignedValue): EntityRef[Idempotent.Command] = {
    idempotencyCluster.entityRefFor(
      Idempotent.EntityKey,
      requestId.value
    )
  }

  /** Claims the request: first caller is fresh, every later caller is marked
    * idempotent and receives the stored reply. Mutates entity state — use only
    * where arriving IS the request being deduplicated. */
  def idempotencyResult(requestId: ExpiringSignedValue, resultEntityRef: Option[EntityRef[Idempotent.Command]] = None): Future[Idempotent.Summary] = {
    resultEntityRef.getOrElse(idempotencyEntityRef(requestId)).askWithStatus(
      Idempotent.SingleRequest(Option.empty, Option.empty, _)
    )
  }

  /** Read-only summary lookup — observes state without claiming the key or
    * mutating stored data. */
  def idempotencyStatus(requestId: ExpiringSignedValue, resultEntityRef: Option[EntityRef[Idempotent.Command]] = None): Future[Idempotent.Summary] = {
    resultEntityRef.getOrElse(idempotencyEntityRef(requestId)).askWithStatus(
      Idempotent.StatusRequest(_)
    )
  }

  /** Explicitly releases an incomplete claim, leaving completed replies intact.
    * Reconcile external effects and quiesce the previous execution and its
    * outstanding messages first: this is not cancellation or a fenced takeover.
    * A failed acknowledgement is uncertain, not proof that release did not occur.
    */
  def release(requestId: ExpiringSignedValue): Future[Idempotent.Summary] =
    idempotencyEntityRef(requestId).askWithStatus[Idempotent.Summary](
      Idempotent.AbandonRequest(_)
    )

  /** Compatibility projection of [[reqReplyOutcome]]. Pending maps to an empty
    * RequestReply, which is indistinguishable here from a completed empty reply.
    * Use reqReplyOutcome when completion must be explicit. Failures remain failed
    * futures; the callback is evaluated only for a fresh claim.
    */
  def reqReply(
      requestId: ExpiringSignedValue,
      responseBodyCallback: => Future[Idempotency.RequestReply]
  ): Future[Idempotency.RequestReply] =
    reqReplyOutcome(requestId, responseBodyCallback).map {
      case Idempotency.Outcome.Pending => Idempotency.RequestReply(None, None)
      case Idempotency.Outcome.Completed(reply) => reply
    }

  /** Claims the key and returns Pending for an incomplete duplicate, or Completed
    * with the stored response, including a deliberately empty response. A fresh
    * claim evaluates the callback once and waits for the recorded reply's
    * acknowledgement. Callback failure attempts release; unconfirmed recording
    * fails without release because the write or external effect may have happened.
    * Pending does not establish that the original execution is still alive.
    */
  def reqReplyOutcome(
      requestId: ExpiringSignedValue,
      responseBodyCallback: => Future[Idempotency.RequestReply]
  ): Future[Idempotency.Outcome] = {
    val entityRef = idempotencyEntityRef(requestId)
    idempotencyResult(requestId, Some(entityRef)).flatMap { summary =>
      if (summary.isIdempotent) {
        logger.debug("Duplicate idempotency claim observed")
        Future.successful(
          summary.repliedOn match {
            case None => Idempotency.Outcome.Pending
            case Some(_) => Idempotency.Outcome.Completed(
              Idempotency.RequestReply(summary.replyHeaders, summary.replyBody))
          }
        )
      } else {
        Future.delegate(responseBodyCallback).transformWith {
          case Success(response) =>
            entityRef
              .askWithStatus[Idempotent.Summary](Idempotent.SingleReply(response.headers, response.body, _))
              .transform {
                case Success(recorded) =>
                  Success(Idempotency.Outcome.Completed(
                    Idempotency.RequestReply(recorded.replyHeaders, recorded.replyBody)))
                case Failure(failure) =>
                  logger.warn("Idempotency reply recording was not confirmed; reconcile the outcome before recovery", failure)
                  Failure(failure)
              }
          case Failure(failure) =>
            release(requestId).transformWith {
              case Success(_) => Future.failed(failure)
              case Failure(releaseFailure) =>
                logger.warn("Idempotency claim release was not confirmed; explicit recovery may be required", releaseFailure)
                Future.failed(failure)
            }
        }
      }
    }
  }
}

object Idempotency {
  def apply(system: ActorSystem[?]) = new Idempotency(system)

  final case class RequestReply(headers: Option[Map[String, String]],
                                body: Option[String])

  enum Outcome {
    case Pending
    case Completed(reply: RequestReply)
  }
}
