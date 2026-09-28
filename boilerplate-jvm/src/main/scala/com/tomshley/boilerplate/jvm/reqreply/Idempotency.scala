package com.tomshley.boilerplate.jvm.reqreply

import com.tomshley.boilerplate.jvm.security.tokens.ExpiringSignedValue
import org.apache.pekko.actor.typed.ActorSystem
import org.apache.pekko.cluster.sharding.typed.scaladsl.{ClusterSharding, EntityRef}
import org.apache.pekko.util.Timeout
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.Future

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

  def reqReply(
                requestId: ExpiringSignedValue,
                responseBodyCallback: => Future[Idempotency.RequestReply]
              ): Future[Idempotency.RequestReply] = {
    val entityRef = idempotencyEntityRef(requestId)
    idempotencyResult(requestId, Some(entityRef))
      .flatMap(idempotentRequest =>
        if (idempotentRequest.isIdempotent) {
          logger.info(
            "idempotencyKey {} isIdempotent {};",
            idempotentRequest.idempotencyKey,
            idempotentRequest.isIdempotent
          )
          Future.successful(
            Idempotency.RequestReply(idempotentRequest.replyHeaders, idempotentRequest.replyBody)
          )
        } else {
          Future.delegate(responseBodyCallback).map { response =>
            entityRef ! Idempotent.SingleReply(response.headers, response.body, system.ignoreRef)
            response
          }.recoverWith { case failure =>
            entityRef
              .askWithStatus(Idempotent.AbandonRequest(_))
              .transformWith(_ => Future.failed(failure))
          }
        }
      )
  }
}

object Idempotency {
  def apply(system: ActorSystem[?]) = new Idempotency(system)

  final case class RequestReply(headers: Option[Map[String, String]],
                                body: Option[String])
}
