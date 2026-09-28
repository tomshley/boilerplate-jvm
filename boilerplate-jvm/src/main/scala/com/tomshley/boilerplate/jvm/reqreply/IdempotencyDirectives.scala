package com.tomshley.boilerplate.jvm.reqreply

import com.tomshley.boilerplate.jvm.reqreply.exceptions.UnknownRejection
import com.tomshley.boilerplate.jvm.security.tokens.ExpiringSignedValue
import org.apache.pekko.http.scaladsl.server.Directive1
import org.apache.pekko.http.scaladsl.server.Directives.*

import scala.concurrent.Future
import scala.util.{Failure, Success}

trait IdempotencyDirectives {

  /** Read-only result lookup — observes stored state without claiming the
    * key or mutating it. */
  def getRequestId(idempotency: Idempotency, expiringValue: ExpiringSignedValue): Directive1[Idempotent.Summary] = {
    onComplete(idempotency.idempotencyStatus(expiringValue)) flatMap {
      case Success(summary) =>
        summary.replyBody match
          case Some(_) => provide(summary)
          case None => reject(UnknownRejection("Unknown error occurred"))
      case Failure(_) =>
        reject(UnknownRejection("Unknown error occurred"))
    }
  }

  def idempotentRequestReply(idempotency: Idempotency, expiringValue: ExpiringSignedValue, responseBodyCallback: => Future[Idempotency.RequestReply]): Directive1[Idempotency.RequestReply] = {
    onComplete(idempotency.reqReply(
      expiringValue,
      responseBodyCallback
    )) flatMap {
      case Success(requestReply) =>
        requestReply.body match
          case Some(_) => provide(requestReply)
          case None => reject(UnknownRejection("Unknown error occurred"))
      case Failure(_) =>
        reject(UnknownRejection("Unknown error occurred"))
    }
  }
}
