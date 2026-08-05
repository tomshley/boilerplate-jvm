package com.tomshley.boilerplate.jvm.reqreply

import com.tomshley.boilerplate.jvm.reqreply.exceptions.UnknownRejection
import com.tomshley.boilerplate.jvm.security.tokens.ExpiringSignedValue
import org.apache.pekko.http.scaladsl.server.{Directive, Directive1}
import org.apache.pekko.http.scaladsl.server.Directives.*

import scala.concurrent.Future
import scala.util.{Failure, Success}

trait IdempotencyDirectives {

  def getRequestId(idempotency: Idempotency, expiringValue: ExpiringSignedValue):Directive1[Idempotent.Summary] = {
      onComplete(idempotency.idempotencyResult(expiringValue)) flatMap {
        case Success(summary: Idempotent.Summary) =>
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
      case Success(requestReply: Idempotency.RequestReply) =>
        requestReply.body match
          case Some(_) => provide(requestReply)
          case None => reject(UnknownRejection("Unknown error occurred"))
      case scala.util.Failure(exception: Exception) =>
        reject(UnknownRejection(exception.getMessage))
      case scala.util.Failure(_) =>
        reject(UnknownRejection("Unknown error occurred"))
    }
  }
}