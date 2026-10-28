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
