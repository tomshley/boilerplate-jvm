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

package com.tomshley.boilerplate.jvm.twilio.util

import scala.concurrent.duration.FiniteDuration

/** The provider-level answer to one submission attempt — acceptance,
 *  permanent refusal, retryable failure, or an ambiguous outcome the
 *  caller must arbitrate. `detail` strings produced by [[TwilioMessaging]]
 *  omit provider bodies and account facts.
 */
sealed trait TwilioSendVerdict

object TwilioSendVerdict {

  /** Queued for delivery. `providerReference` carries the provider-assigned
   *  handle (a message SID, an operation id) when the API returned one —
   *  acceptance is *not* a delivery receipt. */
  final case class Accepted(providerReference: Option[String]) extends TwilioSendVerdict

  /** A refusal no retry can clear — the record is dead for this sender. */
  sealed trait Refused extends TwilioSendVerdict {
    /** Provider error token when one was supplied (e.g. `"21211"`, `"400"`). */
    def providerCode: Option[String]
    def detail: String
  }

  /** The destination itself is unusable — an invalid number, an unroutable
   *  `to` address. */
  final case class InvalidRecipient(providerCode: Option[String], detail: String) extends Refused

  /** The destination asked not to be contacted (SMS `21610` — recipient
   *  replied STOP). */
  final case class RecipientOptedOut(providerCode: Option[String], detail: String) extends Refused

  /** Credential, sender-identity, or payload-level refusal with no recipient
   *  basis — `401`/`403`/`413` and email `400`s outside the recipient path. */
  final case class Rejected(httpStatus: Int, providerCode: Option[String], detail: String) extends Refused

  /** Transient — the submission was definitely not accepted: a refusal
   *  status, a rate limit, or a failure before the request was sent.
   *  `retryAfter` is the provider's own hint when it sent one. */
  final case class Retryable(detail: String, retryAfter: Option[FiniteDuration]) extends TwilioSendVerdict

  /** The request may have reached Twilio — the deadline expired, the
   *  connection failed after submission, or an accepted response could not
   *  be read — so retrying may deliver the message twice. Whether to retry
   *  is the caller's decision, not this classifier's. */
  final case class OutcomeUnknown(detail: String) extends TwilioSendVerdict
}
