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

import scala.concurrent.duration.{DurationInt, FiniteDuration}

/** Credentials, enabled channels, and submission bounds. */
final case class TwilioMessagingConfig(
    accountSid: String,
    authToken: String,
    sms: Option[TwilioSmsChannel] = None,
    email: Option[TwilioEmailChannel] = None,
    /** One deadline per submission verdict on both channels; it also bounds
     *  the SDK transport's connect, pool-wait, and response-read timeouts. */
    requestTimeout: FiniteDuration = 15.seconds,
    /** Bound on the email response body decoded for verdict metadata — the
     *  SDK owns network buffering; an oversized or unreadable body degrades
     *  to status-only classification. Must fit an `Int` for the decode read. */
    responseMaxBytes: Long = 64L * 1024L
) {
  require(requestTimeout >= 1.milli, "requestTimeout must be at least 1 ms")
  require(
    responseMaxBytes > 0 && responseMaxBytes < Int.MaxValue,
    "responseMaxBytes must be positive and smaller than Int.MaxValue")

  /** The auth token is a credential — it never renders. */
  override def toString: String =
    s"TwilioMessagingConfig(accountSid=$accountSid, authToken=<redacted>, sms=$sms, email=$email, requestTimeout=$requestTimeout, responseMaxBytes=$responseMaxBytes)"
}
