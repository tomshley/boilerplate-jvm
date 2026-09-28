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

/** Email sender configuration for [[TwilioMessaging]]. */
final case class TwilioEmailChannel(
    fromAddress: String,
    fromName: String,
    /** Origin the `/v1/Emails` path is posted against. An override must be
     *  `https`; plain `http` is allowed only on a loopback host, for local
     *  tests. */
    baseUrl: String = TwilioEmailChannel.DefaultBaseUrl
)

object TwilioEmailChannel:
  /** Twilio's Email API origin — overridable only to `https`, or to plain
   *  `http` on a loopback host for local tests. */
  val DefaultBaseUrl = "https://comms.twilio.com"
