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

package com.tomshley.boilerplate.jvm.twilio.util

import com.twilio.Twilio
import com.twilio.`type`.PhoneNumber
import com.twilio.rest.api.v2010.account.Message
import org.apache.pekko.Done
import org.apache.pekko.actor.typed.ActorSystem

import scala.annotation.tailrec
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.jdk.FutureConverters.*

/** Legacy SMS client using Twilio global configuration. */
@deprecated("use TwilioMessaging — per-instance client, both channels, typed TwilioSendVerdict")
object TwilioClient {
  private val configInstance: Promise[TwilioConfig] = Promise()

  private def configInstanceMaybe = configInstance.future.value.flatMap(_.toOption)

  @tailrec
  final def init(twilioConfig: TwilioConfig): TwilioConfig = {
    configInstanceMaybe match
      case Some(value) => value
      case None =>
        configInstance.trySuccess {
          twilioConfig
        }
        init(twilioConfig)
  }

  def sendMessageAsync(system: ActorSystem[?], to: String, body: String): Future[Message] = {
    given ec: ExecutionContext = system.executionContext

    configInstanceMaybe match
      case Some(config) =>
        Twilio.init(config.accountSid, config.authToken)
        Message
          .creator(
            new PhoneNumber(to),
            config.twilioFrom,
            body
          ).createAsync().asScala
      case None =>
        Future.failed(new Exception("Twilio is not initialized"))
  }

  def sendMessage(system: ActorSystem[?], to: String, body: String): Option[Message] = {
    configInstanceMaybe match
      case Some(config) =>
        Twilio.init(config.accountSid, config.authToken)
        Some(Message
          .creator(
            new PhoneNumber(to),
            config.twilioFrom,
            body
          ).create())
      case None =>
        Option.empty[Message]
  }
}


