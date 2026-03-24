package com.tomshley.boilerplate.jvm.twilio.util

import com.twilio.Twilio
import com.twilio.`type`.PhoneNumber
import com.twilio.rest.api.v2010.account.Message
import org.apache.pekko.Done
import org.apache.pekko.actor.typed.ActorSystem

import scala.annotation.tailrec
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.jdk.FutureConverters.*

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


