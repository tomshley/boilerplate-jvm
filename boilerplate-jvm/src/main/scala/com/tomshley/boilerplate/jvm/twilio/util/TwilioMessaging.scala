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

import com.twilio.`type`.PhoneNumber
import com.twilio.base.TwilioResponse
import com.twilio.constant.EnumConstants
import com.twilio.exception.ApiException
import com.twilio.http.{HttpMethod, NetworkHttpClient, TwilioRestClient, Request as SdkRequest, Response as SdkResponse}
import com.twilio.rest.api.v2010.account.Message
import org.apache.hc.client5.http.ConnectTimeoutException
import org.apache.hc.client5.http.config.{ConnectionConfig, RequestConfig}
import org.apache.hc.client5.http.impl.classic.HttpClientBuilder
import org.apache.hc.client5.http.impl.io.PoolingHttpClientConnectionManagerBuilder
import org.apache.hc.core5.http.{ConnectionRequestTimeoutException, Header}
import org.apache.hc.core5.http.io.SocketConfig
import org.apache.hc.core5.io.CloseMode
import org.apache.hc.core5.util.Timeout
import org.apache.pekko.Done
import org.apache.pekko.actor.CoordinatedShutdown
import org.apache.pekko.actor.typed.{ActorSystem, DispatcherSelector}
import org.apache.pekko.http.scaladsl.model.Uri
import org.apache.pekko.stream.scaladsl.{Sink, Source}
import org.apache.pekko.stream.SystemMaterializer
import spray.json.*

import java.net.{ConnectException, UnknownHostException}
import java.nio.charset.StandardCharsets.UTF_8
import java.util.concurrent.TimeoutException
import scala.annotation.tailrec
import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration.{Duration, DurationLong, FiniteDuration}
import scala.jdk.CollectionConverters.*
import scala.util.{Success, Try, Using}
import scala.util.control.NonFatal

/** SMS and email submissions through per-instance Twilio SDK clients.
 *  SDK calls run on Pekko's blocking-I/O dispatcher. Each verdict has a
 *  deadline; retries remain the caller's responsibility. Email uses the
 *  SDK's generic request API because it has no generated email resource.
 */
final class TwilioMessaging(
    config: TwilioMessagingConfig,
    smsTransport: TwilioMessaging.SmsTransport,
    emailTransport: TwilioMessaging.EmailTransport
)(using system: ActorSystem[?]) {

  import TwilioSendVerdict.*

  private given ExecutionContext = system.executionContext
  private given org.apache.pekko.stream.Materializer =
    SystemMaterializer(system).materializer
  private val blockingEc: ExecutionContext =
    system.dispatchers.lookup(DispatcherSelector.blocking())

  /** The email channel paired with its parsed endpoint — parsing and
   *  validation happen at construction so an invalid `baseUrl` fails
   *  wiring, not a send. */
  private val emailChannel: Option[(TwilioEmailChannel, Uri)] =
    config.email.map { channel =>
      val origin = Uri(channel.baseUrl)
      TwilioMessaging.requireSecureEmailEndpoint(origin)
      channel -> origin.withPath(Uri.Path("/v1/Emails"))
    }

  /** Submit one SMS through the Twilio Messages API. */
  def sendSms(to: String, body: String): Future[TwilioSendVerdict] =
    config.sms match {
      case None =>
        Future.failed(new IllegalStateException("twilio sms channel is not configured"))
      case Some(_) =>
        withinDeadline {
          Future.fromTry(Try(smsTransport(to, body))).flatten
            .map(classifySmsResponse)
            .recover { case error: ApiException => classifySmsError(error) }
        }.recover {
          case _: TwilioMessaging.DeadlineExceeded =>
            OutcomeUnknown("twilio request deadline exceeded")
          case NonFatal(error) =>
            if (TwilioMessaging.neverSent(error))
              Retryable(s"twilio transport failure: ${error.getClass.getSimpleName}", None)
            else
              OutcomeUnknown(s"twilio transport failure: ${error.getClass.getSimpleName}")
        }
    }

  /** Submit one email through Twilio's `/v1/Emails` API. `body` is the
   *  caller's plain text; the API requires an HTML part, so the text is
   *  escaped into `content.html` while `content.text` carries it raw.
   *  The SDK client stamps Basic auth on the request when it sends. */
  def sendEmail(to: String, subject: String, body: String): Future[TwilioSendVerdict] =
    emailChannel match {
      case None =>
        Future.failed(new IllegalStateException("twilio email channel is not configured"))
      case Some((channel, endpoint)) =>
        val payload = JsObject(
          "from" -> JsObject(
            "address" -> JsString(channel.fromAddress),
            "name" -> JsString(channel.fromName)
          ),
          "to" -> JsArray(JsObject("address" -> JsString(to))),
          "content" -> JsObject(
            "subject" -> JsString(subject),
            "html" -> JsString(TwilioMessaging.htmlBody(body)),
            "text" -> JsString(body)
          )
        )
        val request = new SdkRequest(HttpMethod.POST, endpoint.toString)
        request.setContentType(EnumConstants.ContentType.JSON)
        request.setBody(payload.compactPrint)
        withinDeadline {
          Future.fromTry(Try(emailTransport(request))).flatten
            .flatMap(decodeEmail)
        }.recover {
          case _: TwilioMessaging.DeadlineExceeded =>
            OutcomeUnknown("twilio request deadline exceeded")
          case NonFatal(error) =>
            if (TwilioMessaging.neverSent(error))
              Retryable(s"twilio email transport failure: ${error.getClass.getSimpleName}", None)
            else
              OutcomeUnknown(s"twilio email transport failure: ${error.getClass.getSimpleName}")
        }
    }

  /** Preserve transport failures as values so only the stream's own timeout
   *  becomes DeadlineExceeded. Late attempts still complete their cleanup. */
  private def withinDeadline[A](attempt: Future[A]): Future[A] =
    Source.future(attempt.transform(Success(_)))
      .completionTimeout(config.requestTimeout)
      .runWith(Sink.head)
      .recoverWith { case _: TimeoutException =>
        Future.failed(new TwilioMessaging.DeadlineExceeded)
      }
      .flatMap(Future.fromTry)

  private def classifySmsResponse(response: TwilioResponse[Message]): TwilioSendVerdict = {
    val retryAfter = retryAfterOf(response.getHeaders)
    if response.getStatusCode / 100 == 2 then
      // A 2xx already means queued; a missing sid only costs the reference,
      // exactly as an unreadable 202 body does on the email channel.
      Accepted(Option(response.getContent).flatMap(m => Option(m.getSid)).filter(_.nonEmpty))
    else Retryable(s"twilio http ${response.getStatusCode}", retryAfter)
  }

  private def classifySmsError(error: ApiException): TwilioSendVerdict = {
    val status = Option(error.getStatusCode).map(_.intValue)
    val code = Option(error.getCode).map(_.toString)
    (status, code) match {
      case (Some(400), Some("21211")) =>
        InvalidRecipient(Some("21211"), "twilio invalid recipient")
      case (Some(400), Some("21610")) =>
        RecipientOptedOut(Some("21610"), "twilio recipient opted out")
      case (Some(400), _) =>
        Retryable("twilio http 400 (operator attention)", None)
      case (Some(401), _) =>
        Rejected(401, Some(code.getOrElse("401")),
          "twilio credentials unusable — the account sid or auth token was rejected")
      case (Some(403), _) =>
        Rejected(403, Some(code.getOrElse("403")),
          "twilio refused the account or sender — the from number is not usable on this account")
      case (Some(other), _) =>
        Retryable(s"twilio http $other", None)
      case (None, _) if TwilioMessaging.neverSent(error) =>
        Retryable("twilio transport failure: ApiException", None)
      case (None, _) =>
        OutcomeUnknown("twilio response unreadable")
    }
  }

  /** Reads at most `responseMaxBytes` of the response body for verdict
   *  metadata — the SDK already buffered the body, so this bounds only the
   *  decode. An oversized or unreadable body degrades to status-only
   *  classification. */
  private def decodeEmail(response: SdkResponse): Future[TwilioSendVerdict] =
    Future {
      val body = Try(Using.resource(response.getStream) { stream =>
        val bytes = stream.readNBytes((config.responseMaxBytes + 1).toInt)
        if (bytes.length > config.responseMaxBytes) ""
        else new String(bytes, UTF_8)
      }).getOrElse("")
      classifyEmail(response.getStatusCode, body, retryAfterOf(response.getHeaders))
    }(blockingEc)

  private def classifyEmail(
      status: Int,
      body: String,
      retryAfter: Option[FiniteDuration]
  ): TwilioSendVerdict =
    status match {
      case 202 =>
        Accepted(operationIdOf(body))
      case 413 =>
        Rejected(413, Some("413"), "twilio email message exceeds provider limits")
      case 400 =>
        classifyEmailBadRequest(body)
      case 401 =>
        Rejected(401, Some("401"),
          "twilio email credentials unusable — the account sid or auth token was rejected")
      case 403 =>
        Rejected(403, Some("403"), "twilio email refused the account or sender")
      case other =>
        Retryable(s"twilio email http $other", retryAfter)
    }

  private def classifyEmailBadRequest(body: String): TwilioSendVerdict = {
    val recipientRefused = Try(body.parseJson.asJsObject).toOption
      .flatMap(_.fields.get("errors"))
      .collect { case JsArray(elements) => elements.toVector }
      .getOrElse(Vector.empty)
      .exists {
        case JsObject(entry) =>
          entry.get("message").collect { case JsString(message) => message }.exists(_.contains("to["))
        case _ => false
      }
    if recipientRefused then InvalidRecipient(Some("400"), "twilio email invalid recipient")
    else Rejected(400, Some("400"), "twilio email rejected")
  }

  private def operationIdOf(body: String): Option[String] =
    Try(body.parseJson.asJsObject).toOption
      .flatMap(_.fields.get("operationId"))
      .collect { case JsString(value) => value }
      .filter(_.nonEmpty)

  private def retryAfterOf(headers: Array[Header]): Option[FiniteDuration] =
    Option(headers).getOrElse(Array.empty[Header])
      .find(_.getName.equalsIgnoreCase("retry-after"))
      .flatMap(header => TwilioMessaging.retryAfterSeconds(header.getValue))

  private def retryAfterOf(headers: java.util.Map[String, String]): Option[FiniteDuration] =
    headers.asScala.iterator
      .find(_._1.equalsIgnoreCase("retry-after"))
      .flatMap((_, value) => TwilioMessaging.retryAfterSeconds(value))
}

object TwilioMessaging {

  /** The request deadline won before the attempt produced a verdict. */
  private final class DeadlineExceeded
      extends TimeoutException("twilio request deadline exceeded")
      with scala.util.control.NoStackTrace

  /** Require an HTTPS origin; allow literal loopback or localhost over HTTP
   *  for local use. */
  private def requireSecureEmailEndpoint(endpoint: Uri): Unit = {
    require(
      endpoint.authority.userinfo.isEmpty &&
        endpoint.rawQueryString.isEmpty &&
        endpoint.fragment.isEmpty &&
        (endpoint.path.isEmpty || endpoint.path == Uri.Path("/")),
      "twilio email baseUrl must be an origin — no userinfo, path, query, or fragment"
    )
    val host = endpoint.authority.host
    val loopback = host match {
      case Uri.NamedHost(name) => name.equalsIgnoreCase("localhost")
      case _ => host.inetAddresses.exists(_.isLoopbackAddress)
    }
    require(
      !host.isEmpty &&
        (endpoint.scheme == "https" ||
          (endpoint.scheme == "http" && loopback)),
      "twilio email baseUrl must use https (plain http is allowed only for a loopback host)"
    )
  }

  /** A wired [[TwilioMessaging]] on the production SDK transports. */
  def apply(config: TwilioMessagingConfig)(using system: ActorSystem[?]): TwilioMessaging =
    new TwilioMessaging(
      config,
      sdkSmsTransport(config),
      sdkEmailTransport(config)
    )

  /** `(to, body) => submission attempt`. Success resolves the SDK
   *  `TwilioResponse` of an accepted (2xx) submission; provider refusals
   *  surface as a failed future carrying [[ApiException]], transport
   *  failures as other exceptions (e.g. `ApiConnectionException`). */
  type SmsTransport = (String, String) => Future[TwilioResponse[Message]]

  /** One SDK request attempt — the email channel's transport. */
  type EmailTransport = SdkRequest => Future[SdkResponse]

  /** SDK transport with bounded timeouts, no retries or redirects, and
   *  shutdown cleanup. */
  private[util] def sdkHttpClient(config: TwilioMessagingConfig)(using
      system: ActorSystem[?]
  ): NetworkHttpClient = {
    val timeout = Timeout.ofMilliseconds(config.requestTimeout.toMillis)
    val pool = PoolingHttpClientConnectionManagerBuilder.create()
      .setDefaultSocketConfig(SocketConfig.custom().setSoTimeout(timeout).build())
      .setDefaultConnectionConfig(ConnectionConfig.custom()
        .setConnectTimeout(timeout)
        .setSocketTimeout(timeout)
        .build())
      .build()
    val requests = RequestConfig.custom()
      .setConnectionRequestTimeout(timeout)
      .setResponseTimeout(timeout)
      .build()
    val httpBuilder = HttpClientBuilder.create()
      .useSystemProperties()
      .setConnectionManager(pool)
      .setDefaultRequestConfig(requests)
      .disableAutomaticRetries()
      .disableRedirectHandling()
    val transport = new NetworkHttpClient(httpBuilder) {
      override def reliableRequest(request: SdkRequest): SdkResponse =
        makeRequest(request)
    }
    val blockingEc = system.dispatchers.lookup(DispatcherSelector.blocking())
    CoordinatedShutdown(system.classicSystem).addTask(
      CoordinatedShutdown.PhaseBeforeActorSystemTerminate,
      s"twilio-http-${java.util.UUID.randomUUID()}") { () =>
      Future { pool.close(CloseMode.IMMEDIATE); Done }(blockingEc)
    }
    transport
  }

  /** One SMS submission through a caller-supplied [[TwilioRestClient]] —
   *  the SDK `Message.creator` path on Pekko's blocking-IO dispatcher. */
  private[util] def smsTransport(client: TwilioRestClient, channel: TwilioSmsChannel)(using
      system: ActorSystem[?]
  ): SmsTransport = {
    val from = new PhoneNumber(channel.fromNumber)
    val blockingEc = system.dispatchers.lookup(DispatcherSelector.blocking())
    (to, body) =>
      Future {
        Message
          .creator(new PhoneNumber(to), from, body)
          .createWithResponse(client)
      }(blockingEc)
  }

  /** SDK-backed SMS transport on a per-instance [[TwilioRestClient]] —
   *  never `Twilio.init`. Routing overrides come from the channel config;
   *  an unconfigured channel yields a transport that always fails fast. */
  def sdkSmsTransport(config: TwilioMessagingConfig)(using system: ActorSystem[?]): SmsTransport =
    config.sms match {
      case None =>
        (_, _) => Future.failed(new IllegalStateException("twilio sms channel is not configured"))
      case Some(channel) =>
        val builder = new TwilioRestClient.Builder(config.accountSid, config.authToken)
          .httpClient(sdkHttpClient(config))
        channel.routingLocation.foreach(builder.edge)
        channel.region.foreach(builder.region)
        smsTransport(builder.build(), channel)
    }

  /** SDK-backed email transport — `client.request` sends each `SdkRequest`
   *  through a per-instance client that stamps Basic auth at send time.
   *  SMS routing overrides do not apply: the email endpoint is fixed. An
   *  unconfigured channel yields a transport that always fails fast. */
  def sdkEmailTransport(config: TwilioMessagingConfig)(using
      system: ActorSystem[?]
  ): EmailTransport =
    config.email match {
      case None =>
        _ => Future.failed(new IllegalStateException("twilio email channel is not configured"))
      case Some(_) =>
        val client = new TwilioRestClient.Builder(config.accountSid, config.authToken)
          .httpClient(sdkHttpClient(config))
          .build()
        val blockingEc = system.dispatchers.lookup(DispatcherSelector.blocking())
        request => Future(client.request(request))(blockingEc)
    }

  /** Parse a non-negative Retry-After delta without propagating malformed
   *  or overflowing values. */
  private def retryAfterSeconds(value: String): Option[FiniteDuration] =
    Try(value.trim.toLong.seconds).toOption.filter(_ >= Duration.Zero)

  /** Failures known to occur before the HTTP request is sent. */
  private def neverSent(error: Throwable): Boolean = {
    @tailrec
    def walk(current: Option[Throwable], remaining: Int): Boolean =
      current match {
        case Some(cause) if remaining > 0 =>
          cause match {
            case _: ConnectException => true
            case _: UnknownHostException => true
            case _: ConnectTimeoutException => true
            case _: ConnectionRequestTimeoutException => true
            case _ => walk(Option(cause.getCause), remaining - 1)
          }
        case _ => false
      }
    walk(Option(error), 16)
  }

  /** The API requires an HTML body, but callers submit plain text — escaped
   *  (ampersand first, so the escapes themselves are not re-escaped) and
   *  wrapped before interpolation into `content.html`. */
  private def htmlBody(text: String): String = {
    val escaped = text
      .replace("&", "&amp;")
      .replace("<", "&lt;")
      .replace(">", "&gt;")
      .replace("\"", "&quot;")
      .replace("\n", "<br/>")
    s"<p>$escaped</p>"
  }
}
