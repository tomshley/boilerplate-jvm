package com.tomshley.boilerplate.jvm.twilio.util

import com.fasterxml.jackson.databind.ObjectMapper
import com.twilio.base.TwilioResponse
import com.twilio.constant.EnumConstants
import com.twilio.exception.{ApiConnectionException, ApiException, RestException}
import com.twilio.http.{HttpClient as TwilioHttpClient, HttpMethod, TwilioRestClient, Request as SdkRequest, Response as SdkResponse}
import com.twilio.rest.api.v2010.account.Message
import org.apache.hc.client5.http.ConnectTimeoutException
import org.apache.hc.core5.http.{ConnectionRequestTimeoutException, Header}
import org.apache.hc.core5.http.message.BasicHeader
import org.apache.pekko.Done
import org.apache.pekko.actor.testkit.typed.scaladsl.{ActorTestKit, LogCapturing, ScalaTestWithActorTestKit}
import org.apache.pekko.actor.typed.ActorSystem
import org.apache.pekko.http.scaladsl.Http
import org.apache.pekko.http.scaladsl.model.*
import org.apache.pekko.http.scaladsl.model.headers.Location
import org.apache.pekko.stream.{Materializer, SystemMaterializer}
import org.scalatest.time.{Millis, Seconds, Span}
import org.scalatest.wordspec.AnyWordSpecLike
import spray.json.*

import java.io.{ByteArrayInputStream, IOException, InputStream}
import java.net.{ConnectException, ServerSocket, SocketTimeoutException}
import java.nio.charset.StandardCharsets.UTF_8
import java.util.Base64
import java.util.concurrent.TimeoutException
import scala.concurrent.{Future, Promise}
import scala.concurrent.duration.{DurationInt, FiniteDuration}

final class TwilioMessagingSpec
    extends ScalaTestWithActorTestKit
    with AnyWordSpecLike
    with LogCapturing {

  private given ActorSystem[?] = system
  private given Materializer = SystemMaterializer(system).materializer

  override implicit val patienceConfig: PatienceConfig =
    PatienceConfig(timeout = Span(3, Seconds), interval = Span(25, Millis))

  private val config = TwilioMessagingConfig(
    accountSid = "ACtest",
    authToken = "token",
    sms = Some(TwilioSmsChannel(fromNumber = "+15550001111")),
    email = Some(TwilioEmailChannel(fromAddress = "noreply@example.com", fromName = "Alerts")),
    requestTimeout = 5.seconds
  )

  private def messaging(
      config: TwilioMessagingConfig = config,
      sms: TwilioMessaging.SmsTransport = (_, _) => Future.failed(new UnsupportedOperationException),
      email: TwilioMessaging.EmailTransport = _ => Future.failed(new UnsupportedOperationException)
  ): TwilioMessaging =
    new TwilioMessaging(config, sms, email)

  private def smsMessage(sid: String): Message =
    Message.fromJson(s"""{"sid": "$sid"}""", new ObjectMapper())

  private def smsResponse(status: Int, sid: Option[String]): TwilioResponse[Message] =
    new TwilioResponse[Message](sid.map(smsMessage).orNull, status, Array.empty)

  private def apiError(status: Int, code: Int): ApiException = {
    val json = s"""{"status": $status, "code": $code, "message": "provider detail"}"""
    new ApiException(RestException.fromJson(new ByteArrayInputStream(json.getBytes(UTF_8)), new ObjectMapper()))
  }

  private def sdkResponse(
      body: String,
      status: Int,
      headers: Array[Header] = Array.empty
  ): SdkResponse =
    new SdkResponse(body, status, headers)

  /** The decode closes the stream it reads — this fixture records it. */
  private final class CloseTrackingStream(body: String, closed: Promise[Done])
      extends ByteArrayInputStream(body.getBytes(UTF_8)) {
    override def close(): Unit = {
      super.close()
      closed.trySuccess(Done)
      ()
    }
  }

  private final class FailingStream(closed: Promise[Done]) extends InputStream {
    override def read(): Int = throw new IOException("stream exploded")
    override def close(): Unit = {
      closed.trySuccess(Done)
      ()
    }
  }

  private final case class CapturedRequest(
      method: String,
      path: String,
      auth: Option[String],
      body: String)

  /** Binds a loopback pekko-http fixture for the real SDK transport, runs
   *  `f` with the port and a probe of decoded requests, and always
   *  terminates the binding. */
  private def withEmailServer(
      respond: String => Future[HttpResponse]
  )(f: (Int, org.apache.pekko.actor.testkit.typed.scaladsl.TestProbe[CapturedRequest]) => Unit): Unit = {
    val probe = testKit.createTestProbe[CapturedRequest]()
    val binding = Http()(using system.classicSystem)
      .newServerAt("127.0.0.1", 0)
      .bind { request =>
        request.entity.toStrict(500.millis).flatMap { strict =>
          probe.ref ! CapturedRequest(
            request.method.value,
            request.uri.path.toString,
            request.headers.find(_.is("authorization")).map(_.value),
            strict.data.utf8String)
          respond(strict.data.utf8String)
        }(system.executionContext)
      }.futureValue
    try f(binding.localAddress.getPort, probe)
    finally binding.terminate(1.second).futureValue
  }

  private def loopbackConfig(port: Int, timeout: FiniteDuration = 5.seconds): TwilioMessagingConfig =
    config.copy(
      sms = None,
      email = Some(TwilioEmailChannel(
        fromAddress = "noreply@example.com",
        fromName = "Alerts",
        baseUrl = s"http://127.0.0.1:$port")),
      requestTimeout = timeout)

  "TwilioMessaging.sendSms" should {
    "return Accepted with the message sid on 2xx" in {
      val client = messaging(sms = (_, _) => Future.successful(smsResponse(201, Some("SM123"))))
      client.sendSms("+15551212", "hi").futureValue shouldBe
        TwilioSendVerdict.Accepted(Some("SM123"))
    }

    "accept a 2xx carrying no sid — the request was already queued" in {
      val client = messaging(sms = (_, _) => Future.successful(smsResponse(201, None)))
      client.sendSms("+15551212", "hi").futureValue shouldBe
        TwilioSendVerdict.Accepted(None)
    }

    "classify 21211 as an invalid-recipient refusal" in {
      val client = messaging(sms = (_, _) => Future.failed(apiError(400, 21211)))
      client.sendSms("+15551212", "hi").futureValue shouldBe
        TwilioSendVerdict.InvalidRecipient(Some("21211"), "twilio invalid recipient")
    }

    "classify 21610 as an opt-out refusal" in {
      val client = messaging(sms = (_, _) => Future.failed(apiError(400, 21610)))
      client.sendSms("+15551212", "hi").futureValue shouldBe a[TwilioSendVerdict.RecipientOptedOut]
    }

    "retry a 400 without a recipient code" in {
      val client = messaging(sms = (_, _) => Future.failed(apiError(400, 30007)))
      client.sendSms("+15551212", "hi").futureValue shouldBe a[TwilioSendVerdict.Retryable]
    }

    "refuse 401 permanently" in {
      val client = messaging(sms = (_, _) => Future.failed(apiError(401, 20003)))
      client.sendSms("+15551212", "hi").futureValue shouldBe a[TwilioSendVerdict.Rejected]
    }

    "refuse 403 permanently" in {
      val client = messaging(sms = (_, _) => Future.failed(apiError(403, 20001)))
      client.sendSms("+15551212", "hi").futureValue shouldBe
        TwilioSendVerdict.Rejected(403, Some("20001"),
          "twilio refused the account or sender — the from number is not usable on this account")
    }

    "report the outcome as unknown when the error carries no status" in {
      val client = messaging(sms = (_, _) => Future.failed(new ApiException("x")))
      client.sendSms("+15551212", "hi").futureValue shouldBe
        TwilioSendVerdict.OutcomeUnknown("twilio response unreadable")
    }

    "retry when the connection provably failed before submission" in {
      val failure = new ApiConnectionException("no response", new ConnectException("refused"))
      val client = messaging(sms = (_, _) => Future.failed(failure))
      client.sendSms("+15551212", "hi").futureValue shouldBe a[TwilioSendVerdict.Retryable]
    }

    "retry an SDK-wrapped connection refusal — it never reached Twilio" in {
      val failure = new ApiException("SECRET-SENTINEL", new ConnectException("refused"))
      val client = messaging(sms = (_, _) => Future.failed(failure))
      client.sendSms("+15551212", "hi").futureValue shouldBe
        TwilioSendVerdict.Retryable("twilio transport failure: ApiException", None)
    }

    "report an SDK-wrapped failure with an opaque cause as unknown" in {
      val failure = new ApiException("SECRET-SENTINEL", new IOException("reset"))
      val client = messaging(sms = (_, _) => Future.failed(failure))
      client.sendSms("+15551212", "hi").futureValue shouldBe
        TwilioSendVerdict.OutcomeUnknown("twilio response unreadable")
    }

    "retry SDK-wrapped connect and pool-acquisition timeouts" in {
      for (cause <- Seq[Throwable](
        new ConnectTimeoutException("connect timed out"),
        new ConnectionRequestTimeoutException("pool wait timed out")
      )) {
        val client = messaging(sms = (_, _) =>
          Future.failed(new ApiException("SECRET-SENTINEL", cause)))
        client.sendSms("+15551212", "hi").futureValue shouldBe
          TwilioSendVerdict.Retryable("twilio transport failure: ApiException", None)
      }
    }

    "report a raw socket read timeout as unknown — it may have reached Twilio" in {
      val client = messaging(sms = (_, _) => Future.failed(new SocketTimeoutException("read timed out")))
      client.sendSms("+15551212", "hi").futureValue shouldBe
        TwilioSendVerdict.OutcomeUnknown("twilio transport failure: SocketTimeoutException")
    }

    "retry a synchronous transport throw that proves never-sent" in {
      val client = messaging(sms = (_, _) => throw new ConnectException("refused"))
      client.sendSms("+15551212", "hi").futureValue shouldBe
        a[TwilioSendVerdict.Retryable]
    }

    "report a synchronous transport throw as unknown without its message" in {
      val client = messaging(sms = (_, _) => throw new RuntimeException("SECRET-SENTINEL"))
      client.sendSms("+15551212", "hi").futureValue shouldBe
        TwilioSendVerdict.OutcomeUnknown("twilio transport failure: RuntimeException")
    }

    "report an unexpected transport failure as unknown — it may have reached Twilio" in {
      val client = messaging(sms = (_, _) => Future.failed(new RuntimeException("socket reset")))
      client.sendSms("+15551212", "hi").futureValue shouldBe a[TwilioSendVerdict.OutcomeUnknown]
    }

    "report the outcome as unknown when the request deadline wins" in {
      val client = messaging(
        config = config.copy(requestTimeout = 200.millis),
        sms = (_, _) => Future.never
      )
      client.sendSms("+15551212", "hi").futureValue shouldBe
        TwilioSendVerdict.OutcomeUnknown("twilio request deadline exceeded")
    }

    "drive the real SDK Message.creator through a stub HttpClient" in {
      val seen = Promise[SdkRequest]()
      val stubHttp = new TwilioHttpClient {
        override def makeRequest(request: SdkRequest): SdkResponse = {
          seen.trySuccess(request)
          new SdkResponse("""{"sid":"SMfixture"}""", 201)
        }
      }
      val client = new TwilioRestClient.Builder("ACtest", "token").httpClient(stubHttp).build()
      val transport =
        TwilioMessaging.smsTransport(client, TwilioSmsChannel(fromNumber = "+15550001111"))
      val messaging = new TwilioMessaging(
        config.copy(email = None),
        transport,
        _ => Future.failed(new UnsupportedOperationException))
      messaging.sendSms("+15551212", "hi").futureValue shouldBe
        TwilioSendVerdict.Accepted(Some("SMfixture"))

      val request = seen.future.futureValue
      request.getMethod shouldBe HttpMethod.POST
      request.getUrl should include("/2010-04-01/Accounts/ACtest/Messages.json")
      request.getPostParams.get("To") shouldBe java.util.List.of("+15551212")
      request.getPostParams.get("From") shouldBe java.util.List.of("+15550001111")
      request.getPostParams.get("Body") shouldBe java.util.List.of("hi")
    }

    "fail fast when the sms channel is unconfigured" in {
      val bare = new TwilioMessaging(
        config.copy(sms = None),
        (_, _) => Future.failed(new UnsupportedOperationException),
        _ => Future.failed(new UnsupportedOperationException)
      )
      bare.sendSms("+15551212", "hi").failed.futureValue shouldBe a[IllegalStateException]
    }

    "produce a fail-fast sdk transport when the sms channel is unconfigured" in {
      TwilioMessaging.sdkSmsTransport(config.copy(sms = None))("+15551212", "hi")
        .failed.futureValue shouldBe a[IllegalStateException]
    }
  }

  "TwilioMessaging.sendEmail" should {
    "return Accepted with the operation id on 202" in {
      val client = messaging(email = _ => Future.successful(sdkResponse("""{"operationId":"op-1"}""", 202)))
      client.sendEmail("staff@example.com", "subj", "body").futureValue shouldBe
        TwilioSendVerdict.Accepted(Some("op-1"))
    }

    "still accept a 202 whose body cannot be parsed" in {
      val client = messaging(email = _ => Future.successful(sdkResponse("not json", 202)))
      client.sendEmail("staff@example.com", "subj", "body").futureValue shouldBe
        TwilioSendVerdict.Accepted(None)
    }

    "classify a recipient-path 400 as InvalidRecipient" in {
      val body = """{"errors":[{"message":"to[0].address is invalid"}]}"""
      val client = messaging(email = _ => Future.successful(sdkResponse(body, 400)))
      client.sendEmail("staff@example.com", "subj", "body").futureValue shouldBe
        TwilioSendVerdict.InvalidRecipient(Some("400"), "twilio email invalid recipient")
    }

    "classify a non-recipient 400 as Rejected" in {
      val body = """{"errors":[{"message":"content.html is required"}]}"""
      val client = messaging(email = _ => Future.successful(sdkResponse(body, 400)))
      client.sendEmail("staff@example.com", "subj", "body").futureValue shouldBe
        TwilioSendVerdict.Rejected(400, Some("400"), "twilio email rejected")
    }

    "refuse 401 permanently" in {
      val client = messaging(email = _ => Future.successful(sdkResponse("{}", 401)))
      client.sendEmail("staff@example.com", "subj", "body").futureValue shouldBe
        TwilioSendVerdict.Rejected(401, Some("401"),
          "twilio email credentials unusable — the account sid or auth token was rejected")
    }

    "refuse 403 permanently" in {
      val client = messaging(email = _ => Future.successful(sdkResponse("{}", 403)))
      client.sendEmail("staff@example.com", "subj", "body").futureValue shouldBe
        TwilioSendVerdict.Rejected(403, Some("403"), "twilio email refused the account or sender")
    }

    "refuse 413 permanently" in {
      val client = messaging(email = _ => Future.successful(sdkResponse("{}", 413)))
      client.sendEmail("staff@example.com", "subj", "body").futureValue shouldBe
        TwilioSendVerdict.Rejected(413, Some("413"), "twilio email message exceeds provider limits")
    }

    "retry a rate limit with the provider's Retry-After hint" in {
      val response = sdkResponse("{}", 429, Array(BasicHeader("Retry-After", "30")))
      val client = messaging(email = _ => Future.successful(response))
      client.sendEmail("staff@example.com", "subj", "body").futureValue match {
        case TwilioSendVerdict.Retryable(_, retryAfter) =>
          retryAfter shouldBe Some(30.seconds)
        case other => fail(s"expected Retryable, got $other")
      }
    }

    "ignore unusable Retry-After hints on a retryable refusal" in {
      for (hint <- Seq("-5", "soon", Long.MaxValue.toString)) {
        val response = sdkResponse("{}", 429, Array(BasicHeader("Retry-After", hint)))
        val client = messaging(email = _ => Future.successful(response))
        client.sendEmail("staff@example.com", "subj", "body").futureValue match {
          case TwilioSendVerdict.Retryable(_, retryAfter) =>
            retryAfter shouldBe None
          case other => fail(s"expected Retryable, got $other")
        }
      }
    }

    "keep a 202 accepted despite an unusable Retry-After hint" in {
      for (hint <- Seq("soon", Long.MaxValue.toString)) {
        val response = sdkResponse("""{"operationId":"op-1"}""", 202,
          Array(BasicHeader("Retry-After", hint)))
        val client = messaging(email = _ => Future.successful(response))
        client.sendEmail("staff@example.com", "subj", "body").futureValue shouldBe
          TwilioSendVerdict.Accepted(Some("op-1"))
      }
    }

    "retry an unexpected status" in {
      val client = messaging(email = _ => Future.successful(sdkResponse("{}", 502)))
      client.sendEmail("staff@example.com", "subj", "body").futureValue shouldBe a[TwilioSendVerdict.Retryable]
    }

    "report an unexpected transport failure as unknown — it may have reached Twilio" in {
      val client = messaging(email = _ => Future.failed(new RuntimeException("reset")))
      client.sendEmail("staff@example.com", "subj", "body").futureValue shouldBe a[TwilioSendVerdict.OutcomeUnknown]
    }

    "retry a synchronous transport throw that proves never-sent" in {
      val client = messaging(email = _ => throw new ConnectException("refused"))
      client.sendEmail("staff@example.com", "subj", "body").futureValue shouldBe
        a[TwilioSendVerdict.Retryable]
    }

    "report a synchronous transport throw as unknown without its message" in {
      val client = messaging(email = _ => throw new RuntimeException("SECRET-SENTINEL"))
      client.sendEmail("staff@example.com", "subj", "body").futureValue shouldBe
        TwilioSendVerdict.OutcomeUnknown("twilio email transport failure: RuntimeException")
    }

    "retry SDK-wrapped connect and pool-acquisition timeouts" in {
      for (cause <- Seq[Throwable](
        new ConnectTimeoutException("connect timed out"),
        new ConnectionRequestTimeoutException("pool wait timed out")
      )) {
        val client = messaging(email = _ =>
          Future.failed(new ApiException("SECRET-SENTINEL", cause)))
        client.sendEmail("staff@example.com", "subj", "body").futureValue shouldBe
          TwilioSendVerdict.Retryable("twilio email transport failure: ApiException", None)
      }
    }

    "report a raw socket read timeout as unknown — it may have reached Twilio" in {
      val client = messaging(email = _ => Future.failed(new SocketTimeoutException("read timed out")))
      client.sendEmail("staff@example.com", "subj", "body").futureValue shouldBe
        TwilioSendVerdict.OutcomeUnknown("twilio email transport failure: SocketTimeoutException")
    }

    "keep a 401 refusal permanent when the body exceeds the decode bound" in {
      val tiny = config.copy(responseMaxBytes = 4)
      val client = messaging(
        config = tiny,
        email = _ => Future.successful(sdkResponse("""{"error":"detail"}""", 401))
      )
      client.sendEmail("staff@example.com", "subj", "body").futureValue shouldBe
        TwilioSendVerdict.Rejected(401, Some("401"),
          "twilio email credentials unusable — the account sid or auth token was rejected")
    }

    "keep a 400 refusal permanent when the body exceeds the decode bound" in {
      val tiny = config.copy(responseMaxBytes = 4)
      val client = messaging(
        config = tiny,
        email = _ => Future.successful(sdkResponse("""{"errors":[{"message":"to[0].address is invalid"}]}""", 400))
      )
      client.sendEmail("staff@example.com", "subj", "body").futureValue shouldBe
        TwilioSendVerdict.Rejected(400, Some("400"), "twilio email rejected")
    }

    "accept a 202 whose body exceeds the decode bound" in {
      val tiny = config.copy(responseMaxBytes = 4)
      val client = messaging(
        config = tiny,
        email = _ => Future.successful(sdkResponse("""{"operationId":"op-1"}""", 202))
      )
      client.sendEmail("staff@example.com", "subj", "body").futureValue shouldBe
        TwilioSendVerdict.Accepted(None)
    }

    "retry a 503 whose body exceeds the decode bound" in {
      val tiny = config.copy(responseMaxBytes = 4)
      val client = messaging(
        config = tiny,
        email = _ => Future.successful(sdkResponse("""{"error":"busy"}""", 503))
      )
      client.sendEmail("staff@example.com", "subj", "body").futureValue shouldBe
        TwilioSendVerdict.Retryable("twilio email http 503", None)
    }

    "close the response stream after a successful decode" in {
      val closed = Promise[Done]()
      val client = messaging(email = _ =>
        Future.successful(new SdkResponse(new CloseTrackingStream("""{"operationId":"op-1"}""", closed), 202)))
      client.sendEmail("staff@example.com", "subj", "body").futureValue shouldBe
        TwilioSendVerdict.Accepted(Some("op-1"))
      closed.future.futureValue shouldBe Done
    }

    "close an oversized response stream and classify by status" in {
      val closed = Promise[Done]()
      val client = messaging(
        config = config.copy(responseMaxBytes = 4),
        email = _ => Future.successful(new SdkResponse(new CloseTrackingStream("""{"operationId":"op-1"}""", closed), 202)))
      client.sendEmail("staff@example.com", "subj", "body").futureValue shouldBe
        TwilioSendVerdict.Accepted(None)
      closed.future.futureValue shouldBe Done
    }

    "close a stream that throws during decode" in {
      val closed = Promise[Done]()
      val client = messaging(email = _ =>
        Future.successful(new SdkResponse(new FailingStream(closed), 401)))
      client.sendEmail("staff@example.com", "subj", "body").futureValue shouldBe
        TwilioSendVerdict.Rejected(401, Some("401"),
          "twilio email credentials unusable — the account sid or auth token was rejected")
      closed.future.futureValue shouldBe Done
    }

    "report the outcome as unknown when the request deadline wins" in {
      val client = messaging(
        config = config.copy(requestTimeout = 200.millis),
        email = _ => Future.never
      )
      client.sendEmail("staff@example.com", "subj", "body").futureValue shouldBe
        TwilioSendVerdict.OutcomeUnknown("twilio request deadline exceeded")
    }

    "close the stream of a response that arrives after the deadline" in {
      val closed = Promise[Done]()
      val late = Promise[SdkResponse]()
      val client = messaging(
        config = config.copy(requestTimeout = 100.millis),
        email = _ => late.future
      )
      client.sendEmail("staff@example.com", "subj", "body").futureValue shouldBe
        TwilioSendVerdict.OutcomeUnknown("twilio request deadline exceeded")
      late.success(new SdkResponse(new CloseTrackingStream("{}", closed), 202))
      closed.future.futureValue shouldBe Done
    }

    "report the attempt's own timeout as unknown — it is not the request deadline" in {
      val client = messaging(email = _ =>
        Future.failed(new TimeoutException("synthetic transport timeout")))
      client.sendEmail("staff@example.com", "subj", "body").futureValue shouldBe
        TwilioSendVerdict.OutcomeUnknown("twilio email transport failure: TimeoutException")
    }

    "post the documented request shape through an SDK request" in {
      val captured = Promise[SdkRequest]()
      val client = messaging(email = { request =>
        captured.success(request)
        Future.successful(sdkResponse("{}", 202))
      })
      client.sendEmail("staff@example.com", "subj", "a <b> & \"c\"\nd").futureValue

      val request = captured.future.futureValue
      request.getMethod shouldBe HttpMethod.POST
      request.getUrl shouldBe "https://comms.twilio.com/v1/Emails"
      request.getContentType shouldBe EnumConstants.ContentType.JSON

      val payload = request.getBody.parseJson.asJsObject
      val content = payload.fields("content").asJsObject
      content.fields("text") shouldBe JsString("a <b> & \"c\"\nd")
      content.fields("html") shouldBe JsString("<p>a &lt;b&gt; &amp; &quot;c&quot;<br/>d</p>")
    }

    "send the real SDK request against a loopback server" in withEmailServer(
      _ => Future.successful(HttpResponse(
        status = StatusCodes.Accepted,
        entity = HttpEntity(ContentTypes.`application/json`, """{"operationId":"op-fixture"}""")))
    ) { (port, probe) =>
      val client = TwilioMessaging(loopbackConfig(port))
      client.sendEmail("staff@example.com", "subj", "body").futureValue shouldBe
        TwilioSendVerdict.Accepted(Some("op-fixture"))

      val captured = probe.expectMessageType[CapturedRequest]
      captured.method shouldBe "POST"
      captured.path shouldBe "/v1/Emails"
      val decoded = new String(
        Base64.getDecoder.decode(captured.auth.get.stripPrefix("Basic ")), UTF_8)
      decoded shouldBe "ACtest:token"
      captured.body.parseJson.asJsObject.fields("to") shouldBe
        JsArray(JsObject("address" -> JsString("staff@example.com")))
    }

    "not follow redirects" in withEmailServer(
      _ => Future.successful(HttpResponse(
        status = StatusCodes.Found,
        headers = Location(Uri("/elsewhere")) :: Nil))
    ) { (port, probe) =>
      val client = TwilioMessaging(loopbackConfig(port))
      client.sendEmail("staff@example.com", "subj", "body").futureValue shouldBe
        TwilioSendVerdict.Retryable("twilio email http 302", None)
      probe.expectMessageType[CapturedRequest]
      probe.expectNoMessage(300.millis)
    }

    "not retry a rate limit inside the SDK" in withEmailServer(
      _ => Future.successful(HttpResponse(status = StatusCodes.TooManyRequests))
    ) { (port, probe) =>
      val client = TwilioMessaging(loopbackConfig(port))
      client.sendEmail("staff@example.com", "subj", "body").futureValue shouldBe
        TwilioSendVerdict.Retryable("twilio email http 429", None)
      probe.expectMessageType[CapturedRequest]
      probe.expectNoMessage(300.millis)
    }

    "report unknown while the provider holds the response past the deadline" in withEmailServer(
      _ => Promise[HttpResponse]().future
    ) { (port, probe) =>
      val client = TwilioMessaging(loopbackConfig(port, timeout = 200.millis))
      client.sendEmail("staff@example.com", "subj", "body").futureValue shouldBe
        a[TwilioSendVerdict.OutcomeUnknown]
    }

    "retry when the request provably never left the process" in {
      val socket = new ServerSocket(0)
      val port = socket.getLocalPort
      socket.close()
      val client = TwilioMessaging(loopbackConfig(port))
      client.sendEmail("staff@example.com", "subj", "body").futureValue shouldBe
        a[TwilioSendVerdict.Retryable]
    }

    "join a trailing-slash baseUrl without doubling the slash" in {
      val captured = Promise[SdkRequest]()
      val trailing = config.copy(email = Some(TwilioEmailChannel(
        fromAddress = "noreply@example.com",
        fromName = "Alerts",
        baseUrl = "https://comms.twilio.com/"
      )))
      val client = messaging(
        config = trailing,
        email = { request =>
          captured.success(request)
          Future.successful(sdkResponse("{}", 202))
        }
      )
      client.sendEmail("staff@example.com", "subj", "body").futureValue
      captured.future.futureValue.getUrl shouldBe "https://comms.twilio.com/v1/Emails"
    }

    "fail construction on an invalid baseUrl" in {
      val bad = config.copy(email = Some(TwilioEmailChannel(
        fromAddress = "noreply@example.com",
        fromName = "Alerts",
        baseUrl = "http://exa mple"
      )))
      an[IllegalUriException] should be thrownBy messaging(config = bad)
    }

    "fail construction on a plain http baseUrl off loopback" in {
      val bad = config.copy(email = Some(TwilioEmailChannel(
        fromAddress = "noreply@example.com",
        fromName = "Alerts",
        baseUrl = "http://comms.example.com"
      )))
      an[IllegalArgumentException] should be thrownBy messaging(config = bad)
    }

    "fail construction on a baseUrl with no scheme or host" in {
      val bad = config.copy(email = Some(TwilioEmailChannel(
        fromAddress = "noreply@example.com",
        fromName = "Alerts",
        baseUrl = "comms.twilio.com"
      )))
      an[IllegalArgumentException] should be thrownBy messaging(config = bad)
    }

    "fail construction on a baseUrl carrying userinfo, path, query, or fragment" in {
      for (bad <- Seq(
        "https://user:pw@comms.example.com",
        "https://comms.example.com/v1",
        "https://comms.example.com?x=1",
        "https://comms.example.com#frag"
      )) {
        val cfg = config.copy(email = Some(TwilioEmailChannel(
          fromAddress = "noreply@example.com",
          fromName = "Alerts",
          baseUrl = bad
        )))
        an[IllegalArgumentException] should be thrownBy messaging(config = cfg)
      }
    }

    "fail construction on a plain http baseUrl to a non-loopback address" in {
      for (bad <- Seq("http://10.0.0.1", "http://localhost.example.com")) {
        val cfg = config.copy(email = Some(TwilioEmailChannel(
          fromAddress = "noreply@example.com",
          fromName = "Alerts",
          baseUrl = bad
        )))
        an[IllegalArgumentException] should be thrownBy messaging(config = cfg)
      }
    }

    "accept plain http baseUrls on loopback hosts" in {
      for (ok <- Seq("http://localhost:1", "http://127.0.0.1:1", "http://127.0.0.2:1", "http://[::1]:1")) {
        val cfg = config.copy(email = Some(TwilioEmailChannel(
          fromAddress = "noreply@example.com",
          fromName = "Alerts",
          baseUrl = ok
        )))
        noException should be thrownBy messaging(config = cfg)
      }
    }

    "accept an https baseUrl with a port and a trailing slash" in {
      for (ok <- Seq("https://comms.example.com", "https://comms.example.com:8443/")) {
        val cfg = config.copy(email = Some(TwilioEmailChannel(
          fromAddress = "noreply@example.com",
          fromName = "Alerts",
          baseUrl = ok
        )))
        noException should be thrownBy messaging(config = cfg)
      }
    }

    "fail fast when the email channel is unconfigured" in {
      val bare = new TwilioMessaging(
        config.copy(email = None),
        (_, _) => Future.failed(new UnsupportedOperationException),
        _ => Future.failed(new UnsupportedOperationException)
      )
      bare.sendEmail("staff@example.com", "subj", "body").failed.futureValue shouldBe a[IllegalStateException]
    }

    "produce a fail-fast sdk transport when the email channel is unconfigured" in {
      TwilioMessaging.sdkEmailTransport(config.copy(email = None))(new SdkRequest(HttpMethod.POST, "https://comms.twilio.com/v1/Emails"))
        .failed.futureValue shouldBe a[IllegalStateException]
    }
  }

  "TwilioMessaging.sdkHttpClient" should {
    "fail after system shutdown instead of reaching the network" in {
      val kit = ActorTestKit()
      val transport = TwilioMessaging.sdkHttpClient(config)(using kit.system)
      kit.shutdownTestKit()
      an[IllegalStateException] should be thrownBy
        transport.makeRequest(new SdkRequest(HttpMethod.GET, "http://127.0.0.1:1/"))
    }
  }

  "TwilioMessagingConfig" should {
    "reject a zero or negative requestTimeout" in {
      for (timeout <- Seq(0.seconds, (-1).seconds)) {
        an[IllegalArgumentException] should be thrownBy config.copy(requestTimeout = timeout)
      }
    }

    "reject a sub-millisecond requestTimeout" in {
      an[IllegalArgumentException] should be thrownBy config.copy(requestTimeout = 500.micros)
    }

    "reject non-positive and overflowing responseMaxBytes" in {
      for (limit <- Seq(0L, -1L, Int.MaxValue.toLong)) {
        an[IllegalArgumentException] should be thrownBy config.copy(responseMaxBytes = limit)
      }
    }

    "accept positive bounds" in {
      noException should be thrownBy
        config.copy(requestTimeout = 1.milli, responseMaxBytes = Int.MaxValue.toLong - 1)
    }

    "never render the auth token" in {
      val redacted = config.copy(authToken = "tok-9f2e7c")
      redacted.toString should not include "tok-9f2e7c"
      redacted.toString should include("authToken=<redacted>")
    }
  }
}
