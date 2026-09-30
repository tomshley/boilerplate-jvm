package com.tomshley.boilerplate.jvm.transport

import com.typesafe.config.ConfigFactory
import org.apache.pekko.actor.CoordinatedShutdown
import org.apache.pekko.actor.typed.ActorSystem
import org.apache.pekko.actor.testkit.typed.scaladsl.LogCapturing
import org.apache.pekko.http.scaladsl.{ConnectionContext, Http, model}
import org.scalatest.concurrent.PatienceConfiguration.Timeout
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{Millis, Seconds, Span}
import org.scalatest.wordspec.AnyWordSpec

import java.net.URI
import java.net.http.{HttpClient as JdkHttpClient, HttpRequest as JdkHttpRequest, HttpResponse as JdkHttpResponse, HttpTimeoutException}
import java.time.Duration
import javax.net.ssl.{SSLContext, SSLHandshakeException}
import scala.concurrent.{Future, Promise}
import scala.jdk.FutureConverters.*
import scala.util.{Try, Using}

final class GrpcServerBoilerplateSpec
    extends AnyWordSpec
    with Matchers
    with ScalaFutures
    with LogCapturing
    with TransportLifecycleChecks {

  override given patienceConfig: PatienceConfig =
    PatienceConfig(timeout = Span(15, Seconds), interval = Span(100, Millis))

  private val service: model.HttpRequest => Future[model.HttpResponse] =
    _ => Future.successful(model.HttpResponse(entity = "ready"))

  private val transport = GrpcServerBoilerplate

  private def request(
      scheme: String,
      host: String,
      port: Int,
      context: SSLContext,
      version: JdkHttpClient.Version = JdkHttpClient.Version.HTTP_2
  ): Try[JdkHttpResponse[String]] = Try {
    val client = JdkHttpClient
      .newBuilder()
      .sslContext(context)
      .version(version)
      .connectTimeout(Duration.ofSeconds(5))
      .build()
    Using.resource(client) { c =>
      val request = JdkHttpRequest
        .newBuilder()
        .uri(URI.create(s"$scheme://$host:$port/"))
        .timeout(Duration.ofSeconds(5))
        .GET()
        .build()
      c.sendAsync(request, JdkHttpResponse.BodyHandlers.ofString())
        .asScala
        .futureValue
    }
  }

  "GrpcServerBoilerplate" should {
    "preserve plaintext serving through the original overload" in
      withKit("grpc-plaintext") { kit =>
        val binding = GrpcServerBoilerplate
          .start(
            interface = "127.0.0.1",
            port = 0,
            system = kit.system,
            service = service
          )
          .futureValue
        val response = request(
          "http",
          "127.0.0.1",
          binding.localAddress.getPort,
          SSLContext.getDefault
        ).get
        response.statusCode() shouldBe 200
        response.body() shouldBe "ready"
      }

    "serve HTTP2 over TLS with the supplied context" in
      withKit("grpc-tls-http2") { kit =>
        TransportTlsFixture.withContexts { (serverContext, clientContext) =>
          val binding = GrpcServerBoilerplate
            .start(
              interface = "127.0.0.1",
              port = 0,
              system = kit.system,
              service = service,
              httpsContext = ConnectionContext.httpsServer(serverContext)
            )
            .futureValue
          val response = request(
            "https",
            "127.0.0.1",
            binding.localAddress.getPort,
            clientContext
          ).get
          response.statusCode() shouldBe 200
          response.body() shouldBe "ready"
          response.version() shouldBe JdkHttpClient.Version.HTTP_2
        }
      }

    "reject plaintext requests on the TLS listener without invoking the service" in
      withKit("grpc-tls-plaintext") { kit =>
        TransportTlsFixture.withContexts { (serverContext, _) =>
          val probe = kit.createTestProbe[model.HttpRequest]()
          val recording: model.HttpRequest => Future[model.HttpResponse] =
            request => {
              probe.ref ! request
              service(request)
            }
          val binding = transport
            .start(
              "127.0.0.1",
              0,
              kit.system,
              recording,
              ConnectionContext.httpsServer(serverContext)
            )
            .futureValue
          val failure = request(
            "http",
            "127.0.0.1",
            binding.localAddress.getPort,
            SSLContext.getDefault
          ).failed.get
          causes(failure).exists(_.isInstanceOf[java.io.IOException]) shouldBe true
          causes(failure).exists(_.isInstanceOf[HttpTimeoutException]) shouldBe false
          probe.expectNoMessage()
        }
      }

    "reject an untrusted server certificate" in
      withKit("grpc-tls-untrusted") { kit =>
        TransportTlsFixture.withContexts { (serverContext, _) =>
          val binding = transport
            .start(
              "127.0.0.1",
              0,
              kit.system,
              service,
              ConnectionContext.httpsServer(serverContext)
            )
            .futureValue
          val failure = request(
            "https",
            "127.0.0.1",
            binding.localAddress.getPort,
            SSLContext.getDefault
          ).failed.get
          causes(failure).exists(_.isInstanceOf[SSLHandshakeException]) shouldBe true
        }
      }

    "reject a server hostname mismatch" in
      withKit("grpc-tls-hostname") { kit =>
        TransportTlsFixture.withContexts { (serverContext, clientContext) =>
          val binding = transport
            .start(
              "127.0.0.1",
              0,
              kit.system,
              service,
              ConnectionContext.httpsServer(serverContext)
            )
            .futureValue
          val failure = request(
            "https",
            "localhost",
            binding.localAddress.getPort,
            clientContext
          ).failed.get
          causes(failure).exists(_.isInstanceOf[SSLHandshakeException]) shouldBe true
        }
      }

    "negotiate HTTP/1.1 on TLS when HTTP2 is disabled for the system" in
      withKit(
        "grpc-tls-http1",
        ConfigFactory.parseString("pekko.http.server.enable-http2 = off")
      ) { kit =>
        TransportTlsFixture.withContexts { (serverContext, clientContext) =>
          val binding = transport
            .start(
              "127.0.0.1",
              0,
              kit.system,
              service,
              ConnectionContext.httpsServer(serverContext)
            )
            .futureValue
          val response = request(
            "https",
            "127.0.0.1",
            binding.localAddress.getPort,
            clientContext
          ).get
          response.statusCode() shouldBe 200
          response.body() shouldBe "ready"
          response.version() shouldBe JdkHttpClient.Version.HTTP_1_1
        }
      }

    "enforce caller-configured client authentication" in
      withKit("grpc-tls-mtls") { kit =>
        TransportTlsFixture.withMutualContexts {
          (serverContext, clientContext, authenticatedContext) =>
            val httpsContext = ConnectionContext.httpsServer(() =>
              TransportTlsFixture.serverEngine(serverContext, needClientAuth = true)
            )
            val binding = transport
              .start("127.0.0.1", 0, kit.system, service, httpsContext)
              .futureValue
            val rejected = request(
              "https",
              "127.0.0.1",
              binding.localAddress.getPort,
              clientContext
            ).failed.get
            causes(rejected).exists(_.isInstanceOf[SSLHandshakeException]) shouldBe true
            val response = request(
              "https",
              "127.0.0.1",
              binding.localAddress.getPort,
              authenticatedContext
            ).get
            response.statusCode() shouldBe 200
            response.body() shouldBe "ready"
        }
      }

    "honor the configured shutdown deadline for in-flight requests" in
      withKit(
        "grpc-deadline",
        ConfigFactory.parseString(
          "tomshley-boilerplate-transport.http.shutdown-timeout = 100 ms"
        )
      ) { kit =>
        val invoked = kit.createTestProbe[Unit]()
        val pending = Promise[model.HttpResponse]()
        val blocking: model.HttpRequest => Future[model.HttpResponse] =
          _ => {
            invoked.ref ! ()
            pending.future
          }
        val binding = transport
          .start("127.0.0.1", 0, kit.system, blocking)
          .futureValue
        val client = JdkHttpClient
          .newBuilder()
          .sslContext(SSLContext.getDefault)
          .version(JdkHttpClient.Version.HTTP_1_1)
          .connectTimeout(Duration.ofSeconds(5))
          .build()
        try {
          val httpRequest = JdkHttpRequest
            .newBuilder()
            .uri(URI.create(s"http://127.0.0.1:${binding.localAddress.getPort}/"))
            .timeout(Duration.ofSeconds(5))
            .GET()
            .build()
          val response = client
            .sendAsync(httpRequest, JdkHttpResponse.BodyHandlers.ofString())
            .asScala
          invoked.expectMessage(())
          val shutdown = CoordinatedShutdown(kit.system)
            .run(CoordinatedShutdown.UnknownReason)
          binding.whenTerminated.futureValue(Timeout(Span(2, Seconds)))
          val outcome = response
            .transform(result => scala.util.Success(result))(scala.concurrent.ExecutionContext.parasitic)
            .futureValue
          outcome match {
            case scala.util.Success(reply) => reply.statusCode() shouldBe 503
            case scala.util.Failure(failure) =>
              causes(failure).exists(_.isInstanceOf[java.io.IOException]) shouldBe true
              causes(failure).exists(_.isInstanceOf[HttpTimeoutException]) shouldBe false
          }
          shutdown.futureValue
          kit.system.whenTerminated.futureValue
        } finally {
          pending.trySuccess(model.HttpResponse())
          client.close()
        }
      }

    serverLifecycleCases[Http.ServerBinding](
      "gRPC-plaintext",
      (system, port) => transport.start("127.0.0.1", port, system, service),
      _.localAddress,
      (_, binding) => binding.whenTerminated
    )

    serverLifecycleCases[Http.ServerBinding](
      "gRPC-TLS",
      (system, port) =>
        TransportTlsFixture.withContexts { (serverContext, _) =>
          transport.start(
            "127.0.0.1",
            port,
            system,
            service,
            ConnectionContext.httpsServer(serverContext)
          )
        },
      _.localAddress,
      (_, binding) => binding.whenTerminated
    )
  }
}
