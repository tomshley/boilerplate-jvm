package com.tomshley.boilerplate.jvm.transport

import com.typesafe.config.ConfigFactory
import org.apache.pekko.actor.CoordinatedShutdown
import org.apache.pekko.actor.testkit.typed.scaladsl.LogCapturing
import org.apache.pekko.http.scaladsl.{ConnectionContext, Http}
import org.apache.pekko.http.scaladsl.model.HttpResponse
import org.apache.pekko.http.scaladsl.server.Directives.{complete, extractRequest, get, path}
import org.apache.pekko.http.scaladsl.server.Route
import org.scalatest.concurrent.PatienceConfiguration.Timeout
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{Millis, Seconds, Span}
import org.scalatest.wordspec.AnyWordSpec

import java.net.URI
import java.net.http.{HttpClient as JdkHttpClient, HttpRequest as JdkHttpRequest, HttpResponse as JdkHttpResponse, HttpTimeoutException}
import java.time.{Duration, Instant}
import javax.net.ssl.{SSLContext, SSLHandshakeException}
import scala.concurrent.Promise
import scala.jdk.FutureConverters.*
import scala.util.{Try, Using}

final class WebServerBoilerplateSpec
    extends AnyWordSpec
    with Matchers
    with ScalaFutures
    with LogCapturing
    with TransportLifecycleChecks {

  override given patienceConfig: PatienceConfig =
    PatienceConfig(timeout = Span(15, Seconds), interval = Span(100, Millis))

  private val routes: Seq[Route] = Seq(
    get {
      path("ready") {
        complete("ready")
      }
    }
  )

  private val transport = WebServerBoilerplate

  private def request(
      scheme: String,
      host: String,
      port: Int,
      requestPath: String,
      context: SSLContext
  ): Try[JdkHttpResponse[String]] = Try {
    val client = JdkHttpClient
      .newBuilder()
      .sslContext(context)
      .version(JdkHttpClient.Version.HTTP_1_1)
      .connectTimeout(Duration.ofSeconds(5))
      .build()
    Using.resource(client) { c =>
      val request = JdkHttpRequest
        .newBuilder()
        .uri(URI.create(s"$scheme://$host:$port$requestPath"))
        .timeout(Duration.ofSeconds(5))
        .GET()
        .build()
      c.sendAsync(request, JdkHttpResponse.BodyHandlers.ofString())
        .asScala
        .futureValue
    }
  }

  "WebServerBoilerplate" should {
    "serve the heartbeat and supplied routes over plaintext" in
      withKit("web-plaintext") { kit =>
        val binding = WebServerBoilerplate
          .start(
            interface = "127.0.0.1",
            port = 0,
            system = kit.system,
            routes = routes
          )
          .futureValue
        val port = binding.localAddress.getPort
        val heartbeat =
          request("http", "127.0.0.1", port, "/heartbeat", SSLContext.getDefault).get
        heartbeat.statusCode() shouldBe 200
        heartbeat.version() shouldBe JdkHttpClient.Version.HTTP_1_1
        noException should be thrownBy Instant.parse(heartbeat.body())
        val ready =
          request("http", "127.0.0.1", port, "/ready", SSLContext.getDefault).get
        ready.statusCode() shouldBe 200
        ready.body() shouldBe "ready"
      }

    "serve the heartbeat and supplied routes over TLS" in
      withKit("web-tls") { kit =>
        TransportTlsFixture.withContexts { (serverContext, clientContext) =>
          val binding = WebServerBoilerplate
            .start(
              interface = "127.0.0.1",
              port = 0,
              system = kit.system,
              routes = routes,
              httpsContext = ConnectionContext.httpsServer(serverContext)
            )
            .futureValue
          val port = binding.localAddress.getPort
          val heartbeat =
            request("https", "127.0.0.1", port, "/heartbeat", clientContext).get
          heartbeat.statusCode() shouldBe 200
          heartbeat.version() shouldBe JdkHttpClient.Version.HTTP_1_1
          noException should be thrownBy Instant.parse(heartbeat.body())
          val ready =
            request("https", "127.0.0.1", port, "/ready", clientContext).get
          ready.statusCode() shouldBe 200
          ready.body() shouldBe "ready"
        }
      }

    "reject plaintext requests on the TLS listener" in
      withKit("web-tls-plaintext") { kit =>
        TransportTlsFixture.withContexts { (serverContext, _) =>
          val binding = transport
            .start(
              "127.0.0.1",
              0,
              kit.system,
              routes,
              ConnectionContext.httpsServer(serverContext)
            )
            .futureValue
          val failure = request(
            "http",
            "127.0.0.1",
            binding.localAddress.getPort,
            "/heartbeat",
            SSLContext.getDefault
          ).failed.get
          causes(failure).exists(_.isInstanceOf[java.io.IOException]) shouldBe true
          causes(failure).exists(_.isInstanceOf[HttpTimeoutException]) shouldBe false
        }
      }

    "reject an untrusted server certificate" in
      withKit("web-tls-untrusted") { kit =>
        TransportTlsFixture.withContexts { (serverContext, _) =>
          val binding = transport
            .start(
              "127.0.0.1",
              0,
              kit.system,
              routes,
              ConnectionContext.httpsServer(serverContext)
            )
            .futureValue
          val failure = request(
            "https",
            "127.0.0.1",
            binding.localAddress.getPort,
            "/heartbeat",
            SSLContext.getDefault
          ).failed.get
          causes(failure).exists(_.isInstanceOf[SSLHandshakeException]) shouldBe true
        }
      }

    "reject a server hostname mismatch" in
      withKit("web-tls-hostname") { kit =>
        TransportTlsFixture.withContexts { (serverContext, clientContext) =>
          val binding = transport
            .start(
              "127.0.0.1",
              0,
              kit.system,
              routes,
              ConnectionContext.httpsServer(serverContext)
            )
            .futureValue
          val failure = request(
            "https",
            "localhost",
            binding.localAddress.getPort,
            "/heartbeat",
            clientContext
          ).failed.get
          causes(failure).exists(_.isInstanceOf[SSLHandshakeException]) shouldBe true
        }
      }

    "honor the configured shutdown deadline for in-flight requests" in
      withKit(
        "web-deadline",
        ConfigFactory.parseString(
          "tomshley-boilerplate-transport.http.shutdown-timeout = 100 ms"
        )
      ) { kit =>
        val invoked = kit.createTestProbe[Unit]()
        val pending = Promise[HttpResponse]()
        val pendingRoutes = routes ++ Seq(
          get {
            path("pending") {
              extractRequest { _ =>
                invoked.ref ! ()
                complete(pending.future)
              }
            }
          }
        )
        val binding = transport
          .start("127.0.0.1", 0, kit.system, pendingRoutes)
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
            .uri(
              URI.create(s"http://127.0.0.1:${binding.localAddress.getPort}/pending")
            )
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
          pending.trySuccess(HttpResponse())
          client.close()
        }
      }

    serverLifecycleCases[Http.ServerBinding](
      "HTTP-plaintext",
      (system, port) => transport.start("127.0.0.1", port, system, routes),
      _.localAddress,
      (_, binding) => binding.whenTerminated
    )

    serverLifecycleCases[Http.ServerBinding](
      "HTTP-TLS",
      (system, port) =>
        TransportTlsFixture.withContexts { (serverContext, _) =>
          transport.start(
            "127.0.0.1",
            port,
            system,
            routes,
            ConnectionContext.httpsServer(serverContext)
          )
        },
      _.localAddress,
      (_, binding) => binding.whenTerminated
    )
  }
}
