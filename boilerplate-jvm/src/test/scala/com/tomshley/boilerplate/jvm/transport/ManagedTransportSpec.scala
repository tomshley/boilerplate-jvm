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

package com.tomshley.boilerplate.jvm.transport

import com.tomshley.boilerplate.jvm.managedmain.{
  ActorSystemMain,
  ManagedSetup,
  Startup,
  StartupTask
}
import com.typesafe.config.ConfigFactory
import java.net.Socket
import java.net.http.HttpClient.Version
import java.net.http.{HttpClient, HttpRequest, HttpResponse}
import java.nio.charset.StandardCharsets
import javax.net.ssl.{SSLContext, SSLSocket}
import org.apache.pekko.Done
import org.apache.pekko.actor.setup.ActorSystemSetup
import org.apache.pekko.actor.{BootstrapSetup, CoordinatedShutdown}
import org.apache.pekko.actor.testkit.typed.scaladsl.{LogCapturing, TestProbe}
import org.apache.pekko.actor.typed.ActorSystem
import org.apache.pekko.stream.scaladsl.Flow
import org.apache.pekko.http.scaladsl.{ConnectionContext, model}
import org.apache.pekko.http.scaladsl.server.Route
import org.apache.pekko.http.scaladsl.server.Directives.{complete, path}
import org.apache.pekko.util.ByteString
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.exceptions.TestFailedException
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{Millis, Seconds, Span}
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.concurrent.duration.*
import scala.util.{Failure, Success, Try, Using}

final class ManagedTransportSpec
    extends AnyWordSpec
    with Matchers
    with ScalaFutures
    with LogCapturing
    with TransportLifecycleChecks {

  override given patienceConfig: PatienceConfig =
    PatienceConfig(timeout = Span(15, Seconds), interval = Span(100, Millis))

  private val ioTimeoutMillis = 5000
  private val quietWindow = 300.millis

  private val managedConfig =
    ConfigFactory.parseString(
      """
      pekko.actor.provider = local
      pekko.coordinated-shutdown.run-by-jvm-shutdown-hook = off
      pekko.http.server.enable-http2 = on
      """
    )

  private def managedSetup(managed: ManagedSetup): ActorSystemSetup =
    ActorSystemSetup(
      BootstrapSetup(managedConfig.withFallback(ConfigFactory.load())),
      managed
    )

  private val webRoutes: Seq[Route] = Seq(path("web") {
    complete("web")
  })

  private val grpcService: model.HttpRequest => Future[model.HttpResponse] =
    _ => Future.successful(model.HttpResponse(entity = "ready"))

  private final case class EchoState(received: Int) extends TcpConnectionState

  private val echoHandler: TcpServerHandlerBoilerplate =
    new TcpServerHandlerBoilerplate {
      override type State = EchoState
      override def initialState: EchoState = EchoState(0)
      override def onMessage(msg: ByteString, state: EchoState)(
          using ActorSystem[?]
      ): Future[(EchoState, ByteString)] =
        Future.successful(
          (EchoState(state.received + 1), ByteString(s"${state.received + 1}:${msg.utf8String}"))
        )
    }

  private def probingHandler(
      inner: TcpServerHandlerBoilerplate,
      probe: TestProbe[ByteString]
  ): TcpServerHandlerBoilerplate =
    new TcpServerHandlerBoilerplate {
      override type State = inner.State
      override def initialState: inner.State = inner.initialState
      override def framing: Flow[ByteString, ByteString, ?] = inner.framing
      override def outboundFraming(response: ByteString): ByteString =
        inner.outboundFraming(response)
      override def onMessage(msg: ByteString, state: inner.State)(
          using system: ActorSystem[?]
      ): Future[(inner.State, ByteString)] = {
        probe.ref ! msg
        inner.onMessage(msg, state)
      }
      override def onConnectionClosed(
          state: inner.State,
          cause: Option[Throwable]
      )(using system: ActorSystem[?]): Unit =
        inner.onConnectionClosed(state, cause)
    }

  private def connectTls(port: Int, context: SSLContext): SSLSocket = {
    val socket = context.getSocketFactory.createSocket().asInstanceOf[SSLSocket]
    try {
      socket.connect(
        new java.net.InetSocketAddress("127.0.0.1", port),
        ioTimeoutMillis
      )
      socket.setSoTimeout(ioTimeoutMillis)
      val parameters = socket.getSSLParameters
      parameters.setEndpointIdentificationAlgorithm("HTTPS")
      socket.setSSLParameters(parameters)
      socket.startHandshake()
      socket
    } catch {
      case scala.util.control.NonFatal(e) =>
        Try(socket.close())
        throw e
    }
  }

  private def connectPlain(port: Int): Socket = {
    val socket = new Socket()
    try {
      socket.connect(
        new java.net.InetSocketAddress("127.0.0.1", port),
        ioTimeoutMillis
      )
      socket.setSoTimeout(ioTimeoutMillis)
      socket
    } catch {
      case scala.util.control.NonFatal(e) =>
        Try(socket.close())
        throw e
    }
  }

  private def exchange(socket: Socket, message: ByteString): String = {
    val reader = new java.io.BufferedReader(
      new java.io.InputStreamReader(socket.getInputStream, java.nio.charset.StandardCharsets.UTF_8)
    )
    socket.getOutputStream.write((message ++ ByteString("\n")).toArray)
    socket.getOutputStream.flush()
    reader.readLine()
  }

  private def request(
      scheme: String,
      port: Int,
      path: String,
      context: Option[SSLContext],
      version: Option[Version] = None
  ): Try[HttpResponse[String]] =
    Try {
      Using.resource {
        val builder = HttpClient.newBuilder()
          .connectTimeout(java.time.Duration.ofMillis(ioTimeoutMillis))
        val withContext = context.fold(builder)(builder.sslContext)
        version.fold(withContext)(withContext.version).build()
      } { client =>
        client.send(
          HttpRequest.newBuilder()
            .uri(java.net.URI.create(s"$scheme://127.0.0.1:$port$path"))
            .timeout(java.time.Duration.ofMillis(ioTimeoutMillis))
            .GET()
            .build(),
          HttpResponse.BodyHandlers.ofString()
        )
      }
    }

  private def expectTlsFailure[T](attempt: Try[T]): Unit = {
    attempt.isFailure shouldBe true
    causes(attempt.failed.get).exists(
      _.isInstanceOf[javax.net.ssl.SSLException]
    ) shouldBe true
  }

  private def expectHttpConnectionDropped(
      attempt: Try[HttpResponse[String]]
  ): Unit = {
    attempt.isFailure shouldBe true
    causes(attempt.failed.get).exists(
      _.isInstanceOf[java.io.IOException]
    ) shouldBe true
    causes(attempt.failed.get).exists(
      _.isInstanceOf[java.net.http.HttpTimeoutException]
    ) shouldBe false
    causes(attempt.failed.get).exists(
      _.isInstanceOf[java.net.SocketTimeoutException]
    ) shouldBe false
  }

  private def expectTcpConnectionDropped(attempt: Try[Int]): Unit =
    attempt match {
      case Success(-1) => ()
      case Success(bytes) =>
        fail(s"expected a dropped connection, read $bytes")
      case Failure(exception) =>
        causes(exception).exists(
          _.isInstanceOf[java.io.IOException]
        ) shouldBe true
        causes(exception).exists(
          _.isInstanceOf[java.net.http.HttpTimeoutException]
        ) shouldBe false
        causes(exception).exists(
          _.isInstanceOf[java.net.SocketTimeoutException]
        ) shouldBe false
    }

  private def droppedAfterPlaintextWrite(port: Int): Try[Int] =
    Try(Using.resource(connectPlain(port)) { socket =>
      socket.getOutputStream.write("hello\n".getBytes(StandardCharsets.UTF_8))
      socket.getOutputStream.flush()
      socket.getInputStream.read()
    })

  private def strictText(entity: model.HttpEntity): String =
    entity match {
      case model.HttpEntity.Strict(_, data) => data.utf8String
      case _ => fail("expected strict fixture entity")
    }

  private def stop(system: ActorSystem[?]): Unit = {
    system.terminate()
    system.whenTerminated.futureValue
  }

  private def startAll(prefix: String, bound: TestProbe[(String, Int)])(
      system: ActorSystem[?]
  ): Startup = {
    given ExecutionContext = system.executionContext
    WebServerBoilerplate.start("127.0.0.1", 0, system, webRoutes)
      .foreach(binding => bound.ref ! (s"$prefix-web" -> binding.localAddress.getPort))
    GrpcServerBoilerplate.start("127.0.0.1", 0, system, grpcService)
      .foreach(binding => bound.ref ! (s"$prefix-grpc" -> binding.localAddress.getPort))
    TcpServerBoilerplate.start("127.0.0.1", 0, system, echoHandler)
      .foreach(binding => bound.ref ! (s"$prefix-tcp" -> binding.localAddress.getPort))
    Startup.Ready
  }

  "ManagedSetup in a managed system" should {
    "apply managed TLS defaults and integration transforms to all transports" in
      withKit("managed-tls-all") { kit =>
        TransportTlsFixture.withContexts { (serverContext, clientContext) =>
          val systems = kit.createTestProbe[ActorSystem[?]]()
          val bound = kit.createTestProbe[(String, Int)]()
          val httpRequests = kit.createTestProbe[model.HttpRequest]()
          val tcpMessages = kit.createTestProbe[ByteString]()
          val released = kit.createTestProbe[String]()
          val gate = Promise[Done]()
          val untrusted = SSLContext.getDefault

          val telemetry = ManagedSetup(
            startup = Vector(StartupTask[Done](
              "telemetry",
              system => {
                systems.ref ! system
                gate.future
              },
              (_, _) => {
                released.ref ! "telemetry"
                Future.successful(Done)
              }
            )),
            http = setup => setup.copy(service = request => {
              httpRequests.ref ! request
              setup.service(request)
            }),
            tcp = setup => setup.copy(handler = probingHandler(setup.handler, tcpMessages))
          )
          val setup = ManagedSetup
            .tls(
              ConnectionContext.httpsServer(serverContext),
              () => TransportTlsFixture.serverEngine(serverContext)
            )
            .andThen(telemetry)

          val managed = ActorSystemMain("managed-tls", managedSetup(setup), system => {
            given ExecutionContext = system.executionContext
            WebServerBoilerplate.start("127.0.0.1", 0, system, webRoutes)
              .foreach(binding => bound.ref ! ("web" -> binding.localAddress.getPort))
            GrpcServerBoilerplate.start("127.0.0.1", 0, system, grpcService)
              .foreach(binding => bound.ref ! ("grpc" -> binding.localAddress.getPort))
            TcpServerBoilerplate.start("127.0.0.1", 0, system, echoHandler)
              .foreach(binding => bound.ref ! ("tcp" -> binding.localAddress.getPort))
            Startup.Ready
          })

          try {
            systems.receiveMessage()
            bound.expectNoMessage(quietWindow)
            gate.success(Done)
            val ports = bound.receiveMessages(3).toMap

            request("https", ports("web"), "/heartbeat", Some(clientContext)) match {
              case Success(response) => response.statusCode() shouldBe 200
              case Failure(exception) => fail("TLS web request failed", exception)
            }
            request("https", ports("grpc"), "/", Some(clientContext), Some(Version.HTTP_2)) match {
              case Success(response) =>
                response.statusCode() shouldBe 200
                response.version() shouldBe Version.HTTP_2
              case Failure(exception) => fail("TLS gRPC request failed", exception)
            }
            Using.resource(connectTls(ports("tcp"), clientContext)) { socket =>
              exchange(socket, ByteString("ping")) shouldBe "1:ping"
            }
            httpRequests.receiveMessages(2).map(_.uri.path.toString).toSet shouldBe
              Set("/heartbeat", "/")
            tcpMessages.expectMessage(ByteString("ping"))

            expectHttpConnectionDropped(request("http", ports("web"), "/heartbeat", None))
            expectHttpConnectionDropped(request("http", ports("grpc"), "/", None))
            expectTcpConnectionDropped(droppedAfterPlaintextWrite(ports("tcp")))
            expectTlsFailure(request("https", ports("web"), "/heartbeat", Some(untrusted)))
            expectTlsFailure(request("https", ports("grpc"), "/", Some(untrusted)))
            expectTlsFailure(
              Try(Using.resource(connectTls(ports("tcp"), untrusted))(_ => ()))
            )
            httpRequests.expectNoMessage(quietWindow)
            tcpMessages.expectNoMessage(quietWindow)
          } finally {
            gate.trySuccess(Done)
            stop(managed)
          }
          released.receiveMessages(1) shouldBe List("telemetry")
        }
      }

    "honor explicit per-start plaintext exceptions while siblings keep managed TLS" in
      withKit("managed-exceptions") { kit =>
        TransportTlsFixture.withContexts { (serverContext, clientContext) =>
          val bound = kit.createTestProbe[(String, Int)]()
          val systems = kit.createTestProbe[ActorSystem[?]]()
          val httpRequests = kit.createTestProbe[model.HttpRequest]()
          val tcpMessages = kit.createTestProbe[ByteString]()
          val setup = ManagedSetup
            .tls(
              ConnectionContext.httpsServer(serverContext),
              () => TransportTlsFixture.serverEngine(serverContext)
            )
            .andThen(ManagedSetup(
              http = setup =>
                setup.copy(service = request => {
                  httpRequests.ref ! request
                  setup.service(request)
                }),
              tcp = setup => setup.copy(handler = probingHandler(setup.handler, tcpMessages))
            ))

          val managed = ActorSystemMain("managed-mixed", managedSetup(setup), system => {
            systems.ref ! system
            given ExecutionContext = system.executionContext
            WebServerBoilerplate.start("127.0.0.1", 0, system, webRoutes)
              .foreach(binding => bound.ref ! ("web-tls" -> binding.localAddress.getPort))
            WebServerBoilerplate.start(
              "127.0.0.1",
              0,
              system,
              webRoutes,
              configure = _.copy(httpsContext = None)
            ).foreach(binding => bound.ref ! ("web-plain" -> binding.localAddress.getPort))
            GrpcServerBoilerplate.start("127.0.0.1", 0, system, grpcService)
              .foreach(binding => bound.ref ! ("grpc-tls" -> binding.localAddress.getPort))
            GrpcServerBoilerplate.start(
              "127.0.0.1",
              0,
              system,
              grpcService,
              configure = _.copy(httpsContext = None)
            ).foreach(binding => bound.ref ! ("grpc-plain" -> binding.localAddress.getPort))
            TcpServerBoilerplate.start("127.0.0.1", 0, system, echoHandler)
              .foreach(binding => bound.ref ! ("tcp-tls" -> binding.localAddress.getPort))
            TcpServerBoilerplate.start(
              "127.0.0.1",
              0,
              system,
              echoHandler,
              configure = _.copy(createSSLEngine = None)
            ).foreach(binding => bound.ref ! ("tcp-plain" -> binding.localAddress.getPort))
            Startup.Ready
          })

          try {
            systems.receiveMessage()
            val ports = bound.receiveMessages(6).toMap

            request("https", ports("web-tls"), "/heartbeat", Some(clientContext)) match {
              case Success(response) => response.statusCode() shouldBe 200
              case Failure(exception) => fail("TLS web request failed", exception)
            }
            request("http", ports("web-plain"), "/heartbeat", None) match {
              case Success(response) => response.statusCode() shouldBe 200
              case Failure(exception) => fail("plaintext web request failed", exception)
            }
            request("https", ports("grpc-tls"), "/", Some(clientContext)) match {
              case Success(response) => response.statusCode() shouldBe 200
              case Failure(exception) => fail("TLS gRPC request failed", exception)
            }
            request("http", ports("grpc-plain"), "/", None) match {
              case Success(response) => response.statusCode() shouldBe 200
              case Failure(exception) => fail("plaintext gRPC request failed", exception)
            }
            Using.resource(connectTls(ports("tcp-tls"), clientContext)) { socket =>
              exchange(socket, ByteString("ping")) shouldBe "1:ping"
            }
            Using.resource(connectPlain(ports("tcp-plain"))) { socket =>
              exchange(socket, ByteString("ping")) shouldBe "1:ping"
            }

            httpRequests.receiveMessages(4)
            tcpMessages.receiveMessages(2) shouldBe
              List(ByteString("ping"), ByteString("ping"))

            expectHttpConnectionDropped(request("http", ports("web-tls"), "/heartbeat", None))
            expectTlsFailure(request("https", ports("web-plain"), "/heartbeat", Some(clientContext)))
            expectTcpConnectionDropped(droppedAfterPlaintextWrite(ports("tcp-tls")))
            expectTlsFailure(
              Try(Using.resource(connectTls(ports("tcp-plain"), clientContext))(_ => ()))
            )
            httpRequests.expectNoMessage(quietWindow)
          } finally stop(managed)
        }
      }

    "replace the managed TLS context when a start configures its own" in
      withKit("managed-replace") { kit =>
        TransportTlsFixture.withContexts { (serverContext, clientContext) =>
          TransportTlsFixture.withContexts { (localServerContext, localClientContext) =>
            val bound = kit.createTestProbe[(String, Int)]()
            val systems = kit.createTestProbe[ActorSystem[?]]()
            val setup = ManagedSetup.tls(
              ConnectionContext.httpsServer(serverContext),
              () => TransportTlsFixture.serverEngine(serverContext)
            )
            val managed = ActorSystemMain("managed-replace", managedSetup(setup), system => {
              systems.ref ! system
              given ExecutionContext = system.executionContext
              WebServerBoilerplate.start("127.0.0.1", 0, system, webRoutes)
                .foreach(binding => bound.ref ! ("default" -> binding.localAddress.getPort))
              WebServerBoilerplate.start(
                "127.0.0.1",
                0,
                system,
                webRoutes,
                configure = _.copy(httpsContext =
                  Some(ConnectionContext.httpsServer(localServerContext))
                )
              ).foreach(binding => bound.ref ! ("local" -> binding.localAddress.getPort))
              Startup.Ready
            })

            try {
              val ports = bound.receiveMessages(2).toMap
              request("https", ports("default"), "/heartbeat", Some(clientContext)) match {
                case Success(response) => response.statusCode() shouldBe 200
                case Failure(exception) => fail("default-context request failed", exception)
              }
              request("https", ports("local"), "/heartbeat", Some(localClientContext)) match {
                case Success(response) => response.statusCode() shouldBe 200
                case Failure(exception) => fail("local-context request failed", exception)
              }
              expectTlsFailure(
                request("https", ports("local"), "/heartbeat", Some(clientContext))
              )
              expectTlsFailure(
                request("https", ports("default"), "/heartbeat", Some(localClientContext))
              )
            } finally stop(managed)
          }
        }
      }

    "scope managed TLS defaults to the configured system only" in
      withKit("managed-scope") { kit =>
        TransportTlsFixture.withContexts { (serverContext, clientContext) =>
          val bound = kit.createTestProbe[(String, Int)]()
          val tlsSystem = ActorSystemMain(
            "tls-scope-svc",
            managedSetup(ManagedSetup.tls(
              ConnectionContext.httpsServer(serverContext),
              () => TransportTlsFixture.serverEngine(serverContext)
            )),
            startAll("tls", bound)
          )
          val plainSystem = ActorSystemMain(
            "plain-scope-svc",
            ActorSystemSetup(
              BootstrapSetup(managedConfig.withFallback(ConfigFactory.load()))
            ),
            startAll("plain", bound)
          )
          try {
            val ports = bound.receiveMessages(6).toMap
            request("https", ports("tls-web"), "/heartbeat", Some(clientContext)) match {
              case Success(response) => response.statusCode() shouldBe 200
              case Failure(exception) => fail("managed TLS web request failed", exception)
            }
            request("https", ports("tls-grpc"), "/", Some(clientContext)) match {
              case Success(response) => response.statusCode() shouldBe 200
              case Failure(exception) => fail("managed TLS gRPC request failed", exception)
            }
            Using.resource(connectTls(ports("tls-tcp"), clientContext)) { socket =>
              exchange(socket, ByteString("ping")) shouldBe "1:ping"
            }
            request("http", ports("plain-web"), "/heartbeat", None) match {
              case Success(response) => response.statusCode() shouldBe 200
              case Failure(exception) => fail("unmanaged web request failed", exception)
            }
            request("http", ports("plain-grpc"), "/", None) match {
              case Success(response) => response.statusCode() shouldBe 200
              case Failure(exception) => fail("unmanaged gRPC request failed", exception)
            }
            Using.resource(connectPlain(ports("plain-tcp"))) { socket =>
              exchange(socket, ByteString("ping")) shouldBe "1:ping"
            }
            expectHttpConnectionDropped(
              request("http", ports("tls-web"), "/heartbeat", None)
            )
            expectTcpConnectionDropped(droppedAfterPlaintextWrite(ports("tls-tcp")))
            expectTlsFailure(
              request("https", ports("plain-web"), "/heartbeat", Some(clientContext))
            )
            expectTlsFailure(
              request("https", ports("plain-grpc"), "/", Some(clientContext))
            )
            expectTlsFailure(
              Try(Using.resource(connectTls(ports("plain-tcp"), clientContext))(_ => ()))
            )
          } finally {
            tlsSystem.terminate()
            plainSystem.terminate()
            tlsSystem.whenTerminated.futureValue
            plainSystem.whenTerminated.futureValue
          }
        }
      }

    "run the two-argument ActorSystemMain without managed setup" in
      withKit("managed-twoarg") { kit =>
        val systems = kit.createTestProbe[ActorSystem[?]]()
        ActorSystemMain("bare-svc", system => {
          systems.ref ! system
          Startup.Ready
        })
        val system = systems.receiveMessage()
        try system.whenTerminated.isCompleted shouldBe false
        finally stop(system)
      }

    "compose transforms in declaration order deterministically" in {
      val base = HttpServerSetup(_ => Future.successful(model.HttpResponse(entity = "ready")))
      val request = model.HttpRequest()
      def markerSetup(letter: String): ManagedSetup = ManagedSetup(http = setup =>
        setup.copy(service = request =>
          setup.service(request).map { response =>
            response.withEntity(letter + strictText(response.entity))
          }(ExecutionContext.parasitic)
        )
      )
      val ab = markerSetup("a").andThen(markerSetup("b")).http(base)
      strictText(ab.service(request).futureValue.entity) shouldBe "baready"
      val ba = markerSetup("b").andThen(markerSetup("a")).http(base)
      strictText(ba.service(request).futureValue.entity) shouldBe "abready"
    }

    "reject timeout false-positives in dropped-connection helpers" in {
      intercept[TestFailedException] {
        expectTcpConnectionDropped(
          Failure(new java.net.SocketTimeoutException("read timed out"))
        )
      }
      intercept[TestFailedException] {
        expectHttpConnectionDropped(
          Failure(new java.net.SocketTimeoutException("read timed out"))
        )
      }
      intercept[TestFailedException] {
        expectHttpConnectionDropped(
          Failure(new java.net.http.HttpTimeoutException("request timed out"))
        )
      }
      intercept[TestFailedException] {
        expectTcpConnectionDropped(Success(0))
      }
      expectTcpConnectionDropped(Success(-1))
      expectTcpConnectionDropped(
        Failure(new java.net.SocketException("Connection reset"))
      )
    }

    "surface a throwing configure as a failed binding and terminate" in
      withKit("managed-throw") { kit =>
        val sentinel = new RuntimeException("configure exploded")
        val binding = WebServerBoilerplate.start(
          "127.0.0.1",
          0,
          kit.system,
          webRoutes,
          (setup: HttpServerSetup) => throw sentinel
        )
        binding.failed.futureValue shouldBe sentinel
        kit.system.whenTerminated.futureValue
      }

    "surface a throwing tcp configure as a failed binding and terminate" in
      withKit("managed-throw-tcp") { kit =>
        val sentinel = new RuntimeException("tcp configure exploded")
        val binding = TcpServerBoilerplate.start(
          "127.0.0.1",
          0,
          kit.system,
          echoHandler,
          (setup: TcpServerSetup) => throw sentinel
        )
        binding.failed.futureValue shouldBe sentinel
        kit.system.whenTerminated.futureValue
      }

    "surface a throwing grpc configure as a failed binding and terminate" in
      withKit("managed-throw-grpc") { kit =>
        val sentinel = new RuntimeException("grpc configure exploded")
        val binding = GrpcServerBoilerplate.start(
          "127.0.0.1",
          0,
          kit.system,
          grpcService,
          (setup: HttpServerSetup) => throw sentinel
        )
        binding.failed.futureValue shouldBe sentinel
        kit.system.whenTerminated.futureValue
      }
  }
}
