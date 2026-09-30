package com.tomshley.boilerplate.jvm.transport

import org.apache.pekko.actor.testkit.typed.scaladsl.{LogCapturing, TestProbe}
import org.apache.pekko.actor.typed.ActorSystem
import org.apache.pekko.stream.scaladsl.Tcp
import org.apache.pekko.util.ByteString
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{Millis, Seconds, Span}
import org.scalatest.wordspec.AnyWordSpec

import java.io.{BufferedReader, IOException, InputStreamReader, OutputStream}
import java.net.{InetSocketAddress, Socket, SocketTimeoutException}
import java.nio.charset.StandardCharsets
import javax.net.ssl.{SSLContext, SSLEngine, SSLHandshakeException, SSLSocket}
import scala.concurrent.Future
import scala.util.{Failure, Success, Try, Using}

final class TcpServerBoilerplateSpec
    extends AnyWordSpec
    with Matchers
    with ScalaFutures
    with LogCapturing
    with TransportLifecycleChecks {

  override given patienceConfig: PatienceConfig =
    PatienceConfig(timeout = Span(15, Seconds), interval = Span(100, Millis))

  private final case class EchoState(received: Int) extends TcpConnectionState

  private val ioTimeoutMillis = 5000

  private val transport = TcpServerBoilerplate

  private def handler(
      closed: Option[TestProbe[(EchoState, Option[Throwable])]] = None,
      messages: Option[TestProbe[ByteString]] = None,
      failure: Option[Throwable] = None
  ): TcpServerHandlerBoilerplate = new TcpServerHandlerBoilerplate {
    override type State = EchoState
    override def initialState: State = EchoState(0)
    override def onMessage(
        msg: ByteString,
        state: State
    )(using ActorSystem[?]): Future[(State, ByteString)] =
      failure.filter(_ => msg.utf8String == "boom") match {
        case Some(cause) => Future.failed(cause)
        case None =>
          messages.foreach(_.ref ! msg)
          Future.successful(
            (
              state.copy(received = state.received + 1),
              ByteString(s"${state.received + 1}:${msg.utf8String}")
            )
          )
      }
    override def onConnectionClosed(
        state: State,
        cause: Option[Throwable]
    )(using ActorSystem[?]): Unit =
      closed.foreach(_.ref ! ((state, cause)))
  }

  private def connect(port: Int): Socket = {
    val socket = new Socket()
    try {
      socket.connect(new InetSocketAddress("127.0.0.1", port), ioTimeoutMillis)
      socket.setSoTimeout(ioTimeoutMillis)
      socket
    } catch {
      case scala.util.control.NonFatal(e) =>
        socket.close()
        throw e
    }
  }

  private def connectTls(port: Int, context: SSLContext): SSLSocket = {
    val socket = context.getSocketFactory.createSocket().asInstanceOf[SSLSocket]
    try {
      socket.connect(new InetSocketAddress("127.0.0.1", port), ioTimeoutMillis)
      socket.setSoTimeout(ioTimeoutMillis)
      val parameters = socket.getSSLParameters
      parameters.setEndpointIdentificationAlgorithm("HTTPS")
      socket.setSSLParameters(parameters)
      socket.startHandshake()
      socket
    } catch {
      case scala.util.control.NonFatal(e) =>
        socket.close()
        throw e
    }
  }

  private def withIo[A](socket: Socket)(
      test: (BufferedReader, OutputStream) => A
  ): A =
    Using.resource(socket) { connected =>
      val reader = new BufferedReader(
        new InputStreamReader(connected.getInputStream, StandardCharsets.UTF_8)
      )
      test(reader, connected.getOutputStream)
    }

  private def exchange(
      reader: BufferedReader,
      out: OutputStream,
      message: String
  ): String = {
    out.write((message + "\n").getBytes(StandardCharsets.UTF_8))
    out.flush()
    reader.readLine()
  }

  "TcpServerBoilerplate" should {
    "echo counted responses per connection over plaintext" in
      withKit("tcp-plaintext") { kit =>
        val closed = kit.createTestProbe[(EchoState, Option[Throwable])]()
        val binding = TcpServerBoilerplate
          .start(
            interface = "127.0.0.1",
            port = 0,
            system = kit.system,
            handler = handler(closed = Some(closed))
          )
          .futureValue
        val port = binding.localAddress.getPort

        withIo(connect(port)) { (reader, out) =>
          exchange(reader, out, "first") shouldBe "1:first"
          exchange(reader, out, "second") shouldBe "2:second"
        }
        closed.expectMessage((EchoState(2), None))

        withIo(connect(port)) { (reader, out) =>
          exchange(reader, out, "third") shouldBe "1:third"
        }
        closed.expectMessage((EchoState(1), None))
      }

    "echo counted responses per connection over TLS with a fresh engine" in
      withKit("tcp-tls") { kit =>
        TransportTlsFixture.withContexts { (serverContext, clientContext) =>
          val closed = kit.createTestProbe[(EchoState, Option[Throwable])]()
          val engines = kit.createTestProbe[SSLEngine]()
          val factory: () => SSLEngine = () => {
            val engine = TransportTlsFixture.serverEngine(serverContext)
            engines.ref ! engine
            engine
          }
          val binding = TcpServerBoilerplate
            .start(
              interface = "127.0.0.1",
              port = 0,
              system = kit.system,
              handler = handler(closed = Some(closed)),
              createSSLEngine = factory
            )
            .futureValue
          val port = binding.localAddress.getPort

          withIo(connectTls(port, clientContext)) { (reader, out) =>
            exchange(reader, out, "first") shouldBe "1:first"
            exchange(reader, out, "second") shouldBe "2:second"
          }
          val firstEngine = engines.receiveMessage()
          closed.expectMessage((EchoState(2), None))

          withIo(connectTls(port, clientContext)) { (reader, out) =>
            exchange(reader, out, "third") shouldBe "1:third"
          }
          val secondEngine = engines.receiveMessage()
          closed.expectMessage((EchoState(1), None))
          firstEngine should not be theSameInstanceAs(secondEngine)
        }
      }

    "close the connection with its cause when the handler fails over plaintext" in
      withKit("tcp-failure-plaintext") { kit =>
        val closed = kit.createTestProbe[(EchoState, Option[Throwable])]()
        val sentinel = new RuntimeException("handler exploded")
        val binding = transport
          .start(
            "127.0.0.1",
            0,
            kit.system,
            handler(closed = Some(closed), failure = Some(sentinel))
          )
          .futureValue
        withIo(connect(binding.localAddress.getPort)) { (reader, out) =>
          exchange(reader, out, "first") shouldBe "1:first"
          out.write("boom\n".getBytes(StandardCharsets.UTF_8))
          out.flush()
          val (state, cause) = closed.receiveMessage()
          state shouldBe EchoState(1)
          cause.isDefined shouldBe true
          causes(cause.get).contains(sentinel) shouldBe true
        }
      }

    "close the connection with its cause when the handler fails over TLS" in
      withKit("tcp-failure-tls") { kit =>
        TransportTlsFixture.withContexts { (serverContext, clientContext) =>
          val closed = kit.createTestProbe[(EchoState, Option[Throwable])]()
          val sentinel = new RuntimeException("handler exploded")
          val binding = transport
            .start(
              "127.0.0.1",
              0,
              kit.system,
              handler(closed = Some(closed), failure = Some(sentinel)),
              () => TransportTlsFixture.serverEngine(serverContext)
            )
            .futureValue
          withIo(connectTls(binding.localAddress.getPort, clientContext)) {
            (reader, out) =>
              exchange(reader, out, "first") shouldBe "1:first"
              out.write("boom\n".getBytes(StandardCharsets.UTF_8))
              out.flush()
              val (state, cause) = closed.receiveMessage()
              state shouldBe EchoState(1)
              cause.isDefined shouldBe true
              causes(cause.get).contains(sentinel) shouldBe true
          }
        }
      }

    "reject plaintext clients on the TLS listener without invoking the handler" in
      withKit("tcp-tls-plaintext") { kit =>
        TransportTlsFixture.withContexts { (serverContext, _) =>
          val messages = kit.createTestProbe[ByteString]()
          val binding = transport
            .start(
              "127.0.0.1",
              0,
              kit.system,
              handler(messages = Some(messages)),
              () => TransportTlsFixture.serverEngine(serverContext)
            )
            .futureValue
          val socket = connect(binding.localAddress.getPort)
          try {
            socket.getOutputStream.write("hello\n".getBytes(StandardCharsets.UTF_8))
            socket.getOutputStream.flush()
            val reader = new BufferedReader(
              new InputStreamReader(socket.getInputStream, StandardCharsets.UTF_8)
            )
            Try(reader.readLine()) match {
              case Success(line) => Option(line) shouldBe None
              case Failure(ex) =>
                ex.isInstanceOf[IOException] shouldBe true
                ex.isInstanceOf[SocketTimeoutException] shouldBe false
            }
            messages.expectNoMessage()
          } finally socket.close()
        }
      }

    "reject an untrusted server certificate" in
      withKit("tcp-tls-untrusted") { kit =>
        TransportTlsFixture.withContexts { (serverContext, _) =>
          val binding = transport
            .start(
              "127.0.0.1",
              0,
              kit.system,
              handler(),
              () => TransportTlsFixture.serverEngine(serverContext)
            )
            .futureValue
          val failure = Try(
            Using.resource(
              connectTls(binding.localAddress.getPort, SSLContext.getDefault)
            )(_ => ())
          )
          failure.isFailure shouldBe true
          causes(failure.failed.get)
            .exists(_.isInstanceOf[SSLHandshakeException]) shouldBe true
        }
      }

    "enforce caller-configured client authentication" in
      withKit("tcp-tls-mtls") { kit =>
        TransportTlsFixture.withMutualContexts {
          (serverContext, clientContext, authenticatedContext) =>
            val messages = kit.createTestProbe[ByteString]()
            val binding = transport
              .start(
                "127.0.0.1",
                0,
                kit.system,
                handler(messages = Some(messages)),
                () =>
                  TransportTlsFixture.serverEngine(
                    serverContext,
                    needClientAuth = true
                  )
              )
              .futureValue
            val rejected = Try {
              Using.resource(
                connectTls(binding.localAddress.getPort, clientContext)
              ) { socket =>
                socket.getInputStream.read()
              }
            }
            rejected.isFailure shouldBe true
            causes(rejected.failed.get)
              .exists(_.isInstanceOf[javax.net.ssl.SSLException]) shouldBe true
            messages.expectNoMessage()
            withIo(
              connectTls(binding.localAddress.getPort, authenticatedContext)
            ) { (reader, out) =>
              exchange(reader, out, "first") shouldBe "1:first"
            }
        }
      }

    serverLifecycleCases[Tcp.ServerBinding](
      "TCP-plaintext",
      (system, port) => transport.start("127.0.0.1", port, system, handler()),
      _.localAddress,
      (_, binding) => binding.whenUnbound
    )

    serverLifecycleCases[Tcp.ServerBinding](
      "TCP-TLS",
      (system, port) =>
        TransportTlsFixture.withContexts { (serverContext, _) =>
          transport.start(
            "127.0.0.1",
            port,
            system,
            handler(),
            () => TransportTlsFixture.serverEngine(serverContext)
          )
        },
      _.localAddress,
      (_, binding) => binding.whenUnbound
    )
  }
}
