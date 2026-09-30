package com.tomshley.boilerplate.jvm.transport

import org.apache.pekko.actor.testkit.typed.scaladsl.LogCapturing
import org.apache.pekko.actor.typed.ActorSystem
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{Millis, Seconds, Span}
import org.scalatest.wordspec.AnyWordSpec

import java.net.InetSocketAddress
import scala.concurrent.{Future, Promise}

final class TransportBoilerplateSpec
    extends AnyWordSpec
    with Matchers
    with ScalaFutures
    with LogCapturing
    with TransportLifecycleChecks {

  override given patienceConfig: PatienceConfig =
    PatienceConfig(timeout = Span(15, Seconds), interval = Span(100, Millis))

  private final class TestTransport extends TransportBoilerplate[String, Unit] {
    override def start(
        interface: String,
        port: Int,
        system: ActorSystem[?],
        router: Unit
    ): Future[String] =
      Future.failed(new UnsupportedOperationException("not under test"))

    def manage(
        system: ActorSystem[?],
        binding: => Future[String],
        registerShutdown: String => Unit,
        localAddress: String => InetSocketAddress
    ): Future[String] =
      manageBinding(system, "test", binding)(registerShutdown, localAddress)
  }

  private val address = new InetSocketAddress("127.0.0.1", 4321)

  "TransportBoilerplate.manageBinding" should {
    "register shutdown before completing the bound future" in
      withKit("manage-order") { kit =>
        val registered = Promise[String]()
        val managed = new TestTransport().manage(
          kit.system,
          Future.successful("bound"),
          bound => {
            registered.success(bound)
            ()
          },
          _ => address
        )
        managed.futureValue shouldBe "bound"
        registered.future.isCompleted shouldBe true
        registered.future.futureValue shouldBe "bound"
      }

    "preserve the bind failure and terminate the system" in
      withKit("manage-failure") { kit =>
        val sentinel = new RuntimeException("bind exploded")
        val managed = new TestTransport().manage(
          kit.system,
          Future.failed(sentinel),
          _ => (),
          _ => address
        )
        managed.failed.futureValue shouldBe sentinel
        kit.system.whenTerminated.futureValue
      }

    "fail and terminate when shutdown registration throws" in
      withKit("manage-registration-failure") { kit =>
        val sentinel = new RuntimeException("registration exploded")
        val managed = new TestTransport().manage(
          kit.system,
          Future.successful("bound"),
          _ => throw sentinel,
          _ => address
        )
        managed.failed.futureValue shouldBe sentinel
        kit.system.whenTerminated.futureValue
      }

    "capture a binding creation throw and terminate the system" in
      withKit("manage-sync-failure") { kit =>
        val sentinel = new RuntimeException("bind exploded inline")
        val managed = new TestTransport().manage(
          kit.system,
          throw sentinel,
          _ => (),
          _ => address
        )
        managed.failed.futureValue shouldBe sentinel
        kit.system.whenTerminated.futureValue
      }

    "preserve the two-type-parameter start contract on transports" in {
      assertCompiles(
        """def bind(t: com.tomshley.boilerplate.jvm.transport.TransportBoilerplate[String, String], s: org.apache.pekko.actor.typed.ActorSystem[?]) = t.start("127.0.0.1", 0, s, "router")"""
      )
      assertCompiles(
        """def bind(g: com.tomshley.boilerplate.jvm.transport.GrpcServerBoilerplate.type, s: org.apache.pekko.actor.typed.ActorSystem[?], service: org.apache.pekko.http.scaladsl.model.HttpRequest => scala.concurrent.Future[org.apache.pekko.http.scaladsl.model.HttpResponse]) = g.start("127.0.0.1", 0, s, service, (setup: com.tomshley.boilerplate.jvm.transport.HttpServerSetup) => setup.copy(httpsContext = None))"""
      )
      assertDoesNotCompile(
        """def bind(g: com.tomshley.boilerplate.jvm.transport.GrpcServerBoilerplate.type, s: org.apache.pekko.actor.typed.ActorSystem[?], service: org.apache.pekko.http.scaladsl.model.HttpRequest => scala.concurrent.Future[org.apache.pekko.http.scaladsl.model.HttpResponse]) = g.start("127.0.0.1", 0, s, service, (setup: com.tomshley.boilerplate.jvm.transport.TcpServerSetup) => setup)"""
      )
    }
  }
}
