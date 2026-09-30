package com.tomshley.boilerplate.jvm.managedmain

import com.tomshley.boilerplate.jvm.transport.{HttpServerSetup, TcpServerSetup}
import org.apache.pekko.Done
import org.apache.pekko.actor.setup.Setup
import org.apache.pekko.actor.typed.ActorSystem
import org.apache.pekko.http.scaladsl.HttpsConnectionContext

import javax.net.ssl.SSLEngine
import scala.concurrent.{ExecutionContext, Future}

final case class ManagedSetup(
    startup: Vector[StartupTask[?]] = Vector.empty,
    http: HttpServerSetup => HttpServerSetup = identity,
    tcp: TcpServerSetup => TcpServerSetup = identity
) extends Setup {

  def andThen(next: ManagedSetup): ManagedSetup =
    ManagedSetup(startup ++ next.startup, http.andThen(next.http), tcp.andThen(next.tcp))

  private[managedmain] def initialize(system: ActorSystem[?]): Future[Done] = {
    given ExecutionContext = system.executionContext
    val starts = startup.map(_.prepare(system))
    starts.foldLeft(Future.successful(Done): Future[Done]) { (ready, start) =>
      ready.flatMap(_ => start())
    }
  }
}

object ManagedSetup {
  val empty: ManagedSetup = ManagedSetup()

  def tls(httpsContext: HttpsConnectionContext, createSSLEngine: () => SSLEngine): ManagedSetup =
    ManagedSetup(
      http = setup => setup.copy(httpsContext = Some(httpsContext)),
      tcp = setup => setup.copy(createSSLEngine = Some(createSSLEngine))
    )

  private[jvm] def from(system: ActorSystem[?]): ManagedSetup =
    system.settings.setup.get[ManagedSetup].getOrElse(empty)
}
