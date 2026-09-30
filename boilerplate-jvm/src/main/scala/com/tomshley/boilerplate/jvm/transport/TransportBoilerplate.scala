package com.tomshley.boilerplate.jvm.transport

import org.apache.pekko.actor.typed.ActorSystem

import java.net.InetSocketAddress
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

trait TransportBoilerplate[TBinding, TRouter] {
  def start(
      interface: String,
      port: Int,
      system: ActorSystem[?],
      router: TRouter
  ): Future[TBinding]

  protected final def manageBinding(
      system: ActorSystem[?],
      transportName: String,
      binding: => Future[TBinding]
  )(
      registerShutdown: TBinding => Unit,
      localAddress: TBinding => InetSocketAddress
  ): Future[TBinding] = {
    given ExecutionContext = system.executionContext
    Future.fromTry(Try(binding)).flatten.map { bound =>
      registerShutdown(bound)
      bound
    }.andThen {
      case Success(bound) =>
        val address = localAddress(bound)
        system.log.info("{} server online at {}:{}", transportName, address.getHostString, address.getPort)
      case Failure(ex) =>
        system.log.error("Failed to bind {} server, terminating system", transportName, ex)
        system.terminate()
    }
  }
}
