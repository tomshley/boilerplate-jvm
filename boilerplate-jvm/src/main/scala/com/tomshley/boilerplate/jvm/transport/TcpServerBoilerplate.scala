package com.tomshley.boilerplate.jvm.transport

import org.apache.pekko.actor
import org.apache.pekko.actor.typed.ActorSystem
import org.apache.pekko.actor.typed.scaladsl.adapter.*
import org.apache.pekko.stream.scaladsl.{Flow, Framing, Tcp}
import org.apache.pekko.stream.Materializer
import org.apache.pekko.util.ByteString

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Success, Failure}



object TcpServerBoilerplate
  extends TransportBoilerplate[Tcp.ServerBinding, TcpServerHandlerBoilerplate] {

  override def start(
                      interface: String,
                      port: Int,
                      system: ActorSystem[?],
                      handler: TcpServerHandlerBoilerplate
                    ): Future[Tcp.ServerBinding] = {

    given sys: ActorSystem[?] = system
    given classicSystem: actor.ActorSystem = system.toClassic
    given ec: ExecutionContext = system.executionContext
    given mat: Materializer = Materializer(classicSystem)

    val flow: Flow[ByteString, ByteString, _] =
      Flow[ByteString]
        // Message framing: newline-delimited text frames
        .via(Framing.delimiter(ByteString("\n"), maximumFrameLength = 65536))
        .map(_.compact) // raw bytes, no trimming or string conversion
        .mapAsync(4)(msg => handler.onMessage(msg))
        .map(_ ++ ByteString("\n")) // newline termination for the outbound frame

    val binding =
      Tcp().bindAndHandle(flow, interface, port)

    //
    // Logging style matches your existing boilerplates
    //
    binding.onComplete {
      case Success(b) =>
        val addr = b.localAddress
        system.log.info(
          "TCP server online at {}:{}",
          addr.getHostString,
          addr.getPort
        )

      case Failure(ex) =>
        system.log.error("Failed to bind TCP server, terminating system", ex)
        system.terminate()
    }

    binding
  }
}
