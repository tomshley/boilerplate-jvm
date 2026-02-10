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

package com.tomshley.boilerplate.jvm.transport

import org.apache.pekko.Done
import org.apache.pekko.actor
import org.apache.pekko.actor.CoordinatedShutdown
import org.apache.pekko.actor.typed.ActorSystem
import org.apache.pekko.actor.typed.scaladsl.adapter.*
import org.apache.pekko.stream.scaladsl.{Flow, Sink, Tcp}
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

    val bound: Future[Tcp.ServerBinding] =
      Tcp()
        .bind(interface, port)
        .to(Sink.foreach { connection =>
          val flow = Flow[ByteString]
            .via(handler.framing)
            .map(_.compact)
            .scanAsync((handler.initialState, ByteString.empty)) { case ((state, _), msg) =>
              handler.onMessage(msg, state)
            }
            .drop(1)
            .map { case (_, response) => handler.outboundFraming(response) }
          connection.handleWith(flow)
        })
        .run()

    bound.foreach { binding =>
      CoordinatedShutdown(classicSystem).addTask(
        CoordinatedShutdown.PhaseServiceUnbind,
        "tcp-server-unbind"
      ) { () =>
        binding.unbind().map(_ => Done)(ec)
      }
    }

    bound.onComplete {
      case Success(binding) =>
        val address = binding.localAddress
        system.log.info(
          "TCP server online at {}:{}",
          address.getHostString,
          address.getPort
        )

      case Failure(ex) =>
        system.log.error("Failed to bind TCP server, terminating system", ex)
        system.terminate()
    }

    bound
  }
}
