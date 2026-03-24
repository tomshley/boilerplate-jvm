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

import org.apache.pekko.{Done, NotUsed}
import org.apache.pekko.actor
import org.apache.pekko.actor.CoordinatedShutdown
import org.apache.pekko.actor.typed.ActorSystem
import org.apache.pekko.actor.typed.scaladsl.adapter.*
import org.apache.pekko.stream.{Attributes, FlowShape, Inlet, Outlet}
import org.apache.pekko.stream.scaladsl.{Flow, Sink, Tcp}
import org.apache.pekko.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler}
import org.apache.pekko.stream.Materializer
import org.apache.pekko.util.ByteString

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Success, Failure}

object TcpServerBoilerplate
  extends TransportBoilerplate[Tcp.ServerBinding, TcpServerHandlerBoilerplate] {

  private class ConnectionLifecycleStage[S](
      initialState: S,
      outboundFraming: ByteString => ByteString,
      onClosed: (S, Option[Throwable]) => Unit
  ) extends GraphStage[FlowShape[(S, ByteString), ByteString]] {

    val in: Inlet[(S, ByteString)] = Inlet("ConnectionLifecycle.in")
    val out: Outlet[ByteString] = Outlet("ConnectionLifecycle.out")
    override val shape: FlowShape[(S, ByteString), ByteString] = FlowShape(in, out)

    override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
      new GraphStageLogic(shape) with InHandler with OutHandler {
        private var lastState: S = initialState
        private var failureCause: Option[Throwable] = None

        setHandlers(in, out, this)

        override def onPush(): Unit = {
          val (state, response) = grab(in)
          lastState = state
          push(out, outboundFraming(response))
        }

        override def onPull(): Unit = pull(in)

        override def onUpstreamFailure(ex: Throwable): Unit = {
          failureCause = Some(ex)
          failStage(ex)
        }

        override def postStop(): Unit =
          onClosed(lastState, failureCause)
      }
  }

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
          val initialState = handler.initialState
          val flow = Flow[ByteString]
            .via(handler.framing)
            .map(_.compact)
            .scanAsync((initialState, ByteString.empty)) { case ((state, _), msg) =>
              handler.onMessage(msg, state)
            }
            .drop(1)
            .via(Flow.fromGraph(new ConnectionLifecycleStage[handler.State](
              initialState = initialState,
              outboundFraming = handler.outboundFraming,
              onClosed = (state, cause) => handler.onConnectionClosed(state, cause)
            )))
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
