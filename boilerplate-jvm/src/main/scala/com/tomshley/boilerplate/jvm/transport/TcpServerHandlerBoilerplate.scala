package com.tomshley.boilerplate.jvm.transport

import org.apache.pekko.actor.typed.ActorSystem
import org.apache.pekko.stream.scaladsl.{Flow, Framing}
import org.apache.pekko.util.ByteString

import scala.concurrent.Future

/** Base handler contract for TCP server connections.
 *
 * A single handler instance is shared across all connections;
 * per-connection state is threaded via [[State]] and `scanAsync`.
 * Implementations must be stateless — all mutable per-connection
 * data must live in [[State]].
 *
 * [[framing]] must remain a `def` (not `val` or `lazy val`)
 * because framing stages like `Framing.lengthField` are stateful.
 * Each connection needs a fresh stage instance.
 */
trait TcpServerHandlerBoilerplate:

  type State <: TcpConnectionState

  /** Initial per-connection state. */
  def initialState: State

  /** 
   * Framing stage to delimit incoming byte stream into messages.
   * Override to provide custom framing (e.g., length-field for binary protocols).
   * Default: newline-delimited text frames.
   */
  def framing: Flow[ByteString, ByteString, ?] =
    Framing.delimiter(ByteString("\n"), maximumFrameLength = 65536)

  /**
   * Outbound framing - how to terminate outbound messages.
   * Override to provide custom outbound framing.
   * Default: append newline.
   */
  def outboundFraming(response: ByteString): ByteString = 
    response ++ ByteString("\n")

  /** Handle one inbound message with current state, returning updated state and response.
   *
   * Returning a failed Future will terminate the connection.
   * Encode application-level errors in the response ByteString instead.
   */
  def onMessage(msg: ByteString, state: State)(using ActorSystem[?]): Future[(State, ByteString)]

  def onConnectionClosed(state: State, cause: Option[Throwable])(using ActorSystem[?]): Unit = ()
