package com.tomshley.boilerplate.jvm.transport

import org.apache.pekko.actor.typed.ActorSystem
import org.apache.pekko.stream.scaladsl.{Flow, Framing}
import org.apache.pekko.util.ByteString

import scala.concurrent.Future

trait TcpServerHandlerBoilerplate:
  
  /** 
   * Framing stage to delimit incoming byte stream into messages.
   * Override to provide custom framing (e.g., length-field for binary protocols).
   * Default: newline-delimited text frames.
   */
  def framing: Flow[ByteString, ByteString, _] =
    Framing.delimiter(ByteString("\n"), maximumFrameLength = 65536)

  /**
   * Outbound framing - how to terminate outbound messages.
   * Override to provide custom outbound framing.
   * Default: append newline.
   */
  def outboundFraming(response: ByteString): ByteString = 
    response ++ ByteString("\n")

  /** Handle one inbound message and return a response. */
  def onMessage(msg: ByteString)(using ActorSystem[?]): Future[ByteString]
