package com.tomshley.boilerplate.jvm.transport

import org.apache.pekko.actor.typed.ActorSystem
import org.apache.pekko.util.ByteString

import scala.concurrent.Future

trait TcpServerHandlerBoilerplate:
  /** Handle one inbound message and return a response. */
  def onMessage(msg: ByteString)(using ActorSystem[?]): Future[ByteString]
