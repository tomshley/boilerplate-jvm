package com.tomshley.boilerplate.jvm.transport

import org.apache.pekko.actor.typed.ActorSystem

import scala.concurrent.Future

trait TransportBoilerplate[TBinding, TRouter] {
  def start(
             interface: String,
             port: Int,
             system: ActorSystem[?],
             router: TRouter
           ): Future[TBinding]
}
