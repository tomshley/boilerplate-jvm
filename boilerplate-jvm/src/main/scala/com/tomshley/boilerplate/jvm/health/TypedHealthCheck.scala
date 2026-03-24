package com.tomshley.boilerplate.jvm.health

import org.apache.pekko.actor.typed.ActorSystem
import org.slf4j.LoggerFactory

import scala.concurrent.Future

final class TypedHealthCheck(using system: ActorSystem[?]) extends (() => Future[Boolean]) {
  private val log = LoggerFactory.getLogger(getClass)

  override def apply(): Future[Boolean] = {
    log.info("TypedHealthCheck called")
    Future.successful(true)
  }
}
