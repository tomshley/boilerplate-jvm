package com.tomshley.boilerplate.jvm.health

import org.apache.pekko.actor.typed.ActorSystem
import org.apache.pekko.cluster.{Cluster, MemberStatus}
import org.slf4j.LoggerFactory

import scala.concurrent.Future

final class TypedClusterHealthCheck(using system: ActorSystem[?]) extends (() => Future[Boolean]) {
  private val log = LoggerFactory.getLogger(getClass)
  private val cluster = Cluster(system.classicSystem)

  override def apply(): Future[Boolean] = {
    log.info("TypedClusterHealthCheck called")
    val status = cluster.selfMember.status
    Future.successful(status == MemberStatus.Up || status == MemberStatus.WeaklyUp)
  }
}
