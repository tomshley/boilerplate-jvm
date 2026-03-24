package com.tomshley.boilerplate.jvm.health

import org.apache.pekko.Done
import org.apache.pekko.actor.CoordinatedShutdown
import org.apache.pekko.actor.typed.ActorSystem
import org.apache.pekko.cluster.{Cluster, MemberStatus}
import org.apache.pekko.http.scaladsl.Http
import org.apache.pekko.http.scaladsl.model.{ContentTypes, HttpEntity, StatusCodes}
import org.apache.pekko.http.scaladsl.server.Directives.{get, path}
import org.apache.pekko.http.scaladsl.server.RouteConcatenation.*
import org.apache.pekko.http.scaladsl.server.{Route, StandardRoute}
import org.apache.pekko.stream.Materializer
import org.slf4j.LoggerFactory

import scala.concurrent.Future

final class TypedClusterHealthCheckRoutes(host: String, port: Int)(using system: ActorSystem[?]) {
  private val log = LoggerFactory.getLogger(getClass)
  private val cluster = Cluster(system.classicSystem)

  private def ready: StandardRoute = {
    val status = cluster.selfMember.status
    if (status == MemberStatus.Up || status == MemberStatus.WeaklyUp)
      org.apache.pekko.http.scaladsl.server.Directives.complete(
        HttpEntity(ContentTypes.`application/json`, s"""{ "health": "OK" }""")
      )
    else
      org.apache.pekko.http.scaladsl.server.Directives.complete(StatusCodes.NotFound)
  }

  def routes: Route =
    path("ready") {
      get {
        ready
      }
    } ~
      path("alive") {
        get {
          ready
        }
      }

  def bind()(using Materializer): Future[Http.ServerBinding] =
    Http()(system.classicSystem).newServerAt(host, port).bind(routes)

  def bootHealthCheck()(using Materializer): Unit = {
    val classicSystem = system.classicSystem
    given scala.concurrent.ExecutionContext = system.executionContext

    val bindingF = bind()
    CoordinatedShutdown(classicSystem).addTask(
      CoordinatedShutdown.PhaseServiceUnbind,
      s"typed-cluster-health-check-routes-unbind-${System.identityHashCode(this)}"
    ) { () =>
      bindingF.flatMap(_.unbind()).map(_ => Done)
    }
    bindingF.failed.foreach { ex =>
      log.error("TypedClusterHealthCheckRoutes failed to bind to {}:{} — terminating.", host, port.asInstanceOf[AnyRef], ex)
      system.terminate()
    }
  }
}
