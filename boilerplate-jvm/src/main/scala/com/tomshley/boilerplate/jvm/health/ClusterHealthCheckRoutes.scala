package com.tomshley.boilerplate.jvm.health

import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.cluster.{Cluster, MemberStatus}
import org.apache.pekko.http.scaladsl.Http
import org.apache.pekko.http.scaladsl.model.{ContentTypes, HttpEntity, _}
import org.apache.pekko.http.scaladsl.server.Directives._
import org.apache.pekko.http.scaladsl.server.{Route, StandardRoute}
import org.apache.pekko.stream.Materializer

import scala.io.StdIn
import scala.concurrent.Future

final class ClusterHealthCheckRoutes(host: String, port: Int)(implicit system: ActorSystem) {

  private val cluster = Cluster(system)

  private def ready: StandardRoute = {
    val status = cluster.selfMember.status
    if (status == MemberStatus.Up || status == MemberStatus.WeaklyUp)
      complete(
        HttpEntity(ContentTypes.`application/json`, s"""{ "heatlh": "OK" }"""))
    else
      complete(StatusCodes.NotFound)
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

  def bind()(implicit mat: Materializer): Future[Http.ServerBinding] =
    Http().bindAndHandle(routes, host, port)

  def bootHealthCheck()(implicit mat: Materializer): Unit = {
    val bindingFuture = bind()
    StdIn.readLine() // let it run until user presses return
    bindingFuture
      .flatMap(_.unbind())(system.dispatcher) // trigger unbinding from the port
      .onComplete(_ => system.terminate())(system.dispatcher) // and shutdown when done
  }

}
