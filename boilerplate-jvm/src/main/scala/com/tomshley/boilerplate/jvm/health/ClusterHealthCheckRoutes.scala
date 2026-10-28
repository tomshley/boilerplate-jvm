/*
 * copyright 2017–present tomshley llc
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

@deprecated("Use TypedClusterHealthCheckRoutes", "next")
final class ClusterHealthCheckRoutes(host: String, port: Int)(implicit system: ActorSystem) {

  private val cluster = Cluster(system)

  private def ready: StandardRoute = {
    val status = cluster.selfMember.status
    if (status == MemberStatus.Up || status == MemberStatus.WeaklyUp)
      complete(
        HttpEntity(ContentTypes.`application/json`, s"""{ "health": "OK" }"""))
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
