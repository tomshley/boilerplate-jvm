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

import com.tomshley.boilerplate.jvm.managedmain.ManagedSetup
import org.apache.pekko.actor.typed.ActorSystem
import org.apache.pekko.http.scaladsl.{Http, HttpsConnectionContext}
import org.apache.pekko.http.scaladsl.server.Directives.{
  complete,
  concat,
  get,
  path
}
import org.apache.pekko.http.scaladsl.server.Route

import java.time.Instant
import scala.concurrent.Future
import scala.concurrent.duration.*

object WebServerBoilerplate
  extends TransportBoilerplate[Http.ServerBinding, Seq[Route]] {

  override def start(
      interface: String,
      port: Int,
      system: ActorSystem[?],
      routes: Seq[Route]
  ): Future[Http.ServerBinding] =
    start(interface, port, system, routes, identity[HttpServerSetup])

  def start(
      interface: String,
      port: Int,
      system: ActorSystem[?],
      routes: Seq[Route],
      httpsContext: HttpsConnectionContext
  ): Future[Http.ServerBinding] =
    start(interface, port, system, routes, _.copy(httpsContext = Some(httpsContext)))

  def start(
      interface: String,
      port: Int,
      system: ActorSystem[?],
      routes: Seq[Route],
      configure: HttpServerSetup => HttpServerSetup
  ): Future[Http.ServerBinding] = {
    given ActorSystem[?] = system

    val shutdownDeadline = system.settings.config
      .getDuration("tomshley-boilerplate-transport.http.shutdown-timeout").toNanos.nanos
    manageBinding(system, "HTTP", {
      val route = concat((Seq(get {
        path("heartbeat") {
          complete(Instant.now().toString)
        }
      }) ++ routes)*)
      val configured = configure(
        ManagedSetup.from(system).http(HttpServerSetup(Route.toFunction(route)))
      )
      val server = Http().newServerAt(interface, port)
      configured.httpsContext.fold(server)(server.enableHttps).bind(configured.service)
    })(
      binding => {
        binding.addToCoordinatedShutdown(shutdownDeadline)
        ()
      },
      _.localAddress
    )
  }
}
