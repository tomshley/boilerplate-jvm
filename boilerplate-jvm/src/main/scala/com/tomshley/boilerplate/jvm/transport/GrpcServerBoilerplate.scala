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
import org.apache.pekko.http.scaladsl.{Http, HttpsConnectionContext, model}

import scala.concurrent.Future
import scala.concurrent.duration.*

object GrpcServerBoilerplate
  extends TransportBoilerplate[
    Http.ServerBinding,
    model.HttpRequest => Future[model.HttpResponse]
  ] {

  override def start(
      interface: String,
      port: Int,
      system: ActorSystem[?],
      service: model.HttpRequest => Future[model.HttpResponse]
  ): Future[Http.ServerBinding] =
    start(interface, port, system, service, identity[HttpServerSetup])

  def start(
      interface: String,
      port: Int,
      system: ActorSystem[?],
      service: model.HttpRequest => Future[model.HttpResponse],
      httpsContext: HttpsConnectionContext
  ): Future[Http.ServerBinding] =
    start(interface, port, system, service, _.copy(httpsContext = Some(httpsContext)))

  def start(
      interface: String,
      port: Int,
      system: ActorSystem[?],
      service: model.HttpRequest => Future[model.HttpResponse],
      configure: HttpServerSetup => HttpServerSetup
  ): Future[Http.ServerBinding] = {
    given ActorSystem[?] = system

    val shutdownDeadline = system.settings.config
      .getDuration("tomshley-boilerplate-transport.http.shutdown-timeout").toNanos.nanos
    manageBinding(system, "gRPC", {
      val configured = configure(
        ManagedSetup.from(system).http(HttpServerSetup(service))
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
