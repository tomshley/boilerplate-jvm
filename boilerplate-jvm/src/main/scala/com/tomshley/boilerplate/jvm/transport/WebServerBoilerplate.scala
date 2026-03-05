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

import org.apache.pekko.actor
import org.apache.pekko.actor.typed.ActorSystem
import org.apache.pekko.http.scaladsl.Http
import org.apache.pekko.http.scaladsl.server.Directives.{
  complete,
  concat,
  get,
  path
}
import org.apache.pekko.http.scaladsl.server.Route

import java.time.Instant
import scala.concurrent.duration.*
import scala.concurrent.{ExecutionContextExecutor, Future}
import scala.util.{Failure, Success}

object WebServerBoilerplate extends TransportBoilerplate[Http.ServerBinding, Seq[Route]] {

  override def start(
                      interface: String,
                      port: Int,
                      system: ActorSystem[?],
                      routes: Seq[Route]
                    ): Future[Http.ServerBinding] = {

    import org.apache.pekko.actor.typed.scaladsl.adapter.*
    given classicSystem: actor.ActorSystem = system.toClassic
    given ec: ExecutionContextExecutor = system.executionContext

    val bound = Http()
      .newServerAt(interface, port)
      .bind(concat((Seq(get {
        path("heartbeat") {
          complete(Instant.now().toString)
        }
      }) ++ routes)*))
      .map(_.addToCoordinatedShutdown(3.seconds))

    bound.onComplete {
      case Success(binding) =>
        val address = binding.localAddress
        system.log.info(
          "HTTP server online at {}:{}",
          address.getHostString,
          address.getPort
        )

      case Failure(ex) =>
        system.log.error("Failed to bind HTTP server, terminating system", ex)
        system.terminate()
    }

    bound
  }
}
