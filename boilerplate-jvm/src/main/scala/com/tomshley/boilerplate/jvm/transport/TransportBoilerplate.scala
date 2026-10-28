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

package com.tomshley.boilerplate.jvm.transport

import org.apache.pekko.actor.typed.ActorSystem

import java.net.InetSocketAddress
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

trait TransportBoilerplate[TBinding, TRouter] {
  def start(
      interface: String,
      port: Int,
      system: ActorSystem[?],
      router: TRouter
  ): Future[TBinding]

  protected final def manageBinding(
      system: ActorSystem[?],
      transportName: String,
      binding: => Future[TBinding]
  )(
      registerShutdown: TBinding => Unit,
      localAddress: TBinding => InetSocketAddress
  ): Future[TBinding] = {
    given ExecutionContext = system.executionContext
    Future.fromTry(Try(binding)).flatten.map { bound =>
      registerShutdown(bound)
      bound
    }.andThen {
      case Success(bound) =>
        val address = localAddress(bound)
        system.log.info("{} server online at {}:{}", transportName, address.getHostString, address.getPort)
      case Failure(ex) =>
        system.log.error("Failed to bind {} server, terminating system", transportName, ex)
        system.terminate()
    }
  }
}
