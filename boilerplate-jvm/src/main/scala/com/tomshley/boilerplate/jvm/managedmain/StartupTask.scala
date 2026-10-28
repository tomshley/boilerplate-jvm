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

package com.tomshley.boilerplate.jvm.managedmain

import org.apache.pekko.Done
import org.apache.pekko.actor.CoordinatedShutdown
import org.apache.pekko.actor.typed.ActorSystem

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

final case class StartupTask[A](
    name: String,
    acquire: ActorSystem[?] => Future[A],
    release: (ActorSystem[?], A) => Future[Done]
) {
  require(name.trim.nonEmpty, "Startup task name must not be empty")

  private[managedmain] def prepare(system: ActorSystem[?]): () => Future[Done] = {
    given ExecutionContext = ExecutionContext.parasitic
    val shutdown = CoordinatedShutdown(system)
    lazy val acquired: Future[A] = shutdown.shutdownReason() match {
      case Some(_) => Future.failed(new IllegalStateException("ActorSystem is shutting down"))
      case None => Future.fromTry(Try(acquire(system))).flatten
    }
    shutdown.addTask(CoordinatedShutdown.PhaseBeforeActorSystemTerminate, s"managed-$name") { () =>
      acquired.transformWith {
        case Success(resource) => Future.fromTry(Try(release(system, resource))).flatten
        case Failure(_) => Future.successful(Done)
      }
    }
    () => acquired.map(_ => Done)
  }
}
