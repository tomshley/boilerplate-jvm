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

package com.tomshley.boilerplate.jvm.managedmain

import org.apache.pekko.actor.typed.ActorSystem
import org.apache.pekko.actor.typed.scaladsl.Behaviors

import scala.util.{Failure, Success}
import scala.util.control.NonFatal

/** Internal coordinator actor that manages a [[Startup.Gated]] prerequisite.
  *
  * Spawned by [[ManagedMain]] when the service body returns a gated startup.
  * Not intended for direct use — use [[Startup.gated]] instead.
  */
private[managedmain] object StartupGate {

  private sealed trait Event[+R]
  private case class Completed[R](result: R) extends Event[R]
  private case class Failed(ex: Throwable) extends Event[Nothing]
  private case object TimedOut extends Event[Nothing]

  def run[R](gate: Startup.Gated[R])(using system: ActorSystem[?]): Unit = {
    system.systemActorOf(
      Behaviors.withTimers[Event[R]] { timers =>
        timers.startSingleTimer(TimedOut, gate.timeout)
        Behaviors.setup[Event[R]] { context =>
          context.pipeToSelf(gate.prerequisite()) {
            case Success(r)  => Completed(r)
            case Failure(ex) => Failed(ex)
          }
          Behaviors.receiveMessage {
            case Completed(result) =>
              system.log.info("StartupGate [{}]: ready.", gate.name)
              try {
                gate.onReady(result)
              } catch {
                case NonFatal(ex) =>
                  system.log.error(
                    "StartupGate [{}]: onReady callback failed — terminating: {}",
                    gate.name,
                    ex.getMessage
                  )
                  system.terminate()
              }
              Behaviors.stopped
            case Failed(ex) =>
              system.log.error(
                "StartupGate [{}]: failed — terminating: {}",
                gate.name,
                ex.getMessage
              )
              system.terminate()
              Behaviors.stopped
            case TimedOut =>
              system.log.error(
                "StartupGate [{}]: did not complete within {} — terminating.",
                gate.name,
                gate.timeout
              )
              system.terminate()
              Behaviors.stopped
          }
        }
      },
      s"startup-gate-${gate.name}"
    )
  }
}
