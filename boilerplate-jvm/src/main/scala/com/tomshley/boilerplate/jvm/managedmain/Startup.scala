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

import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration

/** Describes how a managed service completes its initialization.
  *
  * Services that are immediately ready after their body runs return `Unit`,
  * which is implicitly converted to [[Startup.Ready]]. Services that must
  * wait for an async prerequisite (recovery, migration, warmup) before
  * accepting traffic return [[Startup.gated]].
  *
  * {{{
  * // Immediate — body returns Unit, implicitly Ready:
  * PekkoClusterMain("svc", { system =>
  *   init(system)
  *   GrpcServerBoilerplate.start(...)
  * })
  *
  * // Gated — TCP binds only after recovery completes:
  * PekkoClusterMain("svc", { system =>
  *   val handler = buildHandler(system)
  *   Startup.gated("recovery", recoveryManager.recover(), timeout) { report =>
  *     startTcpServer(system, handler)
  *   }
  * })
  * }}}
  */
sealed trait Startup {
  /** Activate this startup. Called by [[ManagedMain]] after the body returns.
    * For [[Startup.Ready]], this is a no-op. For gated startups, this spawns
    * the coordinator actor that waits for the prerequisite. */
  def run(using system: ActorSystem[?]): Unit
}

object Startup {
  case object Ready extends Startup {
    override def run(using system: ActorSystem[?]): Unit = ()
  }

  given Conversion[Unit, Startup] = _ => Ready

  /** Gates service readiness behind an async prerequisite.
    *
    * The returned [[Startup]] signals to [[ManagedMain]] that a coordinator
    * actor should be spawned. The coordinator waits for `prerequisite` to
    * complete (within `timeout`) and then calls `onReady` with the result.
    * On failure or timeout the system is terminated with diagnostic logging.
    *
    * @param name          human-readable gate name, used in log messages and actor name
    * @param prerequisite  the async work that must finish before the service is ready
    * @param timeout       maximum time to wait before terminating the system
    * @param onReady       callback invoked with the prerequisite result on success
    */
  def gated[R](
      name: String,
      prerequisite: => Future[R],
      timeout: FiniteDuration
  )(onReady: R => Unit): Startup =
    Gated(name, () => prerequisite, timeout, onReady)

  private[managedmain] case class Gated[R](
      name: String,
      prerequisite: () => Future[R],
      timeout: FiniteDuration,
      onReady: R => Unit
  ) extends Startup {
    override def run(using system: ActorSystem[?]): Unit = StartupGate.run(this)
  }
}
