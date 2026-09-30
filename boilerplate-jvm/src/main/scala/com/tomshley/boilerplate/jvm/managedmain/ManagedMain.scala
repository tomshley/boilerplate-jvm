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

import org.apache.pekko.actor.CoordinatedShutdown
import org.apache.pekko.actor.setup.ActorSystemSetup
import org.apache.pekko.actor.typed.ActorSystem
import org.apache.pekko.actor.typed.scaladsl.Behaviors
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.duration.*
import scala.util.control.NonFatal

protected[managedmain] trait ManagedMain {

  def apply(serviceName: String, body: ActorSystem[?] => Startup): Unit = {
    apply(serviceName, ActorSystemSetup.empty, body)
    ()
  }

  def apply(serviceName: String, setup: ManagedSetup, body: ActorSystem[?] => Startup): ActorSystem[Nothing] =
    apply(serviceName, ActorSystemSetup(setup), body)

  def apply(serviceName: String, setup: ActorSystemSetup, body: ActorSystem[?] => Startup): ActorSystem[Nothing] = {
    def logger: Logger = LoggerFactory.getLogger(s"$serviceName-$name")
    ActorSystem[Nothing](Behaviors.setup[Nothing] { context =>
      given ActorSystem[?] = context.system
      def launch(): Unit = {
        if (CoordinatedShutdown(context.system).shutdownReason().isEmpty) {
          bootstrap(context.system, logger)
          body(context.system).run
        }
      }
      try {
        val managed = ManagedSetup.from(context.system)
        if (managed.startup.isEmpty) launch()
        else {
          val timeout = context.system.settings.config
            .getDuration("tomshley-boilerplate-managedmain.startup-timeout").toNanos.nanos
          require(timeout.length > 0, "Managed startup timeout must be positive")
          Startup.gated("managed startup", managed.initialize(context.system), timeout)(_ => launch()).run
        }
      } catch {
        case NonFatal(e) =>
          logger.error("Terminating due to initialization failure.", e)
          context.system.terminate()
      }
      Behaviors.empty
    }, serviceName, setup)
  }

  protected[managedmain] lazy val name:String
  protected[managedmain] def bootstrap(system: ActorSystem[?], logger: Logger): Unit = {
    logger.info(s"Bootstrapping $name")
  }
}
