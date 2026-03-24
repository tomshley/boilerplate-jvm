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

package com.tomshley.boilerplate.jvm.bootstrap

import org.apache.pekko.actor.typed.ActorSystem

import scala.concurrent.{ExecutionContext, Future}

@deprecated("Use Pekko Behaviors.withTimers for periodic work", "next")
object InfiniteIterationPromise {
  def apply(system: ActorSystem[?], body: => Unit): Future[Boolean] = {
    given ec:ExecutionContext = system.executionContext

    def iterate: Future[Boolean] =
      Future(body).flatMap(_ => iterate)

    iterate
  }
}
