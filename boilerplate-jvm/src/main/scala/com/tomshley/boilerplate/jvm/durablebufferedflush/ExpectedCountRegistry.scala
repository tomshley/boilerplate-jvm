/*
 * copyright 2026 tomshley llc
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

package com.tomshley.boilerplate.jvm.durablebufferedflush

import org.apache.pekko.Done
import org.apache.pekko.actor.typed.{ActorRef, ActorSystem, Behavior, Scheduler}
import org.apache.pekko.actor.typed.scaladsl.AskPattern.*
import org.apache.pekko.actor.typed.scaladsl.Behaviors
import org.apache.pekko.util.Timeout

import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}

private object ExpectedCountRegistryActor {
  sealed trait Command

  final case class Get(entityId: String, replyTo: ActorRef[Option[Long]]) extends Command

  final case class Put(entityId: String, count: Long, replyTo: ActorRef[Done]) extends Command

  final case class Remove(entityId: String, replyTo: ActorRef[Done]) extends Command

  def apply(): Behavior[Command] =
    active(Map.empty)

  private def active(counts: Map[String, Long]): Behavior[Command] =
    Behaviors.receiveMessage {
      case Get(entityId, replyTo) =>
        replyTo ! counts.get(entityId)
        Behaviors.same
      case Put(entityId, count, replyTo) =>
        replyTo ! Done
        active(counts.updated(entityId, count))
      case Remove(entityId, replyTo) =>
        replyTo ! Done
        active(counts - entityId)
    }
}

final class ExpectedCountRegistry(
    system: ActorSystem[?],
    actorNamePrefix: String
) {
  private given ExecutionContext = system.executionContext
  private given Scheduler = system.scheduler

  private val registry = system.systemActorOf(
    ExpectedCountRegistryActor(),
    s"$actorNamePrefix-${UUID.randomUUID()}"
  )

  def get(entityId: String)(using Timeout): Future[Option[Long]] =
    registry.ask(replyTo => ExpectedCountRegistryActor.Get(entityId, replyTo))

  def put(entityId: String, count: Long)(using Timeout): Future[Unit] =
    registry.ask(replyTo => ExpectedCountRegistryActor.Put(entityId, count, replyTo)).map(_ => ())

  def remove(entityId: String)(using Timeout): Future[Unit] =
    registry.ask(replyTo => ExpectedCountRegistryActor.Remove(entityId, replyTo)).map(_ => ())
}
