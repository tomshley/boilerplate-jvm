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

package com.tomshley.boilerplate.jvm.reqreply

import org.apache.pekko.actor.typed.scaladsl.Behaviors
import org.apache.pekko.actor.typed.{ActorRef, ActorSystem, Behavior}
import org.apache.pekko.cluster.sharding.typed.scaladsl.{ClusterSharding, Entity, EntityContext, EntityTypeKey}
import org.apache.pekko.pattern.StatusReply
import org.apache.pekko.persistence.typed.scaladsl.{Effect, EventSourcedBehavior}
import org.apache.pekko.persistence.typed.{EventAdapter, PersistenceId, SnapshotAdapter}

import java.time.Instant

/** Plain runtime types — boilerplate ships no serializer binding. The
 *  consuming service chooses its serializer through configuration or
 *  Pekko's `SerializationSetup`, and may lift the protocol into its own
 *  wire classes without replacing the entity or client.
 *
 *  Durability is the consumer's choice. The default behavior is in-memory;
 *  [[persistent]] runs the same transitions journaled. Native `EventAdapter`
 *  and `SnapshotAdapter` values translate library events and state into
 *  consumer-owned storage schemas. Recorded timestamps must survive these
 *  translations unchanged so replay never consults the current clock:
 *
 * {{{
 *   def toJournal(event: Idempotent.Event): MyEvent = event match {
 *     case Idempotent.RequestRecorded(key, on, headers, body) => RequestStored(key, on, headers, body)
 *     case Idempotent.RequestRepeated => DuplicateRequestStored
 *     case Idempotent.ReplyRecorded(on, headers, body) => ReplyStored(on, headers, body)
 *     case Idempotent.ReplyRepeated => DuplicateReplyStored
 *     case Idempotent.RequestAbandoned => AbandonmentStored
 *   }
 * }}}
 */
object Idempotent {
  sealed trait Command {
    def replyTo: ActorRef[StatusReply[Summary]]
  }


  final case class SingleRequest(headers: Option[Map[String, String]],
                                 body: Option[String],
                                 replyTo: ActorRef[StatusReply[Summary]])
      extends Command

  final case class SingleReply(headers: Option[Map[String, String]],
                               body: Option[String],
                               replyTo: ActorRef[StatusReply[Summary]])
      extends Command

  final case class AbandonRequest(replyTo: ActorRef[StatusReply[Summary]]) extends Command

  /** Read-only status query — records nothing, claims nothing. */
  final case class StatusRequest(replyTo: ActorRef[StatusReply[Summary]]) extends Command

  sealed trait Event

  final case class RequestRecorded(key: String,
                                   on: Instant,
                                   headers: Option[Map[String, String]],
                                   body: Option[String]) extends Event

  case object RequestRepeated extends Event

  final case class ReplyRecorded(on: Instant,
                                 headers: Option[Map[String, String]],
                                 body: Option[String]) extends Event

  case object ReplyRepeated extends Event

  case object RequestAbandoned extends Event

  final case class State(idempotencyKey: Option[String],
                         requestedOn: Option[Instant],
                         repliedOn: Option[Instant],
                         requestCount: Int,
                         replyCount: Int,
                         requestHeaders: Option[Map[String, String]],
                         requestBody: Option[String],
                         replyHeaders: Option[Map[String, String]],
                         replyBody: Option[String]) {
    def newRequest(idempotencyKey: String,
                   requestHeaders: Option[Map[String, String]],
                   requestBody: Option[String]): State =
      newRequestAt(idempotencyKey, requestHeaders, requestBody, Instant.now())

    private[reqreply] def newRequestAt(key: String,
                                       headers: Option[Map[String, String]],
                                       body: Option[String],
                                       on: Instant): State = {
      copy(
        idempotencyKey = Some(key),
        requestedOn = Some(on),
        repliedOn = repliedOn,
        requestCount = requestCount + 1,
        replyCount = replyCount,
        requestHeaders = headers,
        requestBody = body,
        replyHeaders = replyHeaders,
        replyBody = replyBody
      )
    }
    def newReply(replyHeaders: Option[Map[String, String]],
                 replyBody: Option[String]): State =
      newReplyAt(replyHeaders, replyBody, Instant.now())

    private[reqreply] def newReplyAt(headers: Option[Map[String, String]],
                                     body: Option[String],
                                     on: Instant): State = {
      copy(
        idempotencyKey = idempotencyKey,
        requestedOn = requestedOn,
        repliedOn = Some(on),
        requestCount = requestCount,
        replyCount = replyCount + 1,
        requestHeaders = requestHeaders,
        requestBody = requestBody,
        replyHeaders = headers,
        replyBody = body
      )
    }

    def existingRequest(): State = {
      copy(
        idempotencyKey = idempotencyKey,
        requestedOn = requestedOn,
        repliedOn = repliedOn,
        requestCount = requestCount + 1,
        replyCount = replyCount,
        requestHeaders = requestHeaders,
        requestBody = requestBody,
        replyHeaders = replyHeaders,
        replyBody = replyBody
      )
    }

    def existingReply(): State = {
      copy(
        idempotencyKey = idempotencyKey,
        requestedOn = requestedOn,
        repliedOn = repliedOn,
        requestCount = requestCount,
        replyCount = replyCount + 1,
        requestHeaders = requestHeaders,
        requestBody = requestBody,
        replyHeaders = replyHeaders,
        replyBody = replyBody
      )
    }

    def toSummary: Summary = {
      Summary(
        idempotencyKey.getOrElse(""),
        isIdempotent,
        repliedOn,
        replyHeaders,
        replyBody
      )
    }

    def isIdempotent: Boolean = {
      isIdempotentRequest || isIdempotentReply
    }

    def isIdempotentRequest: Boolean =
      requestCount > 1

    def isIdempotentReply: Boolean =
      replyCount > 1
  }

  /** The repeat indicator is not a lifecycle status: isIdempotent reports whether
    * a request or reply has been repeated. repliedOn identifies a recorded reply,
    * including one whose body is empty.
    */
  final case class Summary(idempotencyKey: String,
                           isIdempotent: Boolean,
                           repliedOn: Option[Instant],
                           replyHeaders: Option[Map[String, String]],
                           replyBody: Option[String])

  object State {
    val empty: State =
      State(
        idempotencyKey = None,
        requestedOn = None,
        repliedOn = None,
        requestCount = 0,
        replyCount = 0,
        requestHeaders = None,
        requestBody = None,
        replyHeaders = None,
        replyBody = None
      )
  }

  val EntityKey: EntityTypeKey[Command] =
    EntityTypeKey[Command]("Idempotent")

  def init(system: ActorSystem[?]): Unit =
    init(system, ctx => apply(ctx.entityId))

  def init(system: ActorSystem[?],
           behaviorFactory: EntityContext[Command] => Behavior[Command]): Unit = {
    ClusterSharding(system).init(Entity(EntityKey)(behaviorFactory))
    ()
  }

  // Durability is a per-consumer choice: the default behavior remains
  // in-memory, while persistent uses the same transitions with native
  // EventAdapter and SnapshotAdapter values supplied by the consumer.
  // The journal schema and snapshot schema remain consumer-owned,
  // including for consumers that opt into crash-durable bookkeeping.
  def apply(idempotentKey: String): Behavior[Command] =
    idempotency(idempotentKey, State.empty)

  /** Journal-backed behavior using caller-owned storage adapters. idempotentKey
    * is the logical request key; persistenceId is its independently named storage
    * identity. The caller must keep that association stable across recovery.
    */
  def persistent[J](idempotentKey: String,
                    persistenceId: PersistenceId,
                    eventAdapter: EventAdapter[Event, J],
                    snapshotAdapter: SnapshotAdapter[State])
      : EventSourcedBehavior[Command, Event, State] =
    EventSourcedBehavior
      .withEnforcedReplies[Command, Event, State](
        persistenceId,
        State.empty,
        (state, command) =>
          eventFor(idempotentKey, state, command, Instant.now()) match {
            case Left(reason) =>
              Effect.reply(command.replyTo)(StatusReply.error(reason))
            case Right(None) =>
              Effect.reply(command.replyTo)(StatusReply.Success(state.toSummary))
            case Right(Some(event)) =>
              Effect
                .persist(event)
                .thenReply(command.replyTo)(next =>
                  StatusReply.Success(next.toSummary))
          },
        applyEvent
      )
      .eventAdapter(eventAdapter)
      .snapshotAdapter(snapshotAdapter)

  private[reqreply] def eventFor(key: String,
                                 state: State,
                                 command: Command,
                                 now: => Instant): Either[String, Option[Event]] =
    command match {
      case StatusRequest(_) => Right(None)
      case SingleRequest(_, _, _) if state.requestCount > 0 =>
        Right(Some(RequestRepeated))
      case SingleRequest(headers, body, _) =>
        Right(Some(RequestRecorded(key, now, headers, body)))
      case SingleReply(_, _, _) if state.requestCount == 0 =>
        Left("Invalid idempotent state")
      case SingleReply(_, _, _) if state.replyCount > 0 =>
        Right(Some(ReplyRepeated))
      case SingleReply(headers, body, _) =>
        Right(Some(ReplyRecorded(now, headers, body)))
      case AbandonRequest(_) if state.requestCount > 0 && state.replyCount == 0 =>
        Right(Some(RequestAbandoned))
      case AbandonRequest(_) => Right(None)
    }

  private[reqreply] def applyEvent(state: State, event: Event): State =
    event match {
      case RequestRecorded(key, on, headers, body) =>
        state.newRequestAt(key, headers, body, on)
      case RequestRepeated => state.existingRequest()
      case ReplyRecorded(on, headers, body) =>
        state.newReplyAt(headers, body, on)
      case ReplyRepeated => state.existingReply()
      case RequestAbandoned => State.empty
    }

  private def idempotencyResult(
    idempotentKey: String,
    newState: State,
    replyTo: ActorRef[StatusReply[Summary]]
  ): Behavior[Command] = {
    val idempotencyResult = idempotency(idempotentKey, newState)
    replyTo ! StatusReply.Success(newState.toSummary)
    idempotencyResult
  }

  private def idempotency(idempotentKey: String,
                          state: State): Behavior[Command] =
    Behaviors.receiveMessage { message =>
      eventFor(idempotentKey, state, message, Instant.now()) match {
        case Left(reason) =>
          message.replyTo ! StatusReply.error(reason)
          Behaviors.same
        case Right(None) =>
          message.replyTo ! StatusReply.Success(state.toSummary)
          Behaviors.same
        case Right(Some(event)) =>
          idempotencyResult(idempotentKey, applyEvent(state, event), message.replyTo)
      }
    }
}
