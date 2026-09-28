package com.tomshley.boilerplate.jvm.reqreply

import org.apache.pekko.actor.typed.scaladsl.Behaviors
import org.apache.pekko.actor.typed.{ActorRef, ActorSystem, Behavior}
import org.apache.pekko.cluster.sharding.typed.scaladsl.{ClusterSharding, Entity, EntityContext, EntityTypeKey}
import org.apache.pekko.pattern.StatusReply

import java.time.Instant
import scala.concurrent.ExecutionContextExecutor

/** Plain wire types — boilerplate ships no serializer binding. The
 *  consuming service owns serialization and binds `Idempotent$Command`
 *  (plus `Idempotent$Summary` if sent standalone) in its own config.
 *
 *  Durability is likewise the consumer's choice. This entity's bookkeeping
 *  is in-memory only; a consumer that needs crash-durable idempotency runs
 *  its own `EventSourcedBehavior` over consumer-owned events and state —
 *  [[State]] and its transition methods are the reusable pure core, the
 *  same lift rule applied to `ClaimTicket` at the persistence boundary:
 *
 * {{{
 *   // The journaled contract is consumer-owned — this protocol's types
 *   // never persist. Transitions reuse the library's pure core:
 *   def applyEvent(s: MyIdempotencyState, e: MyEvent): MyIdempotencyState =
 *     e match {
 *       case RequestRecorded(key, _)   => MyIdempotencyState.fromLib(s.toLib.newRequest(key, None, None))
 *       case ReplyRecorded(h, b)       => MyIdempotencyState.fromLib(s.toLib.newReply(h, b))
 *       case Abandoned(_) if s.toLib.replyCount == 0 =>
 *         MyIdempotencyState.fromLib(Idempotent.State.empty)
 *       case Abandoned(_)              => s
 *     }
 * }}}
 */
object Idempotent {
  sealed trait Command


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
                   requestBody: Option[String]): State = {
      copy(
        idempotencyKey = Some(idempotencyKey),
        requestedOn = Some(Instant.now()),
        repliedOn = repliedOn,
        requestCount = requestCount + 1,
        replyCount = replyCount,
        requestHeaders = requestHeaders,
        requestBody = requestBody,
        replyHeaders = replyHeaders,
        replyBody = replyBody
      )
    }
    def newReply(replyHeaders: Option[Map[String, String]],
                 replyBody: Option[String]): State = {
      copy(
        idempotencyKey = idempotencyKey,
        requestedOn = requestedOn,
        repliedOn = Some(Instant.now()),
        requestCount = requestCount,
        replyCount = replyCount + 1,
        requestHeaders = requestHeaders,
        requestBody = requestBody,
        replyHeaders = replyHeaders,
        replyBody = replyBody
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

  def init(system: ActorSystem[?]): Unit = {
    given ec: ExecutionContextExecutor = system.executionContext

    val behaviorFactory: EntityContext[Command] => Behavior[Command] = {
      entityContext =>
        Idempotent(entityContext.entityId)
    }
    ClusterSharding(system).init(Entity(EntityKey)(behaviorFactory))
  }

  // Durability is a per-consumer choice: a consumer that needs a
  // crash-durable entity builds its own EventSourcedBehavior over
  // consumer-owned events/state, reusing State's transitions. The library
  // migrating this behavior itself would force a shared journal contract
  // on every consumer — including ones that want in-memory only.
  def apply(idempotentKey: String): Behavior[Command] =
    idempotency(idempotentKey, State.empty)

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
    Behaviors.receive { (context, message) =>
      message match {
        case StatusRequest(replyTo) =>
          replyTo ! StatusReply.Success(state.toSummary)
          Behaviors.same
        case SingleRequest(headers, body, replyTo) =>
          if (state.requestCount > 0) {
            idempotencyResult(idempotentKey, state.existingRequest(), replyTo)
          } else {
            idempotencyResult(
              idempotentKey,
              state.newRequest(idempotentKey, headers, body),
              replyTo
            )
          }
        case AbandonRequest(replyTo) =>
          idempotencyResult(
            idempotentKey,
            if (state.replyCount == 0) State.empty else state,
            replyTo
          )
        case SingleReply(headers, body, replyTo) =>
          if (state.requestCount == 0) {
            replyTo ! StatusReply.error("Invalid idempotent state")
            Behaviors.same
          } else if (state.replyCount > 0) {
            idempotencyResult(idempotentKey, state.existingReply(), replyTo)
          } else {
            idempotencyResult(
              idempotentKey,
              state.newReply(headers, body),
              replyTo
            )
          }
      }
    /*
      UNDER CONSTRUCTION
     */
    //      context.setReceiveTimeout(30.seconds, Idle)
    }
}
