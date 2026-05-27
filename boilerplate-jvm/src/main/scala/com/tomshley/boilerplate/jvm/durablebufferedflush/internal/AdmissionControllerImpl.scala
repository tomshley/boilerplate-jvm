/*
 * Copyright 2026 Tomshley LLC.
 * All Rights Reserved.
 */

package com.tomshley.boilerplate.jvm.durablebufferedflush.internal

import com.tomshley.boilerplate.jvm.durablebufferedflush.AdmissionController
import org.apache.pekko.actor.typed.{ActorSystem, Behavior}
import org.apache.pekko.actor.typed.scaladsl.Behaviors
import org.slf4j.Logger

import java.util.UUID
import scala.concurrent.{Future, Promise}

private object AdmissionGateActor {
  sealed trait Command
  final case class IsOpen(reply: Promise[Boolean]) extends Command
  final case class Open(reply: Promise[Unit]) extends Command
  final case class Close(reason: String, reply: Promise[Unit]) extends Command

  /** Spawn point. Default state is open — a wired pressure monitor flips
    * this on the first Critical transition. */
  def apply(): Behavior[Command] =
    Behaviors.setup { context =>
      active(open = true, context.log)
    }

  /** Behavior recursion holds the gate flag as a plain `Boolean`
    * parameter — no `var`, no atomic, no shared memory. Each transition
    * rebinds the next behavior with the new value.
    *
    * Idempotency: both `Open` and `Close` log only on an edge transition.
    * A `Close` while already closed simply replies and stays put, so a
    * monitor subscriber that fires every tick at `Critical` causes no
    * log churn and no behavior reallocation. */
  private def active(open: Boolean, log: Logger): Behavior[Command] =
    Behaviors.receiveMessage {
      case IsOpen(reply) =>
        reply.trySuccess(open)
        Behaviors.same

      case Open(reply) =>
        reply.trySuccess(())
        if (open) Behaviors.same
        else {
          log.info("AdmissionController OPEN")
          active(open = true, log)
        }

      case Close(reason, reply) =>
        reply.trySuccess(())
        if (!open) Behaviors.same
        else {
          log.warn("AdmissionController CLOSED: {}", reason)
          active(open = false, log)
        }
    }
}

/** Default actor-backed implementation of [[AdmissionController]].
  *
  * State: a single `Boolean` carried as a parameter of
  * [[AdmissionGateActor]]'s behavior recursion. There is no shared
  * mutable memory, no `var`, no atomic — every transition produces a
  * new behavior bound to the next value.
  *
  * Cost per call: one mailbox enqueue plus one [[Promise]] allocation.
  * The admission read is intentionally NOT the per-chunk hot path
  * (see [[AdmissionController]] for the rationale): it runs once per
  * session in
  * [[com.tomshley.boilerplate.jvm.durablebufferedflush.Workflow.prepareTransfer]].
  *
  * Naming: the actor name is suffixed with a UUID so that multiple
  * controllers in the same system never collide. The actor is spawned
  * as a system actor so it shares the lifecycle of the supplied
  * [[ActorSystem]] and requires no explicit `stop`. */
final class AdmissionControllerImpl(system: ActorSystem[?]) extends AdmissionController {

  private val gate = system.systemActorOf(
    AdmissionGateActor(),
    s"admission-controller-${UUID.randomUUID()}"
  )

  override def isOpen(): Future[Boolean] = {
    val promise = Promise[Boolean]()
    gate ! AdmissionGateActor.IsOpen(promise)
    promise.future
  }

  override def close(reason: String): Future[Unit] = {
    val promise = Promise[Unit]()
    gate ! AdmissionGateActor.Close(reason, promise)
    promise.future
  }

  override def open(): Future[Unit] = {
    val promise = Promise[Unit]()
    gate ! AdmissionGateActor.Open(promise)
    promise.future
  }
}
