/*
 * Copyright 2026 Tomshley LLC.
 * All Rights Reserved.
 */

package com.tomshley.boilerplate.jvm.durablebufferedflush

import com.tomshley.boilerplate.jvm.durablebufferedflush.internal.AdmissionControllerImpl
import org.apache.pekko.actor.typed.ActorSystem

import scala.concurrent.Future

/** Single-bit gate that [[Workflow.prepareTransfer]] queries to decide
  * whether to admit new sessions.
  *
  * Owns:
  *   - the open/closed flag as actor-private state — no shared mutable
  *     memory, no atomics; every transition and read flows through a
  *     typed message;
  *   - idempotent `open` / `close` mutations so that the same level
  *     transition delivered twice causes no surprise.
  *
  * Does NOT own:
  *   - the decision of when to open or close — that lives with whatever
  *     subscribes to [[SpoolPressureMonitor.onLevelChange]];
  *   - the storage measurement — that belongs to [[SpoolSizeReporter]].
  *
  * In-flight sessions: never gated. The gate is queried exactly once per
  * session at admission time. A session that has already been admitted
  * continues to flow even if the controller closes mid-session — closing
  * a stream that has already paid the fixed cost of admission would
  * sacrifice durable in-flight bytes for a small reduction in pressure
  * and is the wrong trade.
  *
  * All methods are asynchronous: state lives in an actor, callers see a
  * `Future` reply. [[Workflow.prepareTransfer]] is already `Future`-shaped,
  * so the admission read folds into its existing `for` comprehension with
  * zero ceremony. Per-call cost is one mailbox enqueue plus one
  * [[scala.concurrent.Promise]] allocation; the path is intentionally NOT
  * the per-chunk hot path.
  *
  * Lifecycle: the backing actor is spawned in [[AdmissionController.apply]]
  * and lives for the lifetime of the supplied [[ActorSystem]]. No
  * `start` / `stop` is required — teardown is tied to system shutdown. */
trait AdmissionController {

  /** Async read of the gate. Resolves `true` when admission is open and
    * the workflow may admit a new session, `false` when it must refuse. */
  def isOpen(): Future[Boolean]

  /** Idempotent. Closes admission and records `reason` at WARN on the
    * open → closed edge. A `close` while already closed is a no-op (no
    * log, no churn). The returned `Future` completes once the gate actor
    * has acknowledged the transition. */
  def close(reason: String): Future[Unit]

  /** Idempotent. Opens admission and records the transition at INFO on
    * the closed → open edge. An `open` while already open is a no-op.
    * The returned `Future` completes once the gate actor has
    * acknowledged the transition. */
  def open(): Future[Unit]
}

object AdmissionController {

  /** Default-construct an [[AdmissionController]] backed by an internal
    * actor that owns the gate state. Logs transitions through the actor's
    * context logger. */
  def apply(system: ActorSystem[?]): AdmissionController =
    new AdmissionControllerImpl(system)

  /** No-op gate — admission is permanently open. Used as the default
    * argument for [[Workflow]] callers that have not wired a real pressure
    * monitor, so that the workflow path remains backward-compatible. The
    * sentinel never logs, holds no state, and never spawns an actor. */
  case object AlwaysOpen extends AdmissionController {
    private val openFuture: Future[Boolean] = Future.successful(true)
    override def isOpen(): Future[Boolean] = openFuture
    override def close(reason: String): Future[Unit] = Future.unit
    override def open(): Future[Unit] = Future.unit
  }
}
