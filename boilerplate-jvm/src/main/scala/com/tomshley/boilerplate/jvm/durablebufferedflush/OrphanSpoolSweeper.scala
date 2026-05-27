/*
 * Copyright 2026 Tomshley LLC.
 * All Rights Reserved.
 */

package com.tomshley.boilerplate.jvm.durablebufferedflush

import com.tomshley.boilerplate.jvm.durablebufferedflush.internal.OrphanSpoolSweeperImpl
import org.apache.pekko.actor.typed.ActorSystem

import scala.concurrent.Future

/** Periodic background reconciliation of orphan spool entities to durable
  * storage.
  *
  * The sweeper exists to decouple the durability guarantee from the storage
  * substrate. With the sweeper running, a deployment whose spool directory
  * lives on ephemeral storage can still actively drain in-flight buffered
  * data to durable storage on a configurable cadence — the retransmission
  * window for upstream producers shrinks to the sweep interval rather than
  * spanning the lifetime of the process.
  *
  * Runtime posture:
  *   - the sweeper schedules a tick on the system scheduler, not on a
  *     request-serving dispatcher; ticks fire-and-forget the reconciliation
  *     work onto the system execution context
  *   - per-tick work is delegated to [[OrphanReconciler.reconcileOrphans]],
  *     which honors the underlying flush configuration's `recovery.parallelism`
  *     for per-entity batching
  *   - overlapping ticks are skipped via an in-memory re-entry guard; a slow
  *     sweep cannot pile up against a fast cadence
  *
  * Safety contract:
  *   - the caller MUST supply an `isActive` predicate that returns true for
  *     any entity currently bound to an in-process flusher, lag monitor, or
  *     finalization gate slot. The sweeper passes this through verbatim to
  *     [[OrphanReconciler.reconcileOrphans]] and never touches an active
  *     entity.
  *   - `start()` and `stop()` are idempotent and safe to call concurrently.
  */
trait OrphanSpoolSweeper {

  /** Start the periodic sweep. Idempotent: a second `start` while already
    * running is a no-op and returns a completed future. When the underlying
    * config has `enabled = false`, `start` returns immediately without
    * scheduling. */
  def start(): Future[Unit]

  /** Stop the sweeper. Idempotent. Cancels any scheduled tick; an in-flight
    * sweep is allowed to complete naturally (its result is logged and
    * discarded). */
  def stop(): Future[Unit]
}

object OrphanSpoolSweeper {

  /** Build a sweeper bound to the given orphan reconciler.
    *
    * @param reconciler  the reconciler whose `reconcileOrphans` is invoked per
    *                    tick. Typically a [[RecoveryManager]] implementation
    *                    that also implements [[OrphanReconciler]] — see
    *                    `RecoveryManager.fromConfig` for the default wiring.
    * @param isActive    predicate returning true for any entity currently
    *                    bound in-process; MUST be consistent across calls
    *                    (e.g. an `ask` against the workflow's own
    *                    finalization-gate actor, or any other actor-owned
    *                    registry that observes the workflow's
    *                    open / close / abort transitions). The sweeper
    *                    never touches an entity for which this returns true.
    * @param config      sweeper schedule and gating config
    * @param system      actor system, used for the scheduler and logger */
  def apply(
      reconciler: OrphanReconciler,
      isActive: String => Future[Boolean],
      config: FlushSweeperConfig,
      system: ActorSystem[?]
  ): OrphanSpoolSweeper =
    new OrphanSpoolSweeperImpl(
      reconciler = reconciler,
      isActive = isActive,
      config = config,
      system = system
    )
}
