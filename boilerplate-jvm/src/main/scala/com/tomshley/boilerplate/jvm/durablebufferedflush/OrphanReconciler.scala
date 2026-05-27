/*
 * Copyright 2026 Tomshley LLC.
 * All Rights Reserved.
 */

package com.tomshley.boilerplate.jvm.durablebufferedflush

import scala.concurrent.Future

/** Steady-state reconciliation of spool-resident entities to durable storage.
  *
  * Separate sibling capability to [[RecoveryManager]]:
  *   - `RecoveryManager.recover()` is a one-shot startup pass and assumes no
  *     in-memory bindings exist.
  *   - `OrphanReconciler.reconcileOrphans()` is the steady-state pass that
  *     respects in-memory bindings via an `isActive` predicate, and is safe
  *     to invoke periodically while the process is serving traffic.
  *
  * The two capabilities are intentionally separate traits so that:
  *   - implementations may opt into either, both, or neither;
  *   - mocks for one capability do not need to stub the other;
  *   - callers that only need recovery (e.g. startup wiring) can depend on
  *     the narrower [[RecoveryManager]] type and remain decoupled from any
  *     sweeping behavior.
  */
trait OrphanReconciler {

  /** Reconcile spool-resident entities against durable storage.
    *
    * Lists every entity with a spool directory, filters out any entity for
    * which `isActive` returns true (i.e. there is an in-memory binding
    * currently accepting input or finalizing a transfer in this process),
    * and reconciles the remaining orphans by:
    *   - draining residual buffered data to durable storage,
    *   - closing complete sessions,
    *   - aborting / cleaning sessions that cannot be recovered.
    *
    * The `isActive` predicate is evaluated once per entity per pass and is
    * the caller's safety contract: it MUST return true for any entity
    * currently bound to an in-process flusher, lag monitor, or finalization
    * gate slot. Implementations pass the predicate through verbatim — false
    * negatives produce racing reconciliation; false positives produce
    * stalled cleanup.
    *
    * `isActive` should be cheap and side-effect-free. Implementations are
    * permitted to invoke it for multiple entities concurrently (up to an
    * implementation-defined cap, typically the recovery parallelism), so an
    * expensive predicate amplifies its cost across the spool population.
    *
    * Idempotent: a second pass over an already-drained orphan is a cheap
    * no-op (the underlying cleanup removes the directory, the next list
    * returns nothing).
    *
    * @param isActive predicate returning true for any entity that must not
    *                 be touched by reconciliation this pass
    * @return         a [[RecoveryReport]] summarizing the pass.
    *                 Per-entity failures are contained and reflected in
    *                 `sessionsFailed`. Pass-level failures (e.g. transient
    *                 IO errors while listing entities) propagate as a
    *                 failed Future — callers operating the reconciler in a
    *                 loop (such as [[OrphanSpoolSweeper]]) are expected to
    *                 log and continue. */
  def reconcileOrphans(isActive: String => Future[Boolean]): Future[RecoveryReport]
}
