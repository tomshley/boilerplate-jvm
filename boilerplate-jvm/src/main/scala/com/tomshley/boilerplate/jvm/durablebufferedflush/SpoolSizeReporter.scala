/*
 * Copyright 2026 Tomshley LLC.
 * All Rights Reserved.
 */

package com.tomshley.boilerplate.jvm.durablebufferedflush

import scala.concurrent.Future

/** Reports how many bytes the spool currently holds on disk.
  *
  * Sibling capability to [[ChunkSpool]] — split into a separate trait so that
  * downstream `ChunkSpool` implementations may opt in to size accounting
  * independently, and so that mocks for `ChunkSpool` need not stub a
  * size-reporting story they do not care about. The companion factory
  * [[ChunkSpool.filesystem]] returns the intersection type
  * `ChunkSpool & SpoolSizeReporter`, so a single concrete implementation
  * may satisfy both capabilities without forcing existing call sites that
  * widen to `ChunkSpool` to do anything.
  *
  * Owns:
  *   - the in-memory hot-path counter that is incremented on `write()` and
  *     decremented on `cleanup()`;
  *   - the authoritative `recountFromFilesystem` which corrects drift by
  *     walking the spool root.
  *
  * Does NOT own:
  *   - the threshold / hysteresis / level logic — that belongs to
  *     [[SpoolPressureMonitor]];
  *   - the act of refusing new sessions when the spool is full — that
  *     belongs to [[AdmissionController]].
  *
  * All methods are asynchronous. The counter is actor-owned — there is no
  * shared mutable state across threads, only message passing. The per-call
  * cost is one mailbox enqueue plus one [[scala.concurrent.Promise]]
  * allocation; both reads and writes are O(1). Reads are cheap enough to
  * sit on the [[SpoolPressureMonitor]]'s tick path; writes are cheap
  * enough to sit immediately after the chunk + meta fsyncs on the write
  * hot path (where the fsyncs themselves already dwarf the cost).
  */
trait SpoolSizeReporter {

  /** Total bytes currently on disk under the spool root, observed via the
    * size-accounting actor. The returned [[Future]] resolves with the
    * actor's current view of the count, which is incremented on chunk
    * write completion and decremented on entity cleanup.
    *
    * The observed value may briefly drift from the filesystem truth when
    * a write or cleanup is in flight (the increment / decrement message
    * is enqueued after the corresponding I/O lands but is processed
    * asynchronously by the actor). [[SpoolPressureMonitor]] reconciles
    * drift by calling [[recountFromFilesystem]] on its slow tick. */
  def currentSizeBytes(): Future[Long]

  /** Authoritative recount via filesystem traversal. O(N entities + M chunks)
    * — slow. Intended to be called by [[SpoolPressureMonitor]] on its
    * reconciliation cadence to correct any drift in the actor-owned
    * counter.
    *
    * Implementations are expected to perform the walk on a blocking-friendly
    * dispatcher and to return the post-walk authoritative byte count. After
    * the recount, implementations MAY (but are not required to) replace the
    * actor-owned counter with the recount value; whether they do is
    * documented on each implementation. */
  def recountFromFilesystem(): Future[Long]
}
