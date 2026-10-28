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

/** Content-integrity outcome of [[Workflow.finalizeTransfer]].
  *
  * The workflow maintains an incremental SHA-256 over every accepted chunk
  * (state checkpointed in [[SpoolMeta.digestStateHex]] under the same atomic
  * meta rename that advances `lastSpooledSeq`, so digest coverage and the
  * spool watermark can never diverge — including across process restarts).
  * At finalize the digest is compared against the declared object hash and
  * the comparison is surfaced here as a value on
  * [[FlushFinalizationResult.outcome]].
  *
  * This is errors-as-values: a mismatch is a DOMAIN FACT for the caller's
  * policy layer (log it, meter it, reject the transfer, reset the entity),
  * never an exception. Exceptions out of `finalizeTransfer` remain reserved
  * for true faults — I/O failures, drain shortfalls, close-barrier
  * exhaustion. The workflow itself imposes NO policy: what a mismatch means
  * is decided by the caller via [[HashMismatchDirective]].
  */
enum FinalizeOutcome {

  /** The recomputed content hash equals the declared object hash. */
  case Verified(hashHex: String)

  /** The recomputed content hash differs from the declared object hash.
    * Whether the session still closed is decided by the
    * [[HashMismatchDirective]] the caller passed to `finalizeTransfer`.
    */
  case HashMismatch(declaredHex: String, actualHex: String)

  /** No verification verdict exists for this finalize. */
  case Unverified(reason: FinalizeOutcome.UnverifiedReason)
}

object FinalizeOutcome {

  enum UnverifiedReason {

    /** The result was constructed without a verification pass — the
      * default for [[FlushFinalizationResult]] values built directly
      * (test stubs, non-verifying workflow implementations).
      */
    case NotEvaluated

    /** Chunks were spooled without digest coverage — the spool meta
      * predates digest checkpointing (written by an earlier version) so
      * recomputing the hash from the midstate would be silently wrong.
      * Surfaced explicitly instead of fabricating a false mismatch.
      */
    case NoDigestCoverage
  }
}

/** Caller-supplied directive: what should `finalizeTransfer` do with the
  * session when the content hash mismatches?
  *
  * This is policy-as-data. The workflow knows the MECHANISM (close or hold
  * open); the caller owns the POLICY (e.g. a shadow/enforce rollout switch)
  * and expresses it as a value per finalize call — the workflow never
  * learns the caller's configuration vocabulary.
  */
enum HashMismatchDirective {

  /** Close the session regardless of the verdict and surface the outcome.
    * The compatible default — observation-only callers (shadow rollouts,
    * metrics) keep today's close semantics.
    */
  case CloseAnyway

  /** On mismatch: do NOT close, do NOT clean up the spool. The flusher is
    * stopped, the claim binding is released, and the outcome is returned;
    * the session entity and spool are left exactly as they were so the
    * caller can decide — typically [[Workflow.resetTransfer]] followed by a
    * transport-level rejection, letting the producer retry from sequence
    * zero.
    */
  case HoldOpen
}
