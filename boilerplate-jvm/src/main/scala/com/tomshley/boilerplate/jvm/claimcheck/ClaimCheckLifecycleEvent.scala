/*
 * Copyright 2026 Tomshley LLC.
 * All Rights Reserved.
 */

package com.tomshley.boilerplate.jvm.claimcheck

import java.time.Instant
import com.tomshley.boilerplate.jvm.reqreply.CborSerializable

/**
 * Common lifecycle events for claim-check entities.
 * 
 * These events represent the two-phase claim-check lifecycle:
 * - Receive item metadata (pending, raw data not in journal)
 * - Confirm item stored externally with a ClaimTicket
 * - Optionally claim (retrieve) or discard (delete/erasure)
 * 
 * Extend these in your domain-specific event hierarchy.
 */
sealed trait ClaimCheckLifecycleEvent extends CborSerializable

object ClaimCheckLifecycleEvent {
  
  /** Item metadata received, raw data not yet stored externally */
  case class ItemReceived(
      key: String,
      sizeBytes: Long,
      crc32: Long,
      receivedAt: Instant
  ) extends ClaimCheckLifecycleEvent

  /** Item confirmed stored externally with a ClaimTicket */
  case class ItemChecked(
      key: String,
      claimTicket: ClaimTicket,
      checkedAt: Instant
  ) extends ClaimCheckLifecycleEvent

  /** Item retrieved/re-materialized using its ClaimTicket */
  case class ItemClaimed(
      key: String,
      claimedAt: Instant
  ) extends ClaimCheckLifecycleEvent

  /** Item discarded (GDPR erasure, saga cleanup, expiration) */
  case class ItemDiscarded(
      key: String,
      discardedAt: Instant
  ) extends ClaimCheckLifecycleEvent
}
