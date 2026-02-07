/*
 * Copyright 2026 Tomshley LLC.
 * All Rights Reserved.
 */

package com.tomshley.boilerplate.jvm.claimcheck

import java.time.Instant

/**
 * Boilerplate trait for claim-check eventsourced entities.
 * 
 * Provides common event handling logic for the two-phase claim-check pattern.
 * Extend this in your domain-specific entity behavior.
 * 
 * Usage:
 * {{{
 * object MyEntity extends ClaimCheckBoilerplate {
 *   // Your domain-specific commands and events
 *   // Use applyClaimCheckEvent to handle lifecycle events
 * }
 * }}}
 */
trait ClaimCheckBoilerplate {

  /**
   * Apply a claim-check lifecycle event to state.
   * Delegates to the abstract methods on ClaimCheckState.
   * 
   * @param state Current state (must extend ClaimCheckState)
   * @param event The lifecycle event to apply
   * @return Updated state
   */
  def applyClaimCheckEvent[K, S <: ClaimCheckState[K, S]](
      state: S,
      event: ClaimCheckLifecycleEvent
  )(using keyFromString: String => K): S = {
    import ClaimCheckLifecycleEvent._
    
    event match {
      case ItemReceived(key, _, _, _) =>
        state.withPendingKey(keyFromString(key))
        
      case ItemChecked(key, claimTicket, _) =>
        state.withClaim(keyFromString(key), claimTicket)
        
      case ItemClaimed(_, _) =>
        // Claiming doesn't change state by default;
        // override applyClaimCheckEvent if your domain needs to track claims
        state
        
      case ItemDiscarded(key, _) =>
        state.withDiscarded(keyFromString(key))
    }
  }
  
  /** Create ItemReceived event */
  def itemReceivedEvent(key: String, sizeBytes: Long, crc32: Long): ClaimCheckLifecycleEvent.ItemReceived =
    ClaimCheckLifecycleEvent.ItemReceived(key, sizeBytes, crc32, Instant.now())
  
  /** Create ItemChecked event */
  def itemCheckedEvent(key: String, claimTicket: ClaimTicket): ClaimCheckLifecycleEvent.ItemChecked =
    ClaimCheckLifecycleEvent.ItemChecked(key, claimTicket, Instant.now())
  
  /** Create ItemClaimed event */
  def itemClaimedEvent(key: String): ClaimCheckLifecycleEvent.ItemClaimed =
    ClaimCheckLifecycleEvent.ItemClaimed(key, Instant.now())
  
  /** Create ItemDiscarded event */
  def itemDiscardedEvent(key: String): ClaimCheckLifecycleEvent.ItemDiscarded =
    ClaimCheckLifecycleEvent.ItemDiscarded(key, Instant.now())
}
