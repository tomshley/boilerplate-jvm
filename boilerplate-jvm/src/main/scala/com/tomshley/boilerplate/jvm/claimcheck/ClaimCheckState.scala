/*
 * Copyright 2026 Tomshley LLC.
 * All Rights Reserved.
 */

package com.tomshley.boilerplate.jvm.claimcheck

import com.tomshley.boilerplate.jvm.reqreply.CborSerializable

/**
 * State trait for claim-check entities.
 * 
 * Extend this in your domain-specific state to track claim tickets
 * in an EventSourcedBehavior. Parameterized on key type K so consumers
 * can use Int (chunk numbers), String (correlation IDs), etc.
 * 
 * Extracted from the two-phase pattern:
 * - Receive item metadata (pending)
 * - Store externally, get ClaimTicket (checked)
 * - Optionally claim (retrieve) or discard (delete)
 */
trait ClaimCheckState[K, Self <: ClaimCheckState[K, Self]] extends CborSerializable {
  
  /** Keys received but not yet stored externally */
  def pendingKeys: Set[K]
  
  /** Confirmed claim tickets: key -> ClaimTicket */
  def claims: Map[K, ClaimTicket]
  
  /** Keys that have been checked in (stored externally with a ticket) */
  def checkedKeys: Set[K] = claims.keySet
  
  /** True if the key has a confirmed claim ticket */
  def hasClaim(key: K): Boolean = claims.contains(key)
  
  /** True if the key is pending (received but not yet stored) */
  def isPending(key: K): Boolean = pendingKeys.contains(key)
  
  /** Keys received but not yet confirmed stored */
  def receivedButNotStored: Set[K] = pendingKeys -- checkedKeys
  
  /** Number of confirmed claim tickets */
  def claimsCount: Int = claims.size
  
  /** Total size in bytes across all claim tickets that report size */
  def totalClaimedBytes: Long = claims.values.flatMap(_.sizeBytes).sum
  
  /** Progress as a fraction: checked / totalExpected */
  def progress(totalExpected: Int): Double = {
    if (totalExpected > 0) checkedKeys.size.toDouble / totalExpected
    else 0.0
  }
  
  /** True if all expected keys have been checked in */
  def allChecked(expectedKeys: Set[K]): Boolean =
    expectedKeys.subsetOf(checkedKeys)

  /** Return new state with key added to pending */
  def withPendingKey(key: K): Self
  
  /** Return new state with confirmed claim ticket */
  def withClaim(key: K, ticket: ClaimTicket): Self

  /** Return new state with key removed from pending and claims (discard/erasure) */
  def withDiscarded(key: K): Self
}
