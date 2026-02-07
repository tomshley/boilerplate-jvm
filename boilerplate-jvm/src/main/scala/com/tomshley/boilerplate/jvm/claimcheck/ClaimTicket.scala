/*
 * Copyright 2026 Tomshley LLC.
 * All Rights Reserved.
 */

package com.tomshley.boilerplate.jvm.claimcheck

import com.tomshley.boilerplate.jvm.reqreply.CborSerializable
import java.time.Instant

/**
 * A claim ticket - the reference to stored data.
 * 
 * Like a coat check ticket: you check your item, get a ticket, continue without it.
 * When you need it back, present the ticket to claim it.
 */
case class ClaimTicket(
    number: String,
    location: String,
    storeType: String,
    checkedAt: Option[Instant] = None,
    expiresAt: Option[Instant] = None,
    sizeBytes: Option[Long] = None
) extends CborSerializable

object ClaimTicket {
  /**
   * Create a claim ticket with timestamp.
   */
  def withTimestamp(
      number: String,
      location: String,
      storeType: String
  ): ClaimTicket = ClaimTicket(
    number = number,
    location = location,
    storeType = storeType,
    checkedAt = Some(Instant.now())
  )
}
