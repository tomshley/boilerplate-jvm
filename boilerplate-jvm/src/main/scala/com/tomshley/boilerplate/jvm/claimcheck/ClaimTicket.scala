/*
 * Copyright 2026 Tomshley LLC.
 * All Rights Reserved.
 */

package com.tomshley.boilerplate.jvm.claimcheck

import java.time.Instant

/**
 * A claim ticket - the reference to stored data.
 * 
 * Like a coat check ticket: you check your item, get a ticket, continue without it.
 * When you need it back, present the ticket to claim it.
 */
trait ClaimTicket {
  /** Unique ticket number */
  def number: String
  
  /** Storage location (bucket, topic, etc.) */
  def location: String
  
  /** Type of storage backend */
  def storeType: String
  
  /** When the item was checked */
  def checkedAt: Option[Instant] = None
  
  /** Optional expiration */
  def expiresAt: Option[Instant] = None
  
  /** Optional size in bytes */
  def sizeBytes: Option[Long] = None
}

/**
 * Simple claim ticket implementation.
 */
case class SimpleClaimTicket(
    number: String,
    location: String,
    storeType: String,
    override val checkedAt: Option[Instant] = None,
    override val expiresAt: Option[Instant] = None,
    override val sizeBytes: Option[Long] = None
) extends ClaimTicket

object ClaimTicket {
  /**
   * Create a simple claim ticket.
   */
  def apply(
      number: String,
      location: String,
      storeType: String
  ): ClaimTicket = SimpleClaimTicket(number, location, storeType)
  
  /**
   * Create a claim ticket with timestamp.
   */
  def withTimestamp(
      number: String,
      location: String,
      storeType: String
  ): ClaimTicket = SimpleClaimTicket(
    number = number,
    location = location,
    storeType = storeType,
    checkedAt = Some(Instant.now())
  )
}
