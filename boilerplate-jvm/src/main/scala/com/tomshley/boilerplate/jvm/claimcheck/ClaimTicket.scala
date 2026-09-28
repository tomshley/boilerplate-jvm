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
 *
 * Boundary value type with no serializer binding. A consumer that persists
 * ticket-shaped data lifts it into a domain type it owns — at the port or
 * handler boundary, before it reaches an entity command, event, or the
 * journal — so library class names never appear in its serialized contract:
 *
 * {{{
 * final case class SessionClaim(number: String, location: String,
 *                               storeType: String, checkedAt: Instant,
 *                               sequence: Long,
 *                               expiresAt: Option[Instant] = None)
 * object SessionClaim {
 *   def fromTicket(t: ClaimTicket, sequence: Long): SessionClaim =
 *     SessionClaim(t.number, t.location, t.storeType,
 *                  t.checkedAt.getOrElse(Instant.now()), sequence, t.expiresAt)
 * }
 *
 * // port:  check() -> Future[ClaimTicket] -> SessionClaim -> entity command
 * // journal: the event carries SessionClaim; ClaimTicket is never persisted
 * }}}
 */
case class ClaimTicket(
    number: String,
    location: String,
    storeType: String,
    checkedAt: Option[Instant] = None,
    expiresAt: Option[Instant] = None,
    sizeBytes: Option[Long] = None
)

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
