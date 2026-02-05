/*
 * Copyright 2026 Tomshley LLC.
 * All Rights Reserved.
 */

package com.tomshley.boilerplate.jvm.claimcheck

import scala.concurrent.{ExecutionContext, Future}

/**
 * Claim-Check Pattern (EIP)
 * 
 * Like a coat check: you check your item, get a ticket, continue without it.
 * When you need it back, present the ticket to claim it.
 * 
 * Use Cases:
 * - PCI Compliance: Strip card data at ingress, store securely, process with anonymized data
 * - PII Protection: Store sensitive PII separately, flow safe identifiers through saga
 * - GDPR Compliance: Isolated storage for right-to-erasure
 * - Large Payloads: Keep event journals small, store blobs externally
 * - Saga/Choreography: Safe data flows through services, re-materialize at boundaries
 * 
 * The pattern works with raw bytes - marshalling is the application's responsibility.
 */
trait ClaimCheck {
  
  /**
   * Check item - store data and get a claim ticket.
   * 
   * Call this at the boundary when data enters your system.
   * The returned ticket can safely flow through your system.
   * 
   * @param data The bytes to check (store securely)
   * @param tag Unique tag/correlation ID for this claim
   * @return Future containing the claim ticket
   */
  def check(data: Array[Byte], tag: String): Future[ClaimTicket]

  /**
   * Claim item back - retrieve data using the ticket.
   * 
   * Call this at the boundary when you need the original data back.
   * 
   * @param ticket The claim ticket
   * @return Future containing the original data, or None if not found/expired
   */
  def claim(ticket: ClaimTicket): Future[Option[Array[Byte]]]

  /**
   * Discard unclaimed item.
   * 
   * Use for GDPR erasure, cleanup after saga completion, etc.
   * 
   * @param ticket The claim ticket
   * @return Future indicating success
   */
  def discard(ticket: ClaimTicket): Future[Boolean]

  /**
   * Check if a claimed item exists without retrieving it.
   */
  def exists(ticket: ClaimTicket): Future[Boolean]
}

/**
 * ClaimCheck implementation backed by a ContentEnricher.
 */
class DefaultClaimCheck(
    contentEnricher: ContentEnricher,
    defaultLocation: String
)(using ec: ExecutionContext) extends ClaimCheck {

  override def check(data: Array[Byte], tag: String): Future[ClaimTicket] =
    contentEnricher.store(tag, data, defaultLocation)

  override def claim(ticket: ClaimTicket): Future[Option[Array[Byte]]] =
    contentEnricher.retrieve(ticket)

  override def discard(ticket: ClaimTicket): Future[Boolean] =
    contentEnricher.delete(ticket)

  override def exists(ticket: ClaimTicket): Future[Boolean] =
    contentEnricher.exists(ticket)
}

object ClaimCheck {
  /**
   * Create a ClaimCheck with the given ContentEnricher.
   */
  def apply(
      contentEnricher: ContentEnricher,
      location: String
  )(using ec: ExecutionContext): ClaimCheck =
    new DefaultClaimCheck(contentEnricher, location)
}
