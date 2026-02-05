/*
 * Copyright 2026 Tomshley LLC.
 * All Rights Reserved.
 */

package com.tomshley.boilerplate.jvm.claimcheck

import scala.concurrent.Future

/**
 * Content Enricher (EIP) - Storage adapter for the Claim-Check pattern.
 * 
 * A ContentEnricher stores data and returns claim tickets, then retrieves
 * (enriches/re-materializes) the data when the ticket is presented.
 * 
 * Implementations:
 * - InMemoryContentEnricher - For testing
 * - S3ContentEnricher - S3/MinIO (depends on boilerplate-storage)
 * - Future: KafkaContentEnricher, RedisContentEnricher, VaultContentEnricher
 */
trait ContentEnricher {
  /** Type identifier for this storage backend */
  def storeType: String
  
  /**
   * Store data and return a claim ticket.
   * 
   * @param key Unique key for this data
   * @param data The bytes to store
   * @param location Storage location (bucket, topic, etc.)
   * @return Future containing the claim ticket
   */
  def store(
      key: String,
      data: Array[Byte],
      location: String
  ): Future[ClaimTicket]

  /**
   * Retrieve (enrich/re-materialize) data using a claim ticket.
   * 
   * @param ticket The claim ticket
   * @return Future containing the data, or None if not found/expired
   */
  def retrieve(ticket: ClaimTicket): Future[Option[Array[Byte]]]

  /**
   * Delete stored data.
   * 
   * @param ticket The claim ticket
   * @return Future indicating success
   */
  def delete(ticket: ClaimTicket): Future[Boolean]

  /**
   * Check if data exists without retrieving it.
   * 
   * @param ticket The claim ticket
   * @return Future indicating existence
   */
  def exists(ticket: ClaimTicket): Future[Boolean]
}
