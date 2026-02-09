/*
 * Copyright 2026 Tomshley LLC.
 * All Rights Reserved.
 */

package com.tomshley.boilerplate.jvm.claimcheck

import com.tomshley.boilerplate.jvm.objectstorage.BlobStoreBoilerplate
import com.tomshley.boilerplate.jvm.objectstorage.models.{BlobReference, PartETag, UploadSession}
import org.apache.pekko.stream.scaladsl.Source
import org.apache.pekko.util.ByteString

import java.time.Instant
import scala.collection.concurrent.TrieMap
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.util.Failure
import scala.util.control.NonFatal

/**
 * Adapter trait bridging the Claim-Check pattern with blob storage.
 *
 * Manages multipart upload sessions and part accumulation, producing
 * ClaimTicket references for each stored item. Implementors provide the
 * underlying BlobStoreBoilerplate, bucket, store type, and key generation.
 *
 * Usage:
 * {{{
 * object MyBlobProvider extends ClaimCheckStorageAdapter {
 *   protected val store: BlobStoreBoilerplate = new InMemoryBlobStoreBoilerplate()
 *   protected val defaultBucket: String = "my-bucket"
 *   protected val storeType: String = "memory"
 *   protected def transferObjectKey(transferId: String): String =
 *     s"transfers/$transferId/complete"
 *   protected def locationUri(bucket: String, itemKey: String): String =
 *     s"s3://$bucket/$itemKey"
 * }
 *
 * val ticket: Future[ClaimTicket] =
 *   MyBlobProvider.storeItem("txn-1", "transfers/txn-1/part-1", 1, Source.single(data))
 * }}}
 *
 * ==Transfer lifecycle==
 * Every transfer started via `storeItem` must eventually be finalized by calling
 * either `completeTransfer` or `abortTransfer`. Failure to do so leaks in-memory
 * session state (and, with real S3 backends, incomplete multipart uploads that
 * incur storage charges until S3 lifecycle policies reap them). Implementors
 * targeting production should consider an external reaper or TTL mechanism.
 */
trait ClaimCheckStorageAdapter {

  // --- Abstract: implementors provide these ---

  /** The underlying blob store implementation */
  protected def store: BlobStoreBoilerplate

  /** Default bucket for storage operations */
  protected def defaultBucket: String

  /** Storage backend identifier (e.g. "memory", "s3") used in ClaimTicket.storeType */
  protected def storeType: String

  /** Key for the assembled multipart object in the blob store */
  protected def transferObjectKey(transferId: String): String

  /** URI for the ClaimTicket location (e.g. s3://bucket/key, mem://bucket/key) */
  protected def locationUri(bucket: String, itemKey: String): String

  // --- Session and part state ---

  private val uploadSessions: TrieMap[String, Future[UploadSession]] = TrieMap.empty
  private val reservedParts: TrieMap[String, TrieMap[Int, Boolean]] = TrieMap.empty
  private val uploadedParts: TrieMap[String, TrieMap[Int, PartETag]] = TrieMap.empty

  // --- Concrete operations ---

  /**
   * Store an item and return a ClaimTicket reference.
   *
   * Lazily initiates a multipart upload session for the transfer on first call,
   * uploads the item as a part, accumulates the PartETag for later completion,
   * and returns a ClaimTicket.
   *
   * @param transferId Logical grouping key (e.g. a file transfer ID)
   * @param itemKey    Storage key for the individual item (used in ClaimTicket)
   * @param itemNo     Part number within the transfer (must be unique per transfer)
   * @param data       Item data as a Pekko Source
   * @return Future containing the ClaimTicket reference
   */
  def storeItem(
      transferId: String,
      itemKey: String,
      itemNo: Int,
      data: Source[ByteString, ?]
  )(using ec: ExecutionContext): Future[ClaimTicket] = {
    val bucket = defaultBucket

    // Reserve the itemNo atomically before uploading — prevents TOCTOU race
    val reserved = reservedParts.getOrElseUpdate(transferId, TrieMap.empty)
    if (reserved.putIfAbsent(itemNo, true).isDefined) {
      Future.failed(new IllegalStateException(
        s"Duplicate itemNo $itemNo for transfer $transferId — each item number must be unique within a transfer"
      ))
    } else {
      val sessionFuture = uploadSessions.getOrElseUpdate(transferId, {
        val f = store.initiateUpload(bucket, transferObjectKey(transferId))
        f.onComplete {
          case Failure(_) => uploadSessions.remove(transferId, f)
          case _ =>
        }
        f
      })

      val result = for {
        session <- sessionFuture
        partETag <- store.uploadPart(session, itemNo, data)
      } yield {
        uploadedParts.getOrElseUpdate(transferId, TrieMap.empty).put(itemNo, partETag)
        ClaimTicket(
          number = itemKey,
          location = locationUri(bucket, itemKey),
          storeType = storeType,
          checkedAt = Some(Instant.now()),
          sizeBytes = Some(partETag.sizeBytes)
        )
      }

      // Release reservation if upload fails so caller can retry the same itemNo
      result.recoverWith { case ex =>
        reservedParts.get(transferId).foreach(_.remove(itemNo))
        Future.failed(ex)
      }
    }
  }

  /**
   * Complete a transfer's upload session by assembling all accumulated parts.
   *
   * On success, all session state for this transfer is cleaned up. On failure
   * (e.g. a transient storage error from `store.completeUpload`), state is
   * intentionally '''preserved''' so the caller can retry. If the caller
   * decides not to retry, it '''must''' call `abortTransfer` to release state.
   *
   * @param transferId         the transfer to finalize
   * @param expectedPartCount  if provided, asserts the accumulated part count matches
   * @return Future containing the BlobReference for the assembled object
   * @throws IllegalStateException if no session exists, no parts were stored,
   *                               or the part count doesn't match `expectedPartCount`
   */
  def completeTransfer(
      transferId: String,
      expectedPartCount: Option[Int] = None
  )(using ec: ExecutionContext): Future[BlobReference] = {
    uploadSessions.get(transferId) match {
      case Some(sessionFuture) =>
        sessionFuture.flatMap { session =>
          val parts = uploadedParts.get(transferId)
            .map(_.values.toSeq.sortBy(_.partNo))
            .getOrElse(Seq.empty)
          if (parts.isEmpty)
            throw new IllegalStateException(
              s"No parts found for transfer $transferId — ensure storeItem was called before completeTransfer"
            )
          expectedPartCount.foreach { expected =>
            if (parts.size != expected)
              throw new IllegalStateException(
                s"Expected $expected parts for transfer $transferId but found ${parts.size} — " +
                s"ensure all storeItem calls have completed before calling completeTransfer"
              )
          }
          store.completeUpload(session, parts).map { ref =>
            uploadSessions.remove(transferId)
            uploadedParts.remove(transferId)
            reservedParts.remove(transferId)
            ref
          }
        }
      case None =>
        Future.failed(new IllegalStateException(s"No upload session found for transfer: $transferId"))
    }
  }

  /**
   * Abort a transfer's upload session and clean up state.
   */
  def abortTransfer(transferId: String)(using ec: ExecutionContext): Future[Unit] = {
    uploadSessions.get(transferId) match {
      case Some(sessionFuture) =>
        sessionFuture.flatMap { session =>
          store.abortUpload(session).map { _ =>
            uploadSessions.remove(transferId)
            uploadedParts.remove(transferId)
            reservedParts.remove(transferId)
            ()
          }
        }.recoverWith { case NonFatal(_) =>
          uploadSessions.remove(transferId)
          uploadedParts.remove(transferId)
          reservedParts.remove(transferId)
          Future.successful(())
        }
      case None =>
        Future.successful(())
    }
  }

  /** Clear all in-memory session state. For test use only. */
  def resetForTest(): Unit = {
    given ExecutionContext = ExecutionContext.parasitic
    uploadSessions.values.foreach { sessionFuture =>
      sessionFuture.foreach(session => store.abortUpload(session))
    }
    uploadSessions.clear()
    reservedParts.clear()
    uploadedParts.clear()
  }
}
