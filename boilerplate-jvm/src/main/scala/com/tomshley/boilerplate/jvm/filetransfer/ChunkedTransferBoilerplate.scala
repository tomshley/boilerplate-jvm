/*
 * Copyright 2026 Tomshley LLC.
 * All Rights Reserved.
 */

package com.tomshley.boilerplate.jvm.filetransfer

import java.time.Instant

/**
 * Boilerplate trait for chunked transfer eventsourced entities.
 * 
 * Provides common event handling logic for multipart uploads.
 * Extend this in your domain-specific entity behavior.
 * 
 * Usage:
 * {{{
 * class FileIngressEntity extends ChunkedTransferBoilerplate {
 *   // Your domain-specific commands and events
 *   // Use applyTransferEvent to handle lifecycle events
 * }
 * }}}
 */
trait ChunkedTransferBoilerplate {

  /**
   * Apply a transfer lifecycle event to state.
   * Override to customize state updates.
   * 
   * @param state Current state (must extend ChunkedTransferState)
   * @param event The lifecycle event to apply
   * @param updateState Function to create new state with updated fields
   * @return Updated state
   */
  def applyTransferEvent[S <: ChunkedTransferState](
      state: S,
      event: TransferLifecycleEvent
  )(updateState: (
      Option[String],           // uploadId
      Option[String],           // bucket
      Option[String],           // key
      Map[Int, PartInfo],       // uploadedParts
      Boolean,                  // isInProgress
      Boolean                   // isComplete
  ) => S): S = {
    import TransferLifecycleEvent._
    
    event match {
      case UploadInitiated(uploadId, bucket, key, _) =>
        updateState(
          Some(uploadId),
          Some(bucket),
          Some(key),
          Map.empty,
          true,
          false
        )
        
      case PartUploaded(partNo, etag, sizeBytes, _) =>
        updateState(
          state.uploadId,
          state.bucket,
          state.key,
          state.uploadedParts + (partNo -> PartInfo(etag, sizeBytes)),
          true,
          false
        )
        
      case UploadCompleted(_, _, _, _, _, _) =>
        updateState(
          state.uploadId,
          state.bucket,
          state.key,
          state.uploadedParts,
          false,
          true
        )
        
      case UploadFailed(_, _) | UploadAborted(_) =>
        updateState(
          state.uploadId,
          state.bucket,
          state.key,
          state.uploadedParts,
          false,
          false
        )
    }
  }
  
  /** Create UploadInitiated event */
  def initiatedEvent(uploadId: String, bucket: String, key: String): TransferLifecycleEvent.UploadInitiated =
    TransferLifecycleEvent.UploadInitiated(uploadId, bucket, key, Instant.now())
  
  /** Create PartUploaded event */
  def partUploadedEvent(partNo: Int, etag: String, sizeBytes: Long): TransferLifecycleEvent.PartUploaded =
    TransferLifecycleEvent.PartUploaded(partNo, etag, sizeBytes, Instant.now())
  
  /** Create UploadCompleted event */
  def completedEvent(bucket: String, key: String, etag: String, checksum: String, totalBytes: Long): TransferLifecycleEvent.UploadCompleted =
    TransferLifecycleEvent.UploadCompleted(bucket, key, etag, checksum, totalBytes, Instant.now())
  
  /** Create UploadFailed event */
  def failedEvent(reason: String): TransferLifecycleEvent.UploadFailed =
    TransferLifecycleEvent.UploadFailed(reason, Instant.now())
  
  /** Create UploadAborted event */
  def abortedEvent(): TransferLifecycleEvent.UploadAborted =
    TransferLifecycleEvent.UploadAborted(Instant.now())
}
