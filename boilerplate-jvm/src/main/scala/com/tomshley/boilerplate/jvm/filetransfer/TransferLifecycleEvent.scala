/*
 * Copyright 2026 Tomshley LLC.
 * All Rights Reserved.
 */

package com.tomshley.boilerplate.jvm.filetransfer

import java.time.Instant
import com.tomshley.boilerplate.jvm.reqreply.CborSerializable

/**
 * Common lifecycle events for chunked transfer entities.
 * 
 * These events represent the standard lifecycle of a multipart/chunked upload:
 * - Initiate upload session
 * - Upload parts
 * - Complete or abort/fail
 * 
 * Extend these in your domain-specific event hierarchy.
 */
sealed trait TransferLifecycleEvent extends CborSerializable

object TransferLifecycleEvent {
  
  /** Upload session initiated with storage backend */
  case class UploadInitiated(
      uploadId: String,
      bucket: String,
      key: String,
      initiatedAt: Instant
  ) extends TransferLifecycleEvent

  /** A part was successfully uploaded */
  case class PartUploaded(
      partNo: Int,
      etag: String,
      sizeBytes: Long,
      uploadedAt: Instant
  ) extends TransferLifecycleEvent

  /** Upload completed successfully */
  case class UploadCompleted(
      bucket: String,
      key: String,
      etag: String,
      checksum: String,
      totalBytes: Long,
      completedAt: Instant
  ) extends TransferLifecycleEvent

  /** Upload failed */
  case class UploadFailed(
      reason: String,
      failedAt: Instant
  ) extends TransferLifecycleEvent

  /** Upload was aborted (user-initiated or cleanup) */
  case class UploadAborted(
      abortedAt: Instant
  ) extends TransferLifecycleEvent
}
