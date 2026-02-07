/*
 * Copyright 2026 Tomshley LLC.
 * All Rights Reserved.
 */

package com.tomshley.boilerplate.jvm.filetransfer

import com.tomshley.boilerplate.jvm.reqreply.CborSerializable

/**
 * State trait for chunked transfer entities.
 * 
 * Extend this in your domain-specific state to track multipart upload progress.
 * Provides common fields and helper methods for upload lifecycle management.
 */
trait ChunkedTransferState extends CborSerializable {
  
  /** Upload session ID from storage backend */
  def uploadId: Option[String]
  
  /** Target bucket/container */
  def bucket: Option[String]
  
  /** Target key/path */
  def key: Option[String]
  
  /** Successfully uploaded parts: partNo -> (etag, sizeBytes) */
  def uploadedParts: Map[Int, PartInfo]
  
  /** Upload is currently in progress */
  def isInProgress: Boolean
  
  /** Upload completed successfully */
  def isComplete: Boolean
  
  /** Upload failed or was aborted */
  def isFailed: Boolean = !isInProgress && !isComplete && uploadId.isDefined
  
  /** Can resume this upload (has session, not complete) */
  def canResume: Boolean = uploadId.isDefined && isInProgress && !isComplete
  
  /** Total bytes uploaded so far */
  def totalBytesUploaded: Long = uploadedParts.values.map(_.sizeBytes).sum
  
  /** Number of parts uploaded */
  def partsCount: Int = uploadedParts.size
  
  /** Get next sequential part number (assumes no gaps, 1-indexed) */
  def nextPartNo: Int = if (uploadedParts.isEmpty) 1 else uploadedParts.keys.max + 1
  
  /** Find missing part numbers (gaps) in the sequence from 1 to max uploaded */
  def missingPartNos: Seq[Int] = {
    if (uploadedParts.isEmpty) Seq.empty
    else (1 to uploadedParts.keys.max).filterNot(uploadedParts.contains)
  }
  
  /** True if all parts 1..max are present (no gaps) */
  def hasNoGaps: Boolean = missingPartNos.isEmpty
}

/**
 * Information about an uploaded part.
 */
case class PartInfo(
    etag: String,
    sizeBytes: Long
) extends CborSerializable
