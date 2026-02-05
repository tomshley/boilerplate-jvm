/*
 * Copyright 2026 Tomshley LLC.
 * All Rights Reserved.
 */

package com.tomshley.boilerplate.jvm.objectstorage

import com.tomshley.boilerplate.jvm.objectstorage.models.*
import org.apache.pekko.Done
import org.apache.pekko.stream.scaladsl.Source
import org.apache.pekko.util.ByteString

import scala.concurrent.Future

/**
 * Boilerplate trait for object storage operations.
 * 
 * Provides a standard interface for multipart uploads to S3-compatible storage.
 * Implementations handle the specifics of each storage backend.
 */
trait BlobStoreBoilerplate {
  
  /** Initiate a multipart upload session */
  def initiateUpload(
      bucket: String,
      key: String,
      metadata: Map[String, String] = Map.empty
  ): Future[UploadSession]
  
  /** Upload a single part */
  def uploadPart(
      session: UploadSession,
      partNo: Int,
      data: Source[ByteString, ?]
  ): Future[PartETag]
  
  /** Complete the multipart upload */
  def completeUpload(
      session: UploadSession,
      parts: Seq[PartETag]
  ): Future[BlobReference]
  
  /** Abort an in-progress upload */
  def abortUpload(session: UploadSession): Future[Done]
  
  /** List uploaded parts (for crash recovery) */
  def listParts(session: UploadSession): Future[Seq[PartETag]]
  
  /** Get object as stream */
  def getObject(ref: BlobReference): Source[ByteString, Future[ObjectMetadata]]
  
  /** Delete an object */
  def deleteObject(ref: BlobReference): Future[Done]
  
  /** Check if object exists */
  def objectExists(bucket: String, key: String): Future[Boolean]
}
