/*
 * copyright 2026 tomshley llc
 *
 * licensed under the apache license, version 2.0 (the "license");
 * you may not use this file except in compliance with the license.
 * you may obtain a copy of the license at
 *
 * http://www.apache.org/licenses/license-2.0
 *
 * unless required by applicable law or agreed to in writing, software
 * distributed under the license is distributed on an "as is" basis,
 * without warranties or conditions of any kind, either express or implied.
 * see the license for the specific language governing permissions and
 * limitations under the license.
 *
 * @author thomas schena @sgoggles <https://github.com/sgoggles> | <https://gitlab.com/sgoggles>
 *
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
  
  /** Release underlying resources (connection pools, threads, etc.) */
  def close(): Future[Done]
}
