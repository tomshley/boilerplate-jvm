/*
 * Copyright 2026 Tomshley LLC.
 * All Rights Reserved.
 */

package com.tomshley.boilerplate.jvm.objectstorage

import com.tomshley.boilerplate.jvm.objectstorage.models.BlobReference
import org.apache.pekko.stream.scaladsl.Source
import org.apache.pekko.util.ByteString

import scala.concurrent.{ExecutionContext, Future}

/**
 * Single-shot async blob writer.
 *
 * Wraps a BlobStoreBoilerplate into one call:
 *   write(bucket, key, data) → Future[BlobReference]
 *
 * No multipart session tracking, no claim-check semantics.
 */
trait SingleShotBlobWriter {
  def write(bucket: String, key: String, data: Array[Byte]): Future[BlobReference]
}

class DefaultSingleShotBlobWriter(store: BlobStoreBoilerplate)(using ec: ExecutionContext)
    extends SingleShotBlobWriter {

  override def write(
      bucket: String,
      key: String,
      data: Array[Byte]
  ): Future[BlobReference] = {
    store.initiateUpload(bucket, key).flatMap { session =>
      val uploadAndComplete = for {
        partETag <- store.uploadPart(session, 1, Source.single(ByteString(data)))
        blobRef  <- store.completeUpload(session, Seq(partETag))
      } yield blobRef
      
      // Abort multipart upload on failure to prevent storage cost accumulation
      uploadAndComplete.recoverWith { case ex =>
        store.abortUpload(session).transformWith(_ => Future.failed(ex))
      }
    }
  }
}
