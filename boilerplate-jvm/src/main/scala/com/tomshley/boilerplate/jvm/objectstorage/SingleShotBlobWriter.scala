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
