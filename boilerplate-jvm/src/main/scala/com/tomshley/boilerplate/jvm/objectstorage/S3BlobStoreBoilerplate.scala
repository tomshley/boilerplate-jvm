/*
 * Copyright 2026 Tomshley LLC.
 * All Rights Reserved.
 */

package com.tomshley.boilerplate.jvm.objectstorage

import com.tomshley.boilerplate.jvm.objectstorage.models.*
import org.apache.pekko.Done
import org.apache.pekko.actor.typed.ActorSystem
import org.apache.pekko.actor.CoordinatedShutdown
import org.apache.pekko.stream.scaladsl.{Source, Sink}
import org.apache.pekko.stream.Materializer
import org.apache.pekko.util.ByteString
import scala.util.control.NonFatal
import software.amazon.awssdk.auth.credentials.{AwsBasicCredentials, StaticCredentialsProvider}
import software.amazon.awssdk.core.async.{AsyncRequestBody, AsyncResponseTransformer}
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.S3AsyncClient
import software.amazon.awssdk.services.s3.model.*

import java.net.URI
import java.nio.ByteBuffer
import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.CollectionConverters.*
import scala.jdk.FutureConverters.*

/**
 * S3 implementation of BlobStoreBoilerplate using AWS SDK v2.
 * 
 * Automatically registers cleanup with CoordinatedShutdown on construction.
 * 
 * @param config S3 configuration
 * @param onClose Optional callback invoked before close() during shutdown
 */
class S3BlobStoreBoilerplate(
    config: S3BlobStoreConfig,
    onClose: Option[() => Future[Done]] = None
)(using mat: Materializer, ec: ExecutionContext, system: ActorSystem[?])
    extends BlobStoreBoilerplate {

  private val s3Client: S3AsyncClient = {
    val credentialsProvider = StaticCredentialsProvider.create(
      AwsBasicCredentials.create(config.accessKeyId, config.secretAccessKey)
    )
    
    val clientBuilder = S3AsyncClient.builder()
      .credentialsProvider(credentialsProvider)
      .region(Region.of(config.region))

    config.endpoint.foreach { endpoint =>
      clientBuilder.endpointOverride(URI.create(endpoint))
    }
    
    if (config.pathStyleAccess) {
      clientBuilder.forcePathStyle(true)
    }

    clientBuilder.build()
  }

  private val closed = new java.util.concurrent.atomic.AtomicBoolean(false)

  // Auto-register cleanup on construction
  CoordinatedShutdown(system.classicSystem).addTask(
    CoordinatedShutdown.PhaseServiceUnbind,
    s"close-s3-blobstore-${System.identityHashCode(this)}"
  ) { () =>
    onClose.map(_()).getOrElse(Future.successful(Done))
      .transformWith(_ => close())
  }

  override def initiateUpload(
      bucket: String,
      key: String,
      metadata: Map[String, String]
  ): Future[UploadSession] = {
    val request = CreateMultipartUploadRequest.builder()
      .bucket(bucket)
      .key(key)
      .metadata(metadata.asJava)
      .build()

    s3Client.createMultipartUpload(request)
      .asScala
      .map { response =>
        UploadSession(bucket, key, response.uploadId())
      }
  }

  override def uploadPart(
      session: UploadSession,
      partNo: Int,
      data: Source[ByteString, ?]
  ): Future[PartETag] = {
    // Note: Buffers entire part into memory before upload
    // S3 parts are typically 5-100 MB, but can be up to 5 GB
    data.runWith(Sink.fold(ByteString.empty)(_ ++ _)).flatMap { bytes =>
      val contentLength = bytes.size.toLong
      val request = UploadPartRequest.builder()
        .bucket(session.bucket)
        .key(session.key)
        .uploadId(session.uploadId)
        .partNumber(partNo)
        .contentLength(contentLength)
        .build()

      val requestBody = AsyncRequestBody.fromByteBuffer(ByteBuffer.wrap(bytes.toArray))

      s3Client.uploadPart(request, requestBody)
        .asScala
        .map { response =>
          PartETag(partNo, response.eTag(), contentLength)
        }
    }
  }

  override def completeUpload(
      session: UploadSession,
      parts: Seq[PartETag]
  ): Future[BlobReference] = {
    // S3 requires parts in ascending order by part number
    val completedParts = parts.sortBy(_.partNo).map { part =>
      CompletedPart.builder()
        .partNumber(part.partNo)
        .eTag(part.etag)
        .build()
    }.asJava

    val request = CompleteMultipartUploadRequest.builder()
      .bucket(session.bucket)
      .key(session.key)
      .uploadId(session.uploadId)
      .multipartUpload(CompletedMultipartUpload.builder().parts(completedParts).build())
      .build()

    s3Client.completeMultipartUpload(request)
      .asScala
      .map { response =>
        val etag = response.eTag()
        val totalSize = parts.map(_.sizeBytes).sum
        // S3 multipart ETags (format "md5-N") are NOT content checksums
        // They're MD5s of concatenated part MD5s, not usable for integrity verification
        // Set checksum to empty string to signal unavailable rather than provide misleading value
        BlobReference(session.bucket, session.key, etag, "", totalSize)
      }
  }

  override def abortUpload(session: UploadSession): Future[Done] = {
    val request = AbortMultipartUploadRequest.builder()
      .bucket(session.bucket)
      .key(session.key)
      .uploadId(session.uploadId)
      .build()

    s3Client.abortMultipartUpload(request)
      .asScala
      .map(_ => Done)
  }

  override def listParts(session: UploadSession): Future[Seq[PartETag]] = {
    def listPartsRecursive(partNumberMarker: Option[Integer], accumulated: Seq[PartETag]): Future[Seq[PartETag]] = {
      val requestBuilder = ListPartsRequest.builder()
        .bucket(session.bucket)
        .key(session.key)
        .uploadId(session.uploadId)
      
      partNumberMarker.foreach(marker => requestBuilder.partNumberMarker(marker))
      
      s3Client.listParts(requestBuilder.build())
        .asScala
        .flatMap { response =>
          val parts = response.parts().asScala.map { part =>
            PartETag(part.partNumber(), part.eTag(), part.size())
          }.toSeq
          
          val allParts = accumulated ++ parts
          
          val isTruncated = Option(response.isTruncated).exists(_.booleanValue)
          if (isTruncated) {
            if (response.nextPartNumberMarker() == null) {
              Future.failed(new IllegalStateException(
                s"S3 protocol violation: isTruncated=true but nextPartNumberMarker is null for ${session.bucket}/${session.key}"
              ))
            } else {
              listPartsRecursive(Some(response.nextPartNumberMarker()), allParts)
            }
          } else {
            Future.successful(allParts)
          }
        }
    }
    
    listPartsRecursive(None, Seq.empty)
  }

  override def getObject(ref: BlobReference): Source[ByteString, Future[ObjectMetadata]] = {
    val request = GetObjectRequest.builder()
      .bucket(ref.bucket)
      .key(ref.key)
      .build()

    val futureResponse = s3Client.getObject(
      request,
      AsyncResponseTransformer.toPublisher[GetObjectResponse]()
    ).asScala

    Source.futureSource(
      futureResponse.map { responsePublisher =>
        val metadata = ObjectMetadata(
          contentType = Option(responsePublisher.response().contentType()),
          contentLength = responsePublisher.response().contentLength()
        )
        
        Source.fromPublisher(responsePublisher)
          .map(ByteString.fromByteBuffer)
          .mapMaterializedValue(_ => Future.successful(metadata))
      }
    ).mapMaterializedValue(_.flatten)
  }

  override def deleteObject(ref: BlobReference): Future[Done] = {
    val request = DeleteObjectRequest.builder()
      .bucket(ref.bucket)
      .key(ref.key)
      .build()

    s3Client.deleteObject(request)
      .asScala
      .map(_ => Done)
  }

  override def objectExists(bucket: String, key: String): Future[Boolean] = {
    val request = HeadObjectRequest.builder()
      .bucket(bucket)
      .key(key)
      .build()

    s3Client.headObject(request)
      .asScala
      .map(_ => true)
      .recover {
        case ex: S3Exception if ex.statusCode() == 404 => false
        case NonFatal(ex) => throw new RuntimeException(s"Failed to check if object exists $bucket/$key", ex)
      }
  }

  override def close(): Future[Done] = {
    if (closed.compareAndSet(false, true)) {
      Future {
        scala.concurrent.blocking {
          s3Client.close()
        }
        Done
      }
    } else {
      Future.successful(Done)
    }
  }
}

object S3BlobStoreBoilerplate {
  def apply(
      config: S3BlobStoreConfig,
      onClose: Option[() => Future[Done]] = None
  )(using mat: Materializer, ec: ExecutionContext, system: ActorSystem[?]): BlobStoreBoilerplate = {
    new S3BlobStoreBoilerplate(config, onClose)
  }
}
