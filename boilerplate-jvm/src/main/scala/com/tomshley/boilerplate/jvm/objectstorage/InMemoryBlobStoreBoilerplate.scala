/*
 * Copyright 2026 Tomshley LLC.
 * All Rights Reserved.
 */

package com.tomshley.boilerplate.jvm.objectstorage

import com.tomshley.boilerplate.jvm.objectstorage.models.*
import org.apache.pekko.Done
import org.apache.pekko.stream.Materializer
import org.apache.pekko.stream.scaladsl.{Source, Sink}
import org.apache.pekko.util.ByteString

import java.security.MessageDigest
import java.util.UUID
import scala.collection.concurrent.TrieMap
import scala.concurrent.{ExecutionContext, Future}

/**
 * In-memory implementation of BlobStoreBoilerplate for testing.
 */
class InMemoryBlobStoreBoilerplate(using mat: Materializer, ec: ExecutionContext)
    extends BlobStoreBoilerplate {

  private val objects: TrieMap[String, (ByteString, ObjectMetadata)] = TrieMap.empty
  private val uploads: TrieMap[String, TrieMap[Int, (ByteString, PartETag)]] = TrieMap.empty
  private val sessions: TrieMap[String, UploadSession] = TrieMap.empty

  private def objectKey(bucket: String, key: String): String = s"$bucket/$key"

  override def initiateUpload(
      bucket: String,
      key: String,
      metadata: Map[String, String]
  ): Future[UploadSession] = {
    val uploadId = UUID.randomUUID().toString
    val session = UploadSession(bucket, key, uploadId)
    sessions.put(uploadId, session)
    uploads.put(uploadId, TrieMap.empty)
    Future.successful(session)
  }

  override def uploadPart(
      session: UploadSession,
      partNo: Int,
      data: Source[ByteString, ?]
  ): Future[PartETag] = {
    data.runWith(Sink.fold(ByteString.empty)(_ ++ _)).flatMap { bytes =>
      uploads.get(session.uploadId) match {
        case Some(partMap) =>
          val etag = md5(bytes)
          val partETag = PartETag(partNo, etag, bytes.size.toLong)
          partMap.put(partNo, (bytes, partETag))
          Future.successful(partETag)
        case None =>
          Future.failed(new IllegalStateException(s"Unknown upload session: ${session.uploadId}"))
      }
    }
  }

  override def completeUpload(
      session: UploadSession,
      parts: Seq[PartETag]
  ): Future[BlobReference] = {
    uploads.get(session.uploadId) match {
      case Some(partMap) =>
        val sortedParts = parts.sortBy(_.partNo)
        val missingParts = sortedParts.filterNot(p => partMap.contains(p.partNo))
        if (missingParts.nonEmpty) {
          Future.failed(new IllegalStateException(
            s"Missing parts: ${missingParts.map(_.partNo).mkString(", ")}"
          ))
        } else {
          val fullData = sortedParts.flatMap(p => partMap.get(p.partNo).map(_._1)).fold(ByteString.empty)(_ ++ _)
          val checksum = md5(fullData)
          val etag = s"${checksum}-${parts.size}"
          val ref = BlobReference(session.bucket, session.key, etag, checksum, fullData.size.toLong)
          val metadata = ObjectMetadata(Some("application/octet-stream"), fullData.size.toLong)
          objects.put(objectKey(session.bucket, session.key), (fullData, metadata))
          uploads.remove(session.uploadId)
          sessions.remove(session.uploadId)
          Future.successful(ref)
        }
      case None =>
        Future.failed(new IllegalStateException(s"Unknown upload session: ${session.uploadId}"))
    }
  }

  override def abortUpload(session: UploadSession): Future[Done] = {
    uploads.remove(session.uploadId)
    sessions.remove(session.uploadId)
    Future.successful(Done)
  }

  override def listParts(session: UploadSession): Future[Seq[PartETag]] = {
    val parts = uploads.get(session.uploadId)
      .map(_.values.map(_._2).toSeq.sortBy(_.partNo))
      .getOrElse(Seq.empty)
    Future.successful(parts)
  }

  override def getObject(ref: BlobReference): Source[ByteString, Future[ObjectMetadata]] = {
    objects.get(objectKey(ref.bucket, ref.key)) match {
      case Some((data, metadata)) =>
        Source.single(data).mapMaterializedValue(_ => Future.successful(metadata))
      case None =>
        Source.failed(new NoSuchElementException(s"Object not found: ${ref.uri}"))
          .mapMaterializedValue(_ => Future.failed(new NoSuchElementException(s"Object not found: ${ref.uri}")))
    }
  }

  override def deleteObject(ref: BlobReference): Future[Done] = {
    objects.remove(objectKey(ref.bucket, ref.key))
    Future.successful(Done)
  }

  override def objectExists(bucket: String, key: String): Future[Boolean] = {
    Future.successful(objects.contains(objectKey(bucket, key)))
  }

  override def close(): Future[Done] = {
    Future.successful(Done)
  }

  private def md5(data: ByteString): String = {
    val digest = MessageDigest.getInstance("MD5")
    digest.update(data.toArray)
    digest.digest().map("%02x".format(_)).mkString
  }
}
