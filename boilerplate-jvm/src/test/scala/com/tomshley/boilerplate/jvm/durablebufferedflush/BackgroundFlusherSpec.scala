/*
 * Copyright 2026 Tomshley LLC.
 * All Rights Reserved.
 */

package com.tomshley.boilerplate.jvm.durablebufferedflush

import com.tomshley.boilerplate.jvm.durablebufferedflush.SpoolMeta
import com.tomshley.boilerplate.jvm.durablebufferedflush.internal.{DefaultChunkFlusher, FilesystemChunkSpool}
import com.typesafe.config.ConfigFactory
import com.tomshley.boilerplate.jvm.objectstorage.{DefaultSingleShotBlobWriter, InMemoryBlobStoreBoilerplate, SingleShotBlobWriter}
import com.tomshley.boilerplate.jvm.objectstorage.models.BlobReference
import org.apache.pekko.actor.testkit.typed.scaladsl.ActorTestKit
import org.apache.pekko.actor.typed.ActorSystem
import org.apache.pekko.pattern.{after => scheduleAfter}
import org.apache.pekko.stream.Materializer
import org.apache.pekko.stream.scaladsl.Sink
import org.apache.pekko.util.ByteString

import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.{Eventually, ScalaFutures}
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{Millis, Seconds, Span}
import org.scalatest.wordspec.AnyWordSpec

import java.nio.file.attribute.BasicFileAttributes
import java.nio.file.{FileVisitResult, Files, Path, SimpleFileVisitor}
import scala.collection.concurrent.TrieMap
import scala.concurrent.{ExecutionContext, Future, Promise, TimeoutException}
import scala.concurrent.duration.*

class DefaultChunkFlusherSpec
    extends AnyWordSpec
    with Matchers
    with BeforeAndAfterAll
    with ScalaFutures
    with Eventually {

  override implicit val patienceConfig: PatienceConfig =
    PatienceConfig(timeout = Span(10, Seconds), interval = Span(50, Millis))

  private val testKit = ActorTestKit("DefaultChunkFlusherSpec")
  private given ActorSystem[?] = testKit.system
  private given ExecutionContext = testKit.system.executionContext
  private given Materializer = Materializer(testKit.system)

  /** Test-local metadata adapter — produces the same key structure that a
    * downstream consumer's `SpoolMetaFlusherAdapter` would, without
    * depending on any particular consumer's implementation. */
  private val testMetaAdapter: FlusherMetaAdapter[SpoolMeta] = new FlusherMetaAdapter[SpoolMeta] {
    override def toFlusherMetaView(meta: SpoolMeta): FlusherMetaView =
      FlusherMetaView(
        lastSpooledSeq = meta.lastSpooledSeq,
        blobKeyPrefix = BlobKeyPrefix(s"devices/${meta.deviceId}/sessions/${meta.objectHashHex}")
      )
  }

  private def expectedChunkKey(deviceId: String, objectHashHex: String, seq: Long): String =
    PrefixedSequentialBlobKeyResolver.chunkKey(
      BlobKeyPrefix(s"devices/$deviceId/sessions/$objectHashHex"),
      seq
    )

  override def afterAll(): Unit = {
    testKit.shutdownTestKit()
    super.afterAll()
  }

  "DefaultChunkFlusher" should {

    "build from an otherwise-empty config and still flush with internal defaults" in {
      withTempDir { rootDir =>
        val entityId = "entity-config-defaults"
        val spool = new FilesystemChunkSpool(testKit.system, rootDir)
        val meta = SpoolMeta.initial(entityId, "device-config-defaults", "hash-config-defaults", 32L, 1L)
        spool.initialize(entityId, meta).futureValue
        spool.write(entityId, 0L, Array.fill[Byte](32)(7)).futureValue shouldBe 32L

        val blobStore = new InMemoryBlobStoreBoilerplate()
        val factory = ChunkFlusherFactory.background(
          ConfigFactory.empty(),
          testKit.system,
          new DefaultSingleShotBlobWriter(blobStore),
          "flusher-config-defaults",
          testMetaAdapter
        )

        val flusher = factory.create(entityId, spool, startSeq = 0L)
        flusher.drain().futureValue shouldBe 0L
        spool.readMeta(entityId).futureValue.get.flushedSeq shouldBe 0L

        val key = expectedChunkKey("device-config-defaults", "hash-config-defaults", 0L)
        blobStore.objectExists("flusher-config-defaults", key).futureValue shouldBe true
      }
    }

    "flush all spooled chunks and persist the contiguous watermark on drain" in {
      withTempDir { rootDir =>
        val entityId = "entity-happy"
        val spool = new FilesystemChunkSpool(testKit.system, rootDir)
        val meta = SpoolMeta.initial(entityId, "device-happy", "hash-happy", 96L, 3L)
        spool.initialize(entityId, meta).futureValue

        val payloads = (0L to 2L).map(seq => seq -> Array.fill[Byte](32)((seq + 1L).toByte)).toMap
        payloads.toSeq.sortBy(_._1).foreach { case (seq, bytes) =>
          spool.write(entityId, seq, bytes).futureValue shouldBe bytes.length.toLong
        }

        val blobStore = new InMemoryBlobStoreBoilerplate()
        val flusher = new DefaultChunkFlusher(
          entityId = entityId,
          spool = spool,
          blobWriter = new DefaultSingleShotBlobWriter(blobStore),
          bucket = "flusher-happy",
          startSeq = 0L,
          parallelism = 4,
          tickInterval = 10.millis,
          drainTimeout = 2.seconds,
          maxUploadAttempts = 3,
          initialRetryBackoff = 10.millis,
          metaAdapter = testMetaAdapter
        )

        flusher.start()
        flusher.drain().futureValue shouldBe 2L
        flusher.flushedSeq.futureValue shouldBe 2L
        spool.readMeta(entityId).futureValue.get.flushedSeq shouldBe 2L

        payloads.foreach { case (seq, expectedBytes) =>
          val key = expectedChunkKey("device-happy", "hash-happy", seq)
          blobStore.objectExists("flusher-happy", key).futureValue shouldBe true
          readObject(blobStore, "flusher-happy", key) shouldEqual expectedBytes
        }
      }
    }

    "resolve chunk keys through an injected seam without changing drain behavior" in {
      withTempDir { rootDir =>
        val entityId = "entity-seam"
        val spool = new FilesystemChunkSpool(testKit.system, rootDir)
        val meta = SpoolMeta.initial(entityId, "device-seam", "hash-seam", 64L, 2L)
        spool.initialize(entityId, meta).futureValue

        val payloads = (0L to 1L).map(seq => seq -> Array.fill[Byte](32)((seq + 15L).toByte)).toMap
        payloads.toSeq.sortBy(_._1).foreach { case (seq, bytes) =>
          spool.write(entityId, seq, bytes).futureValue shouldBe bytes.length.toLong
        }

        val blobStore = new InMemoryBlobStoreBoilerplate()
        val customAdapter = new FlusherMetaAdapter[SpoolMeta] {
          override def toFlusherMetaView(meta: SpoolMeta): FlusherMetaView =
            FlusherMetaView(
              lastSpooledSeq = meta.lastSpooledSeq,
              blobKeyPrefix = BlobKeyPrefix(s"custom-prefix/${meta.deviceId}/${meta.objectHashHex}")
            )
        }
        val customResolver = new BlobKeyResolver {
          override def chunkKey(blobKeyPrefix: BlobKeyPrefix, sequence: Long): String =
            f"${blobKeyPrefix.value}/chunk-$sequence%09d"
        }

        val flusher = new DefaultChunkFlusher(
          entityId = entityId,
          spool = spool,
          blobWriter = new DefaultSingleShotBlobWriter(blobStore),
          bucket = "flusher-seam",
          startSeq = 0L,
          parallelism = 2,
          tickInterval = 10.millis,
          drainTimeout = 2.seconds,
          maxUploadAttempts = 3,
          initialRetryBackoff = 10.millis,
          metaAdapter = customAdapter,
          blobKeyResolver = customResolver
        )

        flusher.start()
        flusher.drain().futureValue shouldBe 1L
        flusher.flushedSeq.futureValue shouldBe 1L
        spool.readMeta(entityId).futureValue.get.flushedSeq shouldBe 1L

        payloads.foreach { case (seq, expectedBytes) =>
          val key = f"custom-prefix/device-seam/hash-seam/chunk-$seq%09d"
          blobStore.objectExists("flusher-seam", key).futureValue shouldBe true
          readObject(blobStore, "flusher-seam", key) shouldEqual expectedBytes
        }
      }
    }

    "advance the watermark only when the completed range is contiguous" in {
      withTempDir { rootDir =>
        val entityId = "entity-watermark"
        val spool = new FilesystemChunkSpool(testKit.system, rootDir)
        spool.initialize(entityId, SpoolMeta.initial(entityId, "device-watermark", "hash-watermark", 96L, 3L)).futureValue

        (0L to 2L).foreach { seq =>
          spool.write(entityId, seq, Array.fill[Byte](32)((seq + 10L).toByte)).futureValue
        }

        val blobStore = new InMemoryBlobStoreBoilerplate()
        val delegate = new DefaultSingleShotBlobWriter(blobStore)
        val releaseSeq0 = Promise[Unit]()
        val seq1Uploaded = Promise[Unit]()
        val seq2Uploaded = Promise[Unit]()

        val writer = new SingleShotBlobWriter {
          override def write(bucket: String, key: String, data: Array[Byte]): Future[BlobReference] = {
            val seq = key.takeRight(9).toLong
            seq match {
              case 0L =>
                releaseSeq0.future.flatMap(_ => delegate.write(bucket, key, data))
              case 1L =>
                delegate.write(bucket, key, data).map { ref =>
                  seq1Uploaded.trySuccess(())
                  ref
                }
              case 2L =>
                delegate.write(bucket, key, data).map { ref =>
                  seq2Uploaded.trySuccess(())
                  ref
                }
              case other =>
                delegate.write(bucket, key, data)
            }
          }
        }

        val flusher = new DefaultChunkFlusher(
          entityId = entityId,
          spool = spool,
          blobWriter = writer,
          bucket = "flusher-watermark",
          startSeq = 0L,
          parallelism = 3,
          tickInterval = 10.millis,
          drainTimeout = 2.seconds,
          maxUploadAttempts = 3,
          initialRetryBackoff = 10.millis,
          metaAdapter = testMetaAdapter
        )

        flusher.start()
        seq1Uploaded.future.futureValue
        seq2Uploaded.future.futureValue
        flusher.flushedSeq.futureValue shouldBe -1L

        releaseSeq0.success(())
        flusher.drain().futureValue shouldBe 2L
        spool.readMeta(entityId).futureValue.get.flushedSeq shouldBe 2L
      }
    }

    "never regress the flushed watermark while later chunks complete before a blocked gap" in {
      withTempDir { rootDir =>
        val entityId = "entity-monotonic"
        val spool = new FilesystemChunkSpool(testKit.system, rootDir)
        spool.initialize(entityId, SpoolMeta.initial(entityId, "device-monotonic", "hash-monotonic", 128L, 4L)).futureValue

        (0L to 3L).foreach { seq =>
          spool.write(entityId, seq, Array.fill[Byte](32)((seq + 60L).toByte)).futureValue
        }

        val blobStore = new InMemoryBlobStoreBoilerplate()
        val delegate = new DefaultSingleShotBlobWriter(blobStore)
        val releaseSeq1 = Promise[Unit]()
        val releaseSeq2 = Promise[Unit]()
        val releaseSeq3 = Promise[Unit]()
        val seq0Uploaded = Promise[Unit]()
        val seq1Uploaded = Promise[Unit]()
        val seq2Uploaded = Promise[Unit]()
        val seq3Uploaded = Promise[Unit]()

        val writer = new SingleShotBlobWriter {
          override def write(bucket: String, key: String, data: Array[Byte]): Future[BlobReference] = {
            val seq = key.takeRight(9).toLong
            seq match {
              case 0L =>
                delegate.write(bucket, key, data).map { ref =>
                  seq0Uploaded.trySuccess(())
                  ref
                }
              case 1L =>
                releaseSeq1.future.flatMap { _ =>
                  delegate.write(bucket, key, data).map { ref =>
                    seq1Uploaded.trySuccess(())
                    ref
                  }
                }
              case 2L =>
                releaseSeq2.future.flatMap { _ =>
                  delegate.write(bucket, key, data).map { ref =>
                    seq2Uploaded.trySuccess(())
                    ref
                  }
                }
              case 3L =>
                releaseSeq3.future.flatMap { _ =>
                  delegate.write(bucket, key, data).map { ref =>
                    seq3Uploaded.trySuccess(())
                    ref
                  }
                }
              case _ =>
                delegate.write(bucket, key, data)
            }
          }
        }

        val flusher = new DefaultChunkFlusher(
          entityId = entityId,
          spool = spool,
          blobWriter = writer,
          bucket = "flusher-monotonic",
          startSeq = 0L,
          parallelism = 4,
          tickInterval = 10.millis,
          drainTimeout = 2.seconds,
          maxUploadAttempts = 3,
          initialRetryBackoff = 10.millis,
          metaAdapter = testMetaAdapter
        )

        flusher.flushedSeq.futureValue shouldBe -1L
        flusher.start()

        seq0Uploaded.future.futureValue
        eventually {
          flusher.flushedSeq.futureValue shouldBe 0L
        }

        releaseSeq3.success(())
        seq3Uploaded.future.futureValue
        eventually {
          flusher.flushedSeq.futureValue shouldBe 0L
        }

        releaseSeq2.success(())
        seq2Uploaded.future.futureValue
        eventually {
          flusher.flushedSeq.futureValue shouldBe 0L
        }

        releaseSeq1.success(())
        seq1Uploaded.future.futureValue
        flusher.drain().futureValue shouldBe 3L

        val observedWatermarks = Seq(-1L, 0L, 0L, 0L, flusher.flushedSeq.futureValue)
        observedWatermarks.zip(observedWatermarks.drop(1)).foreach { case (previous, next) =>
          next should be >= previous
        }
        spool.readMeta(entityId).futureValue.get.flushedSeq shouldBe 3L
      }
    }

    "retry transient upload failures and still drain to the latest spooled sequence" in {
      withTempDir { rootDir =>
        val entityId = "entity-retry"
        val spool = new FilesystemChunkSpool(testKit.system, rootDir)
        spool.initialize(entityId, SpoolMeta.initial(entityId, "device-retry", "hash-retry", 96L, 3L)).futureValue

        (0L to 2L).foreach { seq =>
          spool.write(entityId, seq, Array.fill[Byte](32)((seq + 20L).toByte)).futureValue
        }

        val blobStore = new InMemoryBlobStoreBoilerplate()
        val delegate = new DefaultSingleShotBlobWriter(blobStore)
        val failuresSeen = TrieMap.empty[Long, Int]

        val writer = new SingleShotBlobWriter {
          override def write(bucket: String, key: String, data: Array[Byte]): Future[BlobReference] = {
            val seq = key.takeRight(9).toLong
            val attempts = failuresSeen.getOrElse(seq, 0)
            if (seq == 1L && attempts == 0) {
              failuresSeen.put(seq, attempts + 1)
              Future.failed(new RuntimeException("transient failure"))
            } else {
              failuresSeen.put(seq, attempts + 1)
              delegate.write(bucket, key, data)
            }
          }
        }

        val flusher = new DefaultChunkFlusher(
          entityId = entityId,
          spool = spool,
          blobWriter = writer,
          bucket = "flusher-retry",
          startSeq = 0L,
          parallelism = 3,
          tickInterval = 10.millis,
          drainTimeout = 2.seconds,
          maxUploadAttempts = 3,
          initialRetryBackoff = 10.millis,
          metaAdapter = testMetaAdapter
        )

        flusher.start()
        flusher.drain().futureValue shouldBe 2L
        failuresSeen(1L) shouldBe 2
        spool.readMeta(entityId).futureValue.get.flushedSeq shouldBe 2L
      }
    }

    "drain all spooled chunks even if start was never called" in {
      withTempDir { rootDir =>
        val entityId = "entity-drain-without-start"
        val spool = new FilesystemChunkSpool(testKit.system, rootDir)
        val meta = SpoolMeta.initial(entityId, "device-drain", "hash-drain", 96L, 3L)
        spool.initialize(entityId, meta).futureValue

        val payloads = (0L to 2L).map(seq => seq -> Array.fill[Byte](32)((seq + 30L).toByte)).toMap
        payloads.toSeq.sortBy(_._1).foreach { case (seq, bytes) =>
          spool.write(entityId, seq, bytes).futureValue shouldBe bytes.length.toLong
        }

        val blobStore = new InMemoryBlobStoreBoilerplate()
        val flusher = new DefaultChunkFlusher(
          entityId = entityId,
          spool = spool,
          blobWriter = new DefaultSingleShotBlobWriter(blobStore),
          bucket = "flusher-drain-without-start",
          startSeq = 0L,
          parallelism = 4,
          tickInterval = 10.millis,
          drainTimeout = 2.seconds,
          maxUploadAttempts = 3,
          initialRetryBackoff = 10.millis,
          metaAdapter = testMetaAdapter
        )

        flusher.drain().futureValue shouldBe 2L
        flusher.flushedSeq.futureValue shouldBe 2L
        spool.readMeta(entityId).futureValue.get.flushedSeq shouldBe 2L

        payloads.foreach { case (seq, expectedBytes) =>
          val key = expectedChunkKey("device-drain", "hash-drain", seq)
          blobStore.objectExists("flusher-drain-without-start", key).futureValue shouldBe true
          readObject(blobStore, "flusher-drain-without-start", key) shouldEqual expectedBytes
        }
      }
    }

    "return the same drain result to a second caller while a drain is already in progress" in {
      withTempDir { rootDir =>
        val entityId = "entity-double-drain"
        val spool = new FilesystemChunkSpool(testKit.system, rootDir)
        spool.initialize(entityId, SpoolMeta.initial(entityId, "device-double-drain", "hash-double-drain", 96L, 3L)).futureValue

        (0L to 2L).foreach { seq =>
          spool.write(entityId, seq, Array.fill[Byte](32)((seq + 70L).toByte)).futureValue
        }

        val firstUploadStarted = Promise[Unit]()
        val releaseUploads = Promise[Unit]()
        val uploadCounts = TrieMap.empty[Long, Int]
        val blobStore = new InMemoryBlobStoreBoilerplate()
        val delegate = new DefaultSingleShotBlobWriter(blobStore)

        val writer = new SingleShotBlobWriter {
          override def write(bucket: String, key: String, data: Array[Byte]): Future[BlobReference] = {
            val seq = key.takeRight(9).toLong
            uploadCounts.put(seq, uploadCounts.getOrElse(seq, 0) + 1)
            if (seq == 0L) {
              firstUploadStarted.trySuccess(())
              releaseUploads.future.flatMap(_ => delegate.write(bucket, key, data))
            } else {
              delegate.write(bucket, key, data)
            }
          }
        }

        val flusher = new DefaultChunkFlusher(
          entityId = entityId,
          spool = spool,
          blobWriter = writer,
          bucket = "flusher-double-drain",
          startSeq = 0L,
          parallelism = 3,
          tickInterval = 10.millis,
          drainTimeout = 2.seconds,
          maxUploadAttempts = 3,
          initialRetryBackoff = 10.millis,
          metaAdapter = testMetaAdapter
        )

        flusher.start()
        val firstDrain = flusher.drain()
        firstUploadStarted.future.futureValue

        val secondDrain = flusher.drain()
        secondDrain.isCompleted shouldBe false

        releaseUploads.success(())

        firstDrain.futureValue shouldBe 2L
        secondDrain.futureValue shouldBe 2L
        uploadCounts.values.sum shouldBe 3
      }
    }

    "fail an in-progress drain when stop is called before uploads complete" in {
      withTempDir { rootDir =>
        val entityId = "entity-stop-during-drain"
        val spool = new FilesystemChunkSpool(testKit.system, rootDir)
        spool.initialize(entityId, SpoolMeta.initial(entityId, "device-stop", "hash-stop", 64L, 2L)).futureValue

        (0L to 1L).foreach { seq =>
          spool.write(entityId, seq, Array.fill[Byte](32)((seq + 80L).toByte)).futureValue
        }

        val firstUploadStarted = Promise[Unit]()
        val releaseUploads = Promise[Unit]()
        val blobStore = new InMemoryBlobStoreBoilerplate()
        val delegate = new DefaultSingleShotBlobWriter(blobStore)

        val writer = new SingleShotBlobWriter {
          override def write(bucket: String, key: String, data: Array[Byte]): Future[BlobReference] = {
            val seq = key.takeRight(9).toLong
            if (seq == 0L) {
              firstUploadStarted.trySuccess(())
              releaseUploads.future.flatMap(_ => delegate.write(bucket, key, data))
            } else {
              delegate.write(bucket, key, data)
            }
          }
        }

        val flusher = new DefaultChunkFlusher(
          entityId = entityId,
          spool = spool,
          blobWriter = writer,
          bucket = "flusher-stop-during-drain",
          startSeq = 0L,
          parallelism = 2,
          tickInterval = 10.millis,
          drainTimeout = 2.seconds,
          maxUploadAttempts = 3,
          initialRetryBackoff = 10.millis,
          metaAdapter = testMetaAdapter
        )

        flusher.start()
        val drainFuture = flusher.drain()
        firstUploadStarted.future.futureValue

        flusher.stop()
        val failure = drainFuture.failed.futureValue
        failure shouldBe a[IllegalStateException]
        failure.getMessage should include("Chunk flusher stopped")
        flusher.isRunning.futureValue shouldBe false

        releaseUploads.trySuccess(())
      }
    }

    "fail drain on timeout and leave the flusher stopped" in {
      withTempDir { rootDir =>
        val entityId = "entity-drain-timeout"
        val releaseMetaRead = Promise[Unit]()
        val spool = new FilesystemChunkSpool(testKit.system, rootDir) {
          override def readMeta(entityId: String): Future[Option[SpoolMeta]] =
            releaseMetaRead.future.flatMap(_ => super.readMeta(entityId))(using testKit.system.executionContext)
        }
        spool.initialize(entityId, SpoolMeta.initial(entityId, "device-timeout", "hash-timeout", 64L, 2L)).futureValue

        (0L to 1L).foreach { seq =>
          spool.write(entityId, seq, Array.fill[Byte](32)((seq + 90L).toByte)).futureValue
        }

        val blobStore = new InMemoryBlobStoreBoilerplate()

        val flusher = new DefaultChunkFlusher(
          entityId = entityId,
          spool = spool,
          blobWriter = new DefaultSingleShotBlobWriter(blobStore),
          bucket = "flusher-timeout",
          startSeq = 0L,
          parallelism = 1,
          tickInterval = 10.millis,
          drainTimeout = 100.millis,
          maxUploadAttempts = 3,
          initialRetryBackoff = 10.millis,
          metaAdapter = testMetaAdapter
        )

        flusher.start()
        val drainFuture = flusher.drain()
        val secondDrain = flusher.drain()

        val failure = drainFuture.failed.futureValue
        failure shouldBe a[TimeoutException]
        failure.getMessage should include("Chunk flusher drain timeout")

        val secondFailure = secondDrain.failed.futureValue
        secondFailure shouldBe a[TimeoutException]
        secondFailure.getMessage should include("Chunk flusher drain timeout")

        flusher.isRunning.futureValue shouldBe false
        flusher.flushedSeq.futureValue shouldBe -1L

        val laterFailure = flusher.drain().failed.futureValue
        laterFailure shouldBe a[TimeoutException]
        laterFailure.getMessage should include("Chunk flusher drain timeout")

        releaseMetaRead.trySuccess(())

        eventually {
          flusher.flushedSeq.futureValue shouldBe -1L
          spool.readMeta(entityId).futureValue.get.flushedSeq shouldBe -1L
          blobStore.objectExists("flusher-timeout", expectedChunkKey("device-timeout", "hash-timeout", 0L)).futureValue shouldBe false
          blobStore.objectExists("flusher-timeout", expectedChunkKey("device-timeout", "hash-timeout", 1L)).futureValue shouldBe false
        }
      }
    }

    "fail drain on permanent upload failure without advancing watermark past the contiguous prefix" in {
      withTempDir { rootDir =>
        val entityId = "entity-permanent-failure"
        val spool = new FilesystemChunkSpool(testKit.system, rootDir)
        spool.initialize(entityId, SpoolMeta.initial(entityId, "device-permanent", "hash-permanent", 96L, 3L)).futureValue

        (0L to 2L).foreach { seq =>
          spool.write(entityId, seq, Array.fill[Byte](32)((seq + 40L).toByte)).futureValue
        }

        val blobStore = new InMemoryBlobStoreBoilerplate()
        val delegate = new DefaultSingleShotBlobWriter(blobStore)
        val seq0Uploaded = Promise[Unit]()
        val seq2Uploaded = Promise[Unit]()
        val releaseSeq1Failure = Promise[Unit]()
        val failuresSeen = TrieMap.empty[Long, Int]

        val writer = new SingleShotBlobWriter {
          override def write(bucket: String, key: String, data: Array[Byte]): Future[BlobReference] = {
            val seq = key.takeRight(9).toLong
            seq match {
              case 0L =>
                delegate.write(bucket, key, data).map { ref =>
                  seq0Uploaded.trySuccess(())
                  ref
                }
              case 1L =>
                seq0Uploaded.future.flatMap { _ =>
                  releaseSeq1Failure.future.flatMap { _ =>
                  val attempts = failuresSeen.getOrElse(seq, 0) + 1
                  failuresSeen.put(seq, attempts)
                  Future.failed(new RuntimeException("permanent failure"))
                  }
                }
              case 2L =>
                delegate.write(bucket, key, data).map { ref =>
                  seq2Uploaded.trySuccess(())
                  ref
                }
              case _ =>
                delegate.write(bucket, key, data)
            }
          }
        }

        val flusher = new DefaultChunkFlusher(
          entityId = entityId,
          spool = spool,
          blobWriter = writer,
          bucket = "flusher-permanent-failure",
          startSeq = 0L,
          parallelism = 3,
          tickInterval = 10.millis,
          drainTimeout = 2.seconds,
          maxUploadAttempts = 3,
          initialRetryBackoff = 10.millis,
          metaAdapter = testMetaAdapter
        )

        flusher.start()
        val drainFuture = flusher.drain()

        seq0Uploaded.future.futureValue
        seq2Uploaded.future.futureValue
        eventually {
          flusher.flushedSeq.futureValue shouldBe 0L
        }
        releaseSeq1Failure.success(())
        flusher.flushedSeq.futureValue shouldBe 0L

        val failure = drainFuture.failed.futureValue
        failure.getMessage should include ("permanent failure")
        failuresSeen(1L) shouldBe 3
        flusher.flushedSeq.futureValue shouldBe 0L
        spool.readMeta(entityId).futureValue.get.flushedSeq shouldBe 0L

        blobStore.objectExists("flusher-permanent-failure", expectedChunkKey("device-permanent", "hash-permanent", 0L)).futureValue shouldBe true
        blobStore.objectExists("flusher-permanent-failure", expectedChunkKey("device-permanent", "hash-permanent", 2L)).futureValue shouldBe true
        blobStore.objectExists("flusher-permanent-failure", expectedChunkKey("device-permanent", "hash-permanent", 1L)).futureValue shouldBe false
      }
    }

    "resume from the persisted flushed watermark without re-uploading older chunks" in {
      withTempDir { rootDir =>
        val entityId = "entity-resume"
        val spool = new FilesystemChunkSpool(testKit.system, rootDir)
        spool.initialize(entityId, SpoolMeta.initial(entityId, "device-resume", "hash-resume", 96L, 3L)).futureValue

        (0L to 2L).foreach { seq =>
          spool.write(entityId, seq, Array.fill[Byte](32)((seq + 50L).toByte)).futureValue
        }
        spool.updateFlushedSeq(entityId, 1L).futureValue

        val uploadedSeqs = TrieMap.empty[Long, Int]
        val blobStore = new InMemoryBlobStoreBoilerplate()
        val delegate = new DefaultSingleShotBlobWriter(blobStore)

        val writer = new SingleShotBlobWriter {
          override def write(bucket: String, key: String, data: Array[Byte]): Future[BlobReference] = {
            val seq = key.takeRight(9).toLong
            uploadedSeqs.put(seq, uploadedSeqs.getOrElse(seq, 0) + 1)
            delegate.write(bucket, key, data)
          }
        }

        val flusher = new DefaultChunkFlusher(
          entityId = entityId,
          spool = spool,
          blobWriter = writer,
          bucket = "flusher-resume",
          startSeq = 2L,
          parallelism = 3,
          tickInterval = 10.millis,
          drainTimeout = 2.seconds,
          maxUploadAttempts = 3,
          initialRetryBackoff = 10.millis,
          metaAdapter = testMetaAdapter
        )

        flusher.start()
        flusher.drain().futureValue shouldBe 2L
        flusher.flushedSeq.futureValue shouldBe 2L
        spool.readMeta(entityId).futureValue.get.flushedSeq shouldBe 2L

        uploadedSeqs.keySet shouldBe Set(2L)
        uploadedSeqs(2L) shouldBe 1

        blobStore.objectExists("flusher-resume", expectedChunkKey("device-resume", "hash-resume", 2L)).futureValue shouldBe true
        blobStore.objectExists("flusher-resume", expectedChunkKey("device-resume", "hash-resume", 0L)).futureValue shouldBe false
        blobStore.objectExists("flusher-resume", expectedChunkKey("device-resume", "hash-resume", 1L)).futureValue shouldBe false
      }
    }

    "drain to the final spooled sequence after flushing concurrently with ongoing ingest" in {
      withTempDir { rootDir =>
        val entityId = "entity-concurrent-ingest"
        val totalChunks = 48L
        val spool = new FilesystemChunkSpool(testKit.system, rootDir)
        spool.initialize(entityId, SpoolMeta.initial(entityId, "device-concurrent", "hash-concurrent", totalChunks * 32L, totalChunks)).futureValue

        val payloads = (0L until totalChunks).map { seq =>
          seq -> Array.fill[Byte](32)((seq % 100L).toByte)
        }.toMap

        val blobStore = new InMemoryBlobStoreBoilerplate()
        val flusher = new DefaultChunkFlusher(
          entityId = entityId,
          spool = spool,
          blobWriter = new DefaultSingleShotBlobWriter(blobStore),
          bucket = "flusher-concurrent-ingest",
          startSeq = 0L,
          parallelism = 6,
          tickInterval = 5.millis,
          drainTimeout = 5.seconds,
          maxUploadAttempts = 3,
          initialRetryBackoff = 10.millis,
          metaAdapter = testMetaAdapter
        )

        val halfwaySpooled = Promise[Unit]()

        def writeRemaining(seq: Long): Future[Unit] =
          if (seq >= totalChunks) Future.unit
          else {
            spool.write(entityId, seq, payloads(seq)).flatMap { _ =>
              if (seq == totalChunks / 2L) {
                halfwaySpooled.trySuccess(())
              }
              scheduleAfter(2.millis, testKit.system.classicSystem.scheduler)(writeRemaining(seq + 1L))(using summon[ExecutionContext])
            }
          }

        flusher.start()
        val ingestFuture = writeRemaining(0L)

        halfwaySpooled.future.futureValue
        eventually {
          flusher.flushedSeq.futureValue should be >= 0L
        }

        ingestFuture.futureValue
        flusher.drain().futureValue shouldBe totalChunks - 1L
        flusher.flushedSeq.futureValue shouldBe totalChunks - 1L
        spool.readMeta(entityId).futureValue.get.flushedSeq shouldBe totalChunks - 1L

        payloads.foreach { case (seq, expectedBytes) =>
          val key = expectedChunkKey("device-concurrent", "hash-concurrent", seq)
          blobStore.objectExists("flusher-concurrent-ingest", key).futureValue shouldBe true
          readObject(blobStore, "flusher-concurrent-ingest", key) shouldEqual expectedBytes
        }
      }
    }

    "report approximate in-memory flusher throughput for a larger backlog" ignore {
      withTempDir { rootDir =>
        val entityId = "entity-throughput"
        val totalChunks = 2000L
        val spool = new FilesystemChunkSpool(testKit.system, rootDir)
        spool.initialize(entityId, SpoolMeta.initial(entityId, "device-throughput", "hash-throughput", totalChunks * 32L, totalChunks)).futureValue

        (0L until totalChunks).foreach { seq =>
          spool.write(entityId, seq, Array.fill[Byte](32)((seq % 127L).toByte)).futureValue
        }

        val blobStore = new InMemoryBlobStoreBoilerplate()
        val flusher = new DefaultChunkFlusher(
          entityId = entityId,
          spool = spool,
          blobWriter = new DefaultSingleShotBlobWriter(blobStore),
          bucket = "flusher-throughput",
          startSeq = 0L,
          parallelism = 32,
          tickInterval = 5.millis,
          drainTimeout = 30.seconds,
          maxUploadAttempts = 3,
          initialRetryBackoff = 10.millis,
          metaAdapter = testMetaAdapter
        )

        val startedAt = System.nanoTime()
        flusher.start()
        flusher.drain().futureValue shouldBe totalChunks - 1L
        val elapsedMs = (System.nanoTime() - startedAt) / 1_000_000.0
        val chunksPerSecond = totalChunks.toDouble / (elapsedMs / 1000.0)
        val bytesPerSecond = (totalChunks.toDouble * 32.0) / (elapsedMs / 1000.0)

        info(f"in-memory flusher throughput: chunks=$totalChunks%d elapsedMs=$elapsedMs%.2f chunksPerSecond=$chunksPerSecond%.2f bytesPerSecond=$bytesPerSecond%.2f")
      }
    }
  }

  private def readObject(blobStore: InMemoryBlobStoreBoilerplate, bucket: String, key: String): Array[Byte] =
    blobStore
      .getObject(BlobReference(bucket, key, "", "", 0L))
      .runWith(Sink.fold(ByteString.empty)(_ ++ _))
      .futureValue
      .toArray

  private def withTempDir(testCode: Path => Any): Unit = {
    val tempDir = Files.createTempDirectory("default-chunk-flusher-spec-")
    try testCode(tempDir)
    finally {
      try deleteRecursively(tempDir)
      catch {
        case _: java.io.IOException => ()
      }
    }
  }

  private def deleteRecursively(path: Path): Unit = {
    if (Files.exists(path)) {
      Files.walkFileTree(path, new SimpleFileVisitor[Path]() {
        override def visitFile(file: Path, attrs: BasicFileAttributes): FileVisitResult = {
          Files.deleteIfExists(file)
          FileVisitResult.CONTINUE
        }

        override def postVisitDirectory(dir: Path, exc: java.io.IOException | Null): FileVisitResult = {
          Files.deleteIfExists(dir)
          FileVisitResult.CONTINUE
        }

        override def visitFileFailed(file: Path, exc: java.io.IOException): FileVisitResult =
          exc match {
            case _: java.nio.file.NoSuchFileException => FileVisitResult.CONTINUE
            case other => throw other
          }
      })
    }
  }
}
