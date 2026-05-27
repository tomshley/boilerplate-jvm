/*
 * Copyright 2026 Tomshley LLC.
 * All Rights Reserved.
 */

package com.tomshley.boilerplate.jvm.durablebufferedflush

import com.tomshley.boilerplate.jvm.durablebufferedflush.SpoolMeta
import com.tomshley.boilerplate.jvm.durablebufferedflush.internal.FilesystemChunkSpool
import org.apache.pekko.actor.testkit.typed.scaladsl.ActorTestKit
import org.apache.pekko.actor.typed.ActorSystem
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{Millis, Seconds, Span}
import org.scalatest.wordspec.AnyWordSpec

import java.nio.file.attribute.BasicFileAttributes
import java.nio.file.{FileVisitResult, Files, Path, SimpleFileVisitor}
import scala.concurrent.{Await, ExecutionContext, Future, Promise}
import scala.concurrent.duration.*

class FilesystemChunkSpoolSpec
    extends AnyWordSpec
    with Matchers
    with BeforeAndAfterAll
    with ScalaFutures {

  override implicit val patienceConfig: PatienceConfig =
    PatienceConfig(timeout = Span(5, Seconds), interval = Span(50, Millis))

  private val testKit = ActorTestKit("FilesystemChunkSpoolSpec")
  private given ActorSystem[?] = testKit.system
  private given ExecutionContext = testKit.system.executionContext

  override def afterAll(): Unit = {
    testKit.shutdownTestKit()
    super.afterAll()
  }

  "FilesystemChunkSpool" should {

    "initialize and write 100 chunks with correct bytes and padded filenames" in {
      withTempDir { rootDir =>
        val spool = new FilesystemChunkSpool(testKit.system, rootDir)
        val entityId = "entity-100"
        spool.initialize(
          entityId,
          SpoolMeta.initial(entityId, "device-1", "hash-1", 3200L, 100L)
        ).futureValue

        val payloads = (0 until 100).map(i => Array.fill[Byte](32)((i % 127).toByte))
        payloads.zipWithIndex.foreach { case (bytes, seq) =>
          spool.write(entityId, seq.toLong, bytes).futureValue shouldBe bytes.length.toLong
        }

        val meta = spool.readMeta(entityId).futureValue.get
        meta.lastSpooledSeq shouldBe 99L
        meta.totalSpooledBytes shouldBe payloads.map(_.length.toLong).sum
        meta.totalExpectedChunks shouldBe 100L

        payloads.zipWithIndex.foreach { case (bytes, seq) =>
          val filePath = rootDir.resolve(entityId).resolve("chunks").resolve(f"$seq%09d.bin")
          Files.exists(filePath) shouldBe true
          spool.readChunk(entityId, seq.toLong).futureValue shouldEqual bytes
        }
      }
    }

    "reject non-contiguous and duplicate sequence numbers" in {
      withTempDir { rootDir =>
        val spool = new FilesystemChunkSpool(testKit.system, rootDir)
        val entityId = "entity-gap"
        spool.initialize(
          entityId,
          SpoolMeta.initial(entityId, "device-2", "hash-2", 64L, 3L)
        ).futureValue

        spool.write(entityId, 0L, Array[Byte](1, 2, 3)).futureValue

        spool.write(entityId, 2L, Array[Byte](4, 5, 6)).failed.futureValue shouldBe a[IllegalArgumentException]
        spool.write(entityId, 0L, Array[Byte](7, 8, 9)).failed.futureValue shouldBe a[IllegalArgumentException]
      }
    }

    "keep entities isolated under concurrent writes" in {
      withTempDir { rootDir =>
        val spool = new FilesystemChunkSpool(testKit.system, rootDir)
        val entityIds = (1 to 5).map(i => s"entity-$i")

        entityIds.foreach { entityId =>
          spool.initialize(
            entityId,
            SpoolMeta.initial(entityId, s"device-$entityId", s"hash-$entityId", 96L, 3L)
          ).futureValue
        }

        Future.sequence {
          entityIds.map { entityId =>
            (0L to 2L).foldLeft(Future.successful(())) { (acc, seq) =>
              acc.flatMap(_ =>
                spool.write(entityId, seq, Array.fill[Byte](16)((seq + entityId.length).toByte)).map(_ => ())
              )
            }
          }
        }.futureValue

        spool.listEntities().futureValue should contain theSameElementsAs entityIds
        entityIds.foreach { entityId =>
          val meta = spool.readMeta(entityId).futureValue.get
          meta.lastSpooledSeq shouldBe 2L
          meta.totalSpooledBytes shouldBe 48L
        }
      }
    }

    "reject entity IDs that can escape or poison spool paths" in {
      withTempDir { rootDir =>
        val spool = new FilesystemChunkSpool(testKit.system, rootDir)

        spool.initialize("../escape", SpoolMeta.initial("../escape", "device-escape", "hash-escape", 64L, 1L)).failed.futureValue shouldBe a[IllegalArgumentException]
        spool.readMeta("entity/child").failed.futureValue shouldBe a[IllegalArgumentException]
        spool.cleanup("entity\\child").failed.futureValue shouldBe a[IllegalArgumentException]
      }
    }

    "wait for an in-flight write to finish before cleanup removes the entity" in {
      withTempDir { rootDir =>
        val spool = new BlockingFilesystemChunkSpool(testKit.system, rootDir)
        val entityId = "entity-cleanup-write-race"
        spool.initialize(
          entityId,
          SpoolMeta.initial(entityId, "device-race", "hash-race", 64L, 2L)
        ).futureValue

        spool.blockChunkWrite = true
        val writeFuture = spool.write(entityId, 0L, Array.fill[Byte](32)(1))
        spool.chunkWriteEntered.future.futureValue

        val cleanupFuture = spool.cleanup(entityId)
        Thread.sleep(200)
        cleanupFuture.isCompleted shouldBe false

        spool.allowChunkWrite.trySuccess(())

        writeFuture.futureValue shouldBe 32L
        cleanupFuture.futureValue
        spool.readMeta(entityId).futureValue shouldBe None
        spool.listEntities().futureValue shouldBe empty
      }
    }

    "wait for an in-flight meta read to finish before cleanup removes the entity" in {
      withTempDir { rootDir =>
        val spool = new BlockingFilesystemChunkSpool(testKit.system, rootDir)
        val entityId = "entity-cleanup-readmeta-race"
        spool.initialize(
          entityId,
          SpoolMeta.initial(entityId, "device-readmeta-race", "hash-readmeta-race", 64L, 1L)
        ).futureValue

        spool.blockMetaRead = true
        val readFuture = spool.readMeta(entityId)
        spool.metaReadEntered.future.futureValue

        val cleanupFuture = spool.cleanup(entityId)
        Thread.sleep(200)
        cleanupFuture.isCompleted shouldBe false

        spool.allowMetaRead.trySuccess(())

        readFuture.futureValue.get.entityId shouldBe entityId
        cleanupFuture.futureValue
        spool.readMeta(entityId).futureValue shouldBe None
      }
    }

    "wait for a metadata rewrite to finish before cleanup removes the entity" in {
      withTempDir { rootDir =>
        val spool = new BlockingFilesystemChunkSpool(testKit.system, rootDir)
        val entityId = "entity-cleanup-meta-race"
        spool.initialize(
          entityId,
          SpoolMeta.initial(entityId, "device-meta-race", "hash-meta-race", 64L, 2L)
        ).futureValue
        spool.write(entityId, 0L, Array.fill[Byte](32)(2)).futureValue

        spool.blockMetaRename = true
        val updateFuture = spool.updateFlushedSeq(entityId, 0L)
        spool.metaRenameEntered.future.futureValue

        val cleanupFuture = spool.cleanup(entityId)
        Thread.sleep(200)
        cleanupFuture.isCompleted shouldBe false

        spool.allowMetaRename.trySuccess(())

        updateFuture.futureValue
        cleanupFuture.futureValue
        spool.readMeta(entityId).futureValue shouldBe None
        spool.listEntities().futureValue shouldBe empty
      }
    }

    // Track F.14.1 — `currentSizeBytes` increments by exactly the bytes
    // written on every successful chunk write across multiple entities.
    // The accounting is actor-owned and reads are async — futureValue
    // is required on every assertion. Mailbox FIFO + the Promise
    // happens-before from `write().futureValue` guarantees the
    // Increment message lands before any subsequent Query.
    "increment SpoolSizeReporter.currentSizeBytes on every chunk write" in {
      withTempDir { rootDir =>
        val spool = new FilesystemChunkSpool(testKit.system, rootDir)
        spool.currentSizeBytes().futureValue shouldBe 0L

        val entityA = "entity-size-a"
        val entityB = "entity-size-b"
        spool.initialize(entityA, SpoolMeta.initial(entityA, "device-a", "hash-a", 256L, 4L)).futureValue
        spool.initialize(entityB, SpoolMeta.initial(entityB, "device-b", "hash-b", 256L, 4L)).futureValue

        spool.write(entityA, 0L, Array.fill[Byte](64)(1)).futureValue
        spool.currentSizeBytes().futureValue shouldBe 64L

        spool.write(entityA, 1L, Array.fill[Byte](32)(2)).futureValue
        spool.currentSizeBytes().futureValue shouldBe (64L + 32L)

        spool.write(entityB, 0L, Array.fill[Byte](128)(3)).futureValue
        spool.currentSizeBytes().futureValue shouldBe (64L + 32L + 128L)
      }
    }

    // Track F.14.1 — `currentSizeBytes` decrements when an entity is
    // cleaned up. The decrement is driven by meta's `totalSpooledBytes`
    // (not a re-walk of the filesystem); meta / sidecar files are not
    // counted in `currentSizeBytes` because they are not counted on the
    // write path either.
    "decrement SpoolSizeReporter.currentSizeBytes when an entity is cleaned up" in {
      withTempDir { rootDir =>
        val spool = new FilesystemChunkSpool(testKit.system, rootDir)
        val entityA = "entity-size-cleanup-a"
        val entityB = "entity-size-cleanup-b"

        spool.initialize(entityA, SpoolMeta.initial(entityA, "device-a", "hash-a", 256L, 4L)).futureValue
        spool.initialize(entityB, SpoolMeta.initial(entityB, "device-b", "hash-b", 256L, 4L)).futureValue
        spool.write(entityA, 0L, Array.fill[Byte](48)(1)).futureValue
        spool.write(entityA, 1L, Array.fill[Byte](48)(2)).futureValue
        spool.write(entityB, 0L, Array.fill[Byte](32)(3)).futureValue
        spool.currentSizeBytes().futureValue shouldBe (48L + 48L + 32L)

        spool.cleanup(entityA).futureValue
        spool.currentSizeBytes().futureValue shouldBe 32L

        spool.cleanup(entityB).futureValue
        spool.currentSizeBytes().futureValue shouldBe 0L
      }
    }

    // Track F.14.1 — `recountFromFilesystem` walks the spool root, returns
    // the authoritative chunk-byte total, AND emits a `ReplaceWith` message
    // to the size-accounting actor so that synthetic drift introduced after
    // the last write is corrected on the next read.
    "recountFromFilesystem returns the on-disk total and corrects in-memory drift" in {
      withTempDir { rootDir =>
        val spool = new FilesystemChunkSpool(testKit.system, rootDir)
        val entityId = "entity-recount"
        spool.initialize(entityId, SpoolMeta.initial(entityId, "device-r", "hash-r", 256L, 4L)).futureValue
        spool.write(entityId, 0L, Array.fill[Byte](40)(1)).futureValue
        spool.write(entityId, 1L, Array.fill[Byte](40)(2)).futureValue
        spool.currentSizeBytes().futureValue shouldBe 80L

        // Simulate drift: a hand-crafted chunk file outside the spool's
        // own bookkeeping. The recount must observe it.
        val rogueChunk = rootDir.resolve(entityId).resolve("chunks").resolve("000000002.bin")
        java.nio.file.Files.write(rogueChunk, Array.fill[Byte](20)(7))

        val recountTotal = spool.recountFromFilesystem().futureValue
        recountTotal shouldBe (80L + 20L)
        // The actor-owned counter is corrected by the ReplaceWith message
        // the recount enqueued. The Future and the message are dispatched
        // from the same blocking-EC thread, so subsequent reads observe
        // the replaced value (mailbox FIFO).
        spool.currentSizeBytes().futureValue shouldBe (80L + 20L)
      }
    }
  }

  private final class BlockingFilesystemChunkSpool(
      system: ActorSystem[?],
      rootDir: Path
  ) extends FilesystemChunkSpool(system, rootDir) {

    val chunkWriteEntered = Promise[Unit]()
    val allowChunkWrite = Promise[Unit]()
    val metaReadEntered = Promise[Unit]()
    val allowMetaRead = Promise[Unit]()
    val metaRenameEntered = Promise[Unit]()
    val allowMetaRename = Promise[Unit]()
    @volatile var blockChunkWrite = false
    @volatile var blockMetaRead = false
    @volatile var blockMetaRename = false

    override protected def beforeChunkWrite(entityId: String, seq: Long, path: Path, bytes: Array[Byte]): Unit = {
      if (blockChunkWrite) {
        chunkWriteEntered.trySuccess(())
        Await.result(allowChunkWrite.future, 5.seconds)
      }
    }

    override protected def beforeMetaRead(entityId: String, path: Path): Unit = {
      if (blockMetaRead) {
        metaReadEntered.trySuccess(())
        Await.result(allowMetaRead.future, 5.seconds)
      }
    }

    override protected def beforeMetaRename(entityId: String, meta: SpoolMeta, tmpPath: Path, finalPath: Path): Unit = {
      if (blockMetaRename) {
        metaRenameEntered.trySuccess(())
        Await.result(allowMetaRename.future, 5.seconds)
      }
    }
  }

  private def withTempDir(testCode: Path => Any): Unit = {
    val tempDir = Files.createTempDirectory("chunk-spool-spec-")
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
