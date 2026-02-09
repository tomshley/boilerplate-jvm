/*
 * Copyright 2026 Tomshley LLC.
 * All Rights Reserved.
 */

package com.tomshley.boilerplate.jvm.claimcheck

import com.tomshley.boilerplate.jvm.objectstorage.{BlobStoreBoilerplate, InMemoryBlobStoreBoilerplate}
import com.tomshley.boilerplate.jvm.objectstorage.models.{BlobReference, PartETag}
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.Materializer
import org.apache.pekko.stream.scaladsl.Source
import org.apache.pekko.util.ByteString
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{Millis, Seconds, Span}
import org.scalatest.wordspec.AnyWordSpec

import java.util.concurrent.atomic.AtomicBoolean
import scala.concurrent.{ExecutionContext, Future}

final class ClaimCheckStorageAdapterSpec
    extends AnyWordSpec
    with Matchers
    with ScalaFutures
    with BeforeAndAfterEach
    with BeforeAndAfterAll {

  implicit val patience: PatienceConfig =
    PatienceConfig(timeout = Span(5, Seconds), interval = Span(50, Millis))

  private given system: ActorSystem = ActorSystem("ClaimCheckStorageAdapterSpec")
  private given mat: Materializer = Materializer(system)
  private given ec: ExecutionContext = system.dispatcher

  private object UnderTest extends ClaimCheckStorageAdapter {
    override protected val store: BlobStoreBoilerplate = new InMemoryBlobStoreBoilerplate()
    override protected val defaultBucket: String = "test-bucket"
    override protected val storeType: String = "memory"
    override protected def transferObjectKey(transferId: String): String =
      s"transfers/$transferId/complete"
    override protected def locationUri(bucket: String, itemKey: String): String =
      s"s3://$bucket/$itemKey"
  }

  override def beforeEach(): Unit = {
    UnderTest.resetForTest()
  }

  override def afterAll(): Unit = {
    system.terminate()
  }

  "ClaimCheckStorageAdapter.storeItem" should {
    "return a ClaimTicket with correct fields" in {
      val data = ByteString("chunk-data")
      val ticket = UnderTest.storeItem(
        "transfer-1", "transfers/mac/transfer-1/part-1", 1, Source.single(data)
      ).futureValue

      ticket.number shouldBe "transfers/mac/transfer-1/part-1"
      ticket.location shouldBe "s3://test-bucket/transfers/mac/transfer-1/part-1"
      ticket.storeType shouldBe "memory"
      ticket.checkedAt shouldBe defined
      ticket.sizeBytes shouldBe Some(data.length.toLong)
    }

    "handle multiple items in the same transfer" in {
      val t1 = UnderTest.storeItem(
        "transfer-1", "part-1", 1, Source.single(ByteString("chunk-1"))
      ).futureValue
      val t2 = UnderTest.storeItem(
        "transfer-1", "part-2", 2, Source.single(ByteString("chunk-2"))
      ).futureValue

      t1.number shouldBe "part-1"
      t2.number shouldBe "part-2"
      t1.sizeBytes shouldBe Some(7L)
      t2.sizeBytes shouldBe Some(7L)
    }
  }

  "ClaimCheckStorageAdapter.completeTransfer" should {
    "assemble parts and return BlobReference" in {
      UnderTest.storeItem(
        "transfer-1", "part-1", 1, Source.single(ByteString("aaa"))
      ).futureValue
      UnderTest.storeItem(
        "transfer-1", "part-2", 2, Source.single(ByteString("bbb"))
      ).futureValue

      val ref: BlobReference = UnderTest.completeTransfer("transfer-1").futureValue
      ref.bucket shouldBe "test-bucket"
      ref.key shouldBe "transfers/transfer-1/complete"
      ref.sizeBytes shouldBe 6L
    }

    "fail for unknown transfer" in {
      val ex = UnderTest.completeTransfer("unknown").failed.futureValue
      ex shouldBe an[IllegalStateException]
      ex.getMessage should include("unknown")
    }

    "clean up session state after completion" in {
      UnderTest.storeItem(
        "transfer-1", "part-1", 1, Source.single(ByteString("data"))
      ).futureValue
      UnderTest.completeTransfer("transfer-1").futureValue

      // Second complete should fail — session already cleaned up
      val ex = UnderTest.completeTransfer("transfer-1").failed.futureValue
      ex shouldBe an[IllegalStateException]
    }

    "succeed with correct expectedPartCount" in {
      UnderTest.storeItem(
        "transfer-1", "part-1", 1, Source.single(ByteString("aaa"))
      ).futureValue
      UnderTest.storeItem(
        "transfer-1", "part-2", 2, Source.single(ByteString("bbb"))
      ).futureValue

      val ref = UnderTest.completeTransfer("transfer-1", expectedPartCount = Some(2)).futureValue
      ref.sizeBytes shouldBe 6L
    }

    "fail with wrong expectedPartCount" in {
      UnderTest.storeItem(
        "transfer-1", "part-1", 1, Source.single(ByteString("data"))
      ).futureValue

      val ex = UnderTest.completeTransfer("transfer-1", expectedPartCount = Some(3)).failed.futureValue
      ex shouldBe an[IllegalStateException]
      ex.getMessage should include("Expected 3 parts")
    }
  }

  "ClaimCheckStorageAdapter.storeItem duplicate itemNo" should {
    "fail-fast on duplicate itemNo within the same transfer" in {
      UnderTest.storeItem(
        "transfer-dup", "part-1", 1, Source.single(ByteString("first"))
      ).futureValue

      val ex = UnderTest.storeItem(
        "transfer-dup", "part-1-dup", 1, Source.single(ByteString("second"))
      ).failed.futureValue

      ex shouldBe an[IllegalStateException]
      ex.getMessage should include("Duplicate itemNo 1")
    }

    "allow same itemNo across different transfers" in {
      val t1 = UnderTest.storeItem(
        "transfer-a", "a-part-1", 1, Source.single(ByteString("data-a"))
      ).futureValue
      val t2 = UnderTest.storeItem(
        "transfer-b", "b-part-1", 1, Source.single(ByteString("data-b"))
      ).futureValue

      t1.number shouldBe "a-part-1"
      t2.number shouldBe "b-part-1"
    }
  }

  "ClaimCheckStorageAdapter.storeItem retry after failure" should {
    "allow retry of same itemNo after upload failure" in {
      val shouldFail = new AtomicBoolean(true)

      object FailOnceAdapter extends ClaimCheckStorageAdapter {
        private val delegate = new InMemoryBlobStoreBoilerplate()
        override protected val store: BlobStoreBoilerplate = new BlobStoreBoilerplate {
          export delegate.{uploadPart as _, *}
          override def uploadPart(
              session: com.tomshley.boilerplate.jvm.objectstorage.models.UploadSession,
              partNo: Int,
              data: Source[ByteString, ?]
          ): Future[PartETag] = {
            if (shouldFail.getAndSet(false))
              Future.failed(new RuntimeException("Simulated S3 failure"))
            else
              delegate.uploadPart(session, partNo, data)
          }
        }
        override protected val defaultBucket: String = "test-bucket"
        override protected val storeType: String = "memory"
        override protected def transferObjectKey(transferId: String): String =
          s"transfers/$transferId/complete"
        override protected def locationUri(bucket: String, itemKey: String): String =
          s"s3://$bucket/$itemKey"
      }

      // First attempt fails
      val ex = FailOnceAdapter.storeItem(
        "transfer-retry", "part-1", 1, Source.single(ByteString("data"))
      ).failed.futureValue
      ex shouldBe a[RuntimeException]
      ex.getMessage should include("Simulated")

      // Retry same itemNo — should succeed now that reservation was released
      val ticket = FailOnceAdapter.storeItem(
        "transfer-retry", "part-1", 1, Source.single(ByteString("data"))
      ).futureValue
      ticket.number shouldBe "part-1"
    }
  }

  "ClaimCheckStorageAdapter.completeTransfer with no parts" should {
    "fail with IllegalStateException when session exists but no parts were stored" in {
      // Use a store where uploadPart always fails, so initiateUpload succeeds
      // (session is created) but no PartETag is ever recorded.
      object NoPartsAdapter extends ClaimCheckStorageAdapter {
        private val delegate = new InMemoryBlobStoreBoilerplate()
        override protected val store: BlobStoreBoilerplate = new BlobStoreBoilerplate {
          export delegate.{uploadPart as _, *}
          override def uploadPart(
              session: com.tomshley.boilerplate.jvm.objectstorage.models.UploadSession,
              partNo: Int,
              data: Source[ByteString, ?]
          ): Future[PartETag] =
            Future.failed(new RuntimeException("Simulated upload failure"))
        }
        override protected val defaultBucket: String = "test-bucket"
        override protected val storeType: String = "memory"
        override protected def transferObjectKey(transferId: String): String =
          s"transfers/$transferId/complete"
        override protected def locationUri(bucket: String, itemKey: String): String =
          s"s3://$bucket/$itemKey"
      }

      // storeItem fails (uploadPart fails), but the session was created
      NoPartsAdapter.storeItem(
        "transfer-noparts", "part-1", 1, Source.single(ByteString("data"))
      ).failed.futureValue

      // completeTransfer should find the session but no parts
      val ex = NoPartsAdapter.completeTransfer("transfer-noparts").failed.futureValue
      ex shouldBe an[IllegalStateException]
      ex.getMessage should include("No parts found")
    }
  }

  "ClaimCheckStorageAdapter.completeTransfer retry after failure" should {
    "preserve state and succeed on retry after transient completeUpload failure" in {
      val shouldFail = new AtomicBoolean(true)

      object RetryCompleteAdapter extends ClaimCheckStorageAdapter {
        private val delegate = new InMemoryBlobStoreBoilerplate()
        override protected val store: BlobStoreBoilerplate = new BlobStoreBoilerplate {
          export delegate.{completeUpload as _, *}
          override def completeUpload(
              session: com.tomshley.boilerplate.jvm.objectstorage.models.UploadSession,
              parts: Seq[PartETag]
          ): Future[com.tomshley.boilerplate.jvm.objectstorage.models.BlobReference] = {
            if (shouldFail.getAndSet(false))
              Future.failed(new RuntimeException("Simulated S3 completeUpload failure"))
            else
              delegate.completeUpload(session, parts)
          }
        }
        override protected val defaultBucket: String = "test-bucket"
        override protected val storeType: String = "memory"
        override protected def transferObjectKey(transferId: String): String =
          s"transfers/$transferId/complete"
        override protected def locationUri(bucket: String, itemKey: String): String =
          s"s3://$bucket/$itemKey"
      }

      // Store a part
      RetryCompleteAdapter.storeItem(
        "transfer-retry-complete", "part-1", 1, Source.single(ByteString("data"))
      ).futureValue

      // First completeTransfer fails
      val ex = RetryCompleteAdapter.completeTransfer("transfer-retry-complete").failed.futureValue
      ex shouldBe a[RuntimeException]
      ex.getMessage should include("Simulated")

      // Retry — state was preserved, so this succeeds
      val ref = RetryCompleteAdapter.completeTransfer("transfer-retry-complete").futureValue
      ref.sizeBytes shouldBe 4L
    }
  }

  "ClaimCheckStorageAdapter.abortTransfer" should {
    "clean up session state" in {
      UnderTest.storeItem(
        "transfer-1", "part-1", 1, Source.single(ByteString("data"))
      ).futureValue

      UnderTest.abortTransfer("transfer-1").futureValue

      // Complete should fail after abort
      val ex = UnderTest.completeTransfer("transfer-1").failed.futureValue
      ex shouldBe an[IllegalStateException]
    }

    "succeed for unknown transfer (no-op)" in {
      UnderTest.abortTransfer("unknown").futureValue shouldBe (())
    }
  }
}
