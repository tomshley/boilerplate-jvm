package com.tomshley.boilerplate.jvm.objectstorage

import com.tomshley.boilerplate.jvm.objectstorage.models.{BlobReference, ObjectMetadata, PartETag, UploadSession}
import org.apache.pekko.Done
import org.apache.pekko.actor.testkit.typed.scaladsl.ActorTestKit
import org.apache.pekko.actor.typed.ActorSystem
import org.apache.pekko.stream.Materializer
import org.apache.pekko.stream.scaladsl.Source
import org.apache.pekko.util.ByteString
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{Milliseconds, Seconds, Span}
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.{ExecutionContext, Future}

final class SingleShotBlobWriterSpec
    extends AnyWordSpec
    with Matchers
    with ScalaFutures
    with BeforeAndAfterAll {

  private val testKit = ActorTestKit("SingleShotBlobWriterSpec")
  private given ActorSystem[?] = testKit.system
  private given ExecutionContext = testKit.system.executionContext
  private given Materializer = Materializer(testKit.system)

  override implicit val patienceConfig: PatienceConfig =
    PatienceConfig(timeout = Span(3, Seconds), interval = Span(25, Milliseconds))

  override def afterAll(): Unit = {
    testKit.shutdownTestKit()
    super.afterAll()
  }

  "DefaultSingleShotBlobWriter" should {
    "write bytes to the backing blob store and return a BlobReference" in {
      val store = new InMemoryBlobStoreBoilerplate()
      val writer: SingleShotBlobWriter = new DefaultSingleShotBlobWriter(store)
      val bytes = "hello-single-shot".getBytes("UTF-8")

      val ref = writer.write("bucket-a", "key-a", bytes).futureValue

      ref.bucket shouldBe "bucket-a"
      ref.key shouldBe "key-a"
      ref.sizeBytes shouldBe bytes.length.toLong
      store.objectExists("bucket-a", "key-a").futureValue shouldBe true
      store.getObject(ref).runFold(ByteString.empty)(_ ++ _).futureValue shouldBe ByteString(bytes)
    }

    "abort the upload when a write step fails" in {
      val store = new FailingBlobStore
      val writer: SingleShotBlobWriter = new DefaultSingleShotBlobWriter(store)

      val thrown = writer.write("bucket-b", "key-b", "boom".getBytes("UTF-8")).failed.futureValue

      thrown shouldBe store.uploadFailure
      store.abortedSessions should have size 1
      store.abortedSessions.head shouldBe store.initiatedSession.get
    }
  }

  private final class FailingBlobStore extends BlobStoreBoilerplate {
    val session: UploadSession = UploadSession("bucket-b", "key-b", "upload-1")
    val uploadFailure: IllegalStateException = new IllegalStateException("upload failed")
    var abortedSessions: Vector[UploadSession] = Vector.empty
    var initiatedSession: Option[UploadSession] = None

    override def initiateUpload(
        bucket: String,
        key: String,
        metadata: Map[String, String]
    ): Future[UploadSession] = {
      val actualSession = session.copy(bucket = bucket, key = key)
      initiatedSession = Some(actualSession)
      Future.successful(actualSession)
    }

    override def uploadPart(
        session: UploadSession,
        partNo: Int,
        data: Source[ByteString, ?]
    ): Future[PartETag] = Future.failed(uploadFailure)

    override def completeUpload(
        session: UploadSession,
        parts: Seq[PartETag]
    ): Future[BlobReference] =
      Future.failed(new IllegalStateException("completeUpload should not be called"))

    override def abortUpload(session: UploadSession): Future[Done] = {
      abortedSessions = abortedSessions :+ session
      Future.successful(Done)
    }

    override def listParts(session: UploadSession): Future[Seq[PartETag]] = Future.successful(Seq.empty)

    override def getObject(ref: BlobReference): Source[ByteString, Future[ObjectMetadata]] =
      Source.failed(new IllegalStateException("getObject should not be called"))
        .mapMaterializedValue(_ => Future.failed(new IllegalStateException("getObject should not be called")))

    override def deleteObject(ref: BlobReference): Future[Done] = Future.successful(Done)

    override def objectExists(bucket: String, key: String): Future[Boolean] = Future.successful(false)

    override def close(): Future[Done] = Future.successful(Done)
  }
}
