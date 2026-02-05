package com.tomshley.boilerplate.jvm.objectstorage

import com.tomshley.boilerplate.jvm.objectstorage.models.BlobReference
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.Materializer
import org.apache.pekko.stream.scaladsl.Source
import org.apache.pekko.stream.scaladsl.Sink
import org.apache.pekko.stream.scaladsl.Keep
import org.apache.pekko.util.ByteString
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.ExecutionContext

final class InMemoryBlobStoreBoilerplateSpec
    extends AnyWordSpec
    with Matchers
    with ScalaFutures {

  private implicit val system: ActorSystem = ActorSystem("InMemoryBlobStoreBoilerplateSpec")
  private implicit val mat: Materializer = Materializer(system)
  private implicit val ec: ExecutionContext = system.dispatcher

  private val underTest = new InMemoryBlobStoreBoilerplate()

  "InMemoryBlobStoreBoilerplate" should {
    "initiate uploads" in {
      val sessionF = underTest.initiateUpload("b", "k", Map("x" -> "y"))
      whenReady(sessionF) { session =>
        session.bucket shouldBe "b"
        session.key shouldBe "k"
        session.uploadId.nonEmpty shouldBe true
      }
    }

    "upload parts and list them" in {
      val session = underTest.initiateUpload("b", "k").futureValue

      val p1 = underTest.uploadPart(session, 1, Source.single(ByteString("hello"))).futureValue
      val p2 = underTest.uploadPart(session, 2, Source.single(ByteString("world"))).futureValue

      p1.partNo shouldBe 1
      p1.sizeBytes shouldBe 5
      p1.etag.nonEmpty shouldBe true

      p2.partNo shouldBe 2
      p2.sizeBytes shouldBe 5

      val parts = underTest.listParts(session).futureValue
      parts.map(_.partNo) shouldBe Seq(1, 2)
      parts should contain theSameElementsAs Seq(p1, p2)
    }

    "complete uploads and allow getObject" in {
      val session = underTest.initiateUpload("b", "k").futureValue

      val p1 = underTest.uploadPart(session, 1, Source.single(ByteString("hello"))).futureValue
      val p2 = underTest.uploadPart(session, 2, Source.single(ByteString("world"))).futureValue

      val ref = underTest.completeUpload(session, Seq(p1, p2)).futureValue

      ref.bucket shouldBe "b"
      ref.key shouldBe "k"
      ref.sizeBytes shouldBe 10
      ref.checksum.nonEmpty shouldBe true
      ref.etag.nonEmpty shouldBe true

      val src = underTest.getObject(ref)
      val (metaF, bytesF) = src.toMat(Sink.fold(ByteString.empty)(_ ++ _))(Keep.both).run()

      val bytes = bytesF.futureValue
      bytes shouldBe ByteString("helloworld")

      val meta = metaF.futureValue
      meta.contentLength shouldBe 10
    }

    "delete objects and report existence correctly" in {
      val session = underTest.initiateUpload("b", "k").futureValue
      val p1 = underTest.uploadPart(session, 1, Source.single(ByteString("hello"))).futureValue
      val ref = underTest.completeUpload(session, Seq(p1)).futureValue

      underTest.objectExists("b", "k").futureValue shouldBe true
      underTest.deleteObject(ref).futureValue
      underTest.objectExists("b", "k").futureValue shouldBe false
    }

    "abort uploads" in {
      val session = underTest.initiateUpload("b", "k").futureValue
      underTest.uploadPart(session, 1, Source.single(ByteString("hello"))).futureValue

      underTest.abortUpload(session).futureValue

      val parts = underTest.listParts(session).futureValue
      parts shouldBe empty
    }

    "fail getObject for unknown ref" in {
      val missing: BlobReference = BlobReference("b", "missing", "etag", "checksum", 0L)

      val src = underTest.getObject(missing)
      whenReady(src.runFold(ByteString.empty)(_ ++ _).failed) { ex =>
        ex.isInstanceOf[NoSuchElementException] shouldBe true
      }
    }
  }
}
