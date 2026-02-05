package com.tomshley.boilerplate.jvm.filetransfer

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

final class ChunkedTransferBoilerplateSpec extends AnyWordSpec with Matchers {

  private object UnderTest extends ChunkedTransferBoilerplate

  private final case class State(
      uploadId: Option[String],
      bucket: Option[String],
      key: Option[String],
      uploadedParts: Map[Int, PartInfo],
      isInProgress: Boolean,
      isComplete: Boolean
  ) extends ChunkedTransferState

  private def updateFrom(old: State)(
      uploadId: Option[String],
      bucket: Option[String],
      key: Option[String],
      uploadedParts: Map[Int, PartInfo],
      isInProgress: Boolean,
      isComplete: Boolean
  ): State = old.copy(
    uploadId = uploadId,
    bucket = bucket,
    key = key,
    uploadedParts = uploadedParts,
    isInProgress = isInProgress,
    isComplete = isComplete
  )

  "ChunkedTransferBoilerplate.applyTransferEvent" should {
    "handle UploadInitiated" in {
      val s0 = State(None, None, None, Map.empty, isInProgress = false, isComplete = false)
      val ev = UnderTest.initiatedEvent("u1", "b", "k")

      val s1 = UnderTest.applyTransferEvent(s0, ev)(updateFrom(s0))
      s1.uploadId shouldBe Some("u1")
      s1.bucket shouldBe Some("b")
      s1.key shouldBe Some("k")
      s1.isInProgress shouldBe true
      s1.isComplete shouldBe false
      s1.uploadedParts shouldBe Map.empty
    }

    "handle PartUploaded" in {
      val s0 = State(Some("u1"), Some("b"), Some("k"), Map.empty, isInProgress = true, isComplete = false)
      val ev = UnderTest.partUploadedEvent(1, "etag", 10)

      val s1 = UnderTest.applyTransferEvent(s0, ev)(updateFrom(s0))
      s1.uploadedParts(1) shouldBe PartInfo("etag", 10)
      s1.isInProgress shouldBe true
      s1.isComplete shouldBe false
    }

    "handle UploadCompleted" in {
      val s0 = State(Some("u1"), Some("b"), Some("k"), Map(1 -> PartInfo("e", 10)), isInProgress = true, isComplete = false)
      val ev = UnderTest.completedEvent("b", "k", "etag", "checksum", 10)

      val s1 = UnderTest.applyTransferEvent(s0, ev)(updateFrom(s0))
      s1.isInProgress shouldBe false
      s1.isComplete shouldBe true
    }

    "handle UploadFailed and UploadAborted" in {
      val s0 = State(Some("u1"), Some("b"), Some("k"), Map.empty, isInProgress = true, isComplete = false)

      val failed = UnderTest.applyTransferEvent(s0, UnderTest.failedEvent("no"))(updateFrom(s0))
      failed.isInProgress shouldBe false
      failed.isComplete shouldBe false

      val aborted = UnderTest.applyTransferEvent(s0, UnderTest.abortedEvent())(updateFrom(s0))
      aborted.isInProgress shouldBe false
      aborted.isComplete shouldBe false
    }
  }
}
