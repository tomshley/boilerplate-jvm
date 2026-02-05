package com.tomshley.boilerplate.jvm.filetransfer

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

final class ChunkedTransferStateSpec extends AnyWordSpec with Matchers {

  private final case class State(
      uploadId: Option[String],
      bucket: Option[String],
      key: Option[String],
      uploadedParts: Map[Int, PartInfo],
      isInProgress: Boolean,
      isComplete: Boolean
  ) extends ChunkedTransferState

  "ChunkedTransferState" should {
    "compute isFailed" in {
      State(Some("u"), Some("b"), Some("k"), Map.empty, isInProgress = false, isComplete = false).isFailed shouldBe true
      State(None, None, None, Map.empty, isInProgress = false, isComplete = false).isFailed shouldBe false
    }

    "compute canResume" in {
      State(Some("u"), Some("b"), Some("k"), Map.empty, isInProgress = true, isComplete = false).canResume shouldBe true
      State(Some("u"), Some("b"), Some("k"), Map.empty, isInProgress = false, isComplete = true).canResume shouldBe false
      State(Some("u"), Some("b"), Some("k"), Map.empty, isInProgress = false, isComplete = false).canResume shouldBe false
    }

    "sum totalBytesUploaded" in {
      val s = State(Some("u"), Some("b"), Some("k"), Map(1 -> PartInfo("e1", 2), 2 -> PartInfo("e2", 3)), isInProgress = true, isComplete = false)
      s.totalBytesUploaded shouldBe 5L
    }

    "compute nextPartNo" in {
      State(Some("u"), Some("b"), Some("k"), Map.empty, isInProgress = true, isComplete = false).nextPartNo shouldBe 1
      State(Some("u"), Some("b"), Some("k"), Map(1 -> PartInfo("e1", 2), 3 -> PartInfo("e3", 2)), isInProgress = true, isComplete = false).nextPartNo shouldBe 4
    }
  }
}
