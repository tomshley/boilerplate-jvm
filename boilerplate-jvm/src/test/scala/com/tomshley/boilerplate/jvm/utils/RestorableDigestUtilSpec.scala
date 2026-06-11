package com.tomshley.boilerplate.jvm.utils

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import java.security.MessageDigest
import java.util.HexFormat

final class RestorableDigestUtilSpec extends AnyWordSpec with Matchers {

  private object UnderTest extends RestorableDigestUtil

  private def oneShotSha256Hex(bytes: Array[Byte]): String =
    HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes))

  "RestorableDigestUtil.sha256FoldHex / sha256DigestHex" should {

    "produce the one-shot SHA-256 across any chunking of the same bytes" in {
      val payload = Array.tabulate[Byte](4096)(i => (i * 31).toByte)
      val expected = oneShotSha256Hex(payload)

      Seq(Seq(4096), Seq(1, 4095), Seq(1024, 1024, 1024, 1024), Seq(7, 89, 4000)).foreach { sizes =>
        val chunks = sizes.foldLeft((List.empty[Array[Byte]], 0)) { case ((acc, offset), size) =>
          (acc :+ payload.slice(offset, offset + size), offset + size)
        }._1
        val finalState = chunks.foldLeft(Option.empty[String]) { (state, chunk) =>
          Some(UnderTest.sha256FoldHex(state, chunk))
        }
        UnderTest.sha256DigestHex(finalState) shouldBe expected
      }
    }

    "resume from a persisted midstate exactly as if the fold never stopped" in {
      // Simulates a process restart: the midstate hex is the ONLY thing
      // carried across; the digest object itself is never shared.
      val first = "the first half ".getBytes("UTF-8")
      val second = "and the second half".getBytes("UTF-8")
      val expected = oneShotSha256Hex(first ++ second)

      val checkpoint: String = UnderTest.sha256FoldHex(None, first)
      val restored: String = UnderTest.sha256FoldHex(Some(checkpoint), second)
      UnderTest.sha256DigestHex(Some(restored)) shouldBe expected
    }

    "yield the empty-payload SHA-256 from the fresh state" in {
      UnderTest.sha256DigestHex(None) shouldBe oneShotSha256Hex(Array.emptyByteArray)
    }

    "be deterministic for the same state and bytes" in {
      val bytes = "hello".getBytes("UTF-8")
      UnderTest.sha256FoldHex(None, bytes) shouldBe UnderTest.sha256FoldHex(None, bytes)
    }

    "produce a 64-char lowercase hex digest" in {
      val digest = UnderTest.sha256DigestHex(Some(UnderTest.sha256FoldHex(None, "hello".getBytes("UTF-8"))))
      digest.length shouldBe 64
      digest.matches("[0-9a-f]{64}") shouldBe true
      digest shouldBe "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"
    }

    "reject a non-hex midstate loudly" in {
      an[IllegalArgumentException] should be thrownBy
        UnderTest.sha256FoldHex(Some("not-hex!"), "x".getBytes("UTF-8"))
    }
  }
}
