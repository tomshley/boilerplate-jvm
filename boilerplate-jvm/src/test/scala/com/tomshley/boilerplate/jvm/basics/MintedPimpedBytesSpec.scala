package com.tomshley.boilerplate.jvm.basics

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.cbor.CBORFactory
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class MintedPimpedBytesSpec extends AnyWordSpec with Matchers {

  "MintedPimpedBytes equality" should {
    "return true for identical content" in {
      val bytes1 = MintedPimpedBytes(Array[Byte](1, 2, 3))
      val bytes2 = MintedPimpedBytes(Array[Byte](1, 2, 3))
      bytes1 shouldEqual bytes2
    }

    "return false for different content" in {
      val bytes1 = MintedPimpedBytes(Array[Byte](1, 2, 3))
      val bytes2 = MintedPimpedBytes(Array[Byte](1, 2, 4))
      bytes1 should not equal bytes2
    }

    "return false when compared to null" in {
      val bytes = MintedPimpedBytes(Array[Byte](1, 2, 3))
      bytes should not equal null
    }

    "return false when compared to other type" in {
      val bytes = MintedPimpedBytes(Array[Byte](1, 2, 3))
      bytes should not equal "not bytes"
      bytes should not equal Array[Byte](1, 2, 3)
    }
  }

  "MintedPimpedBytes hashCode" should {
    "be identical for identical content" in {
      val bytes1 = MintedPimpedBytes(Array[Byte](1, 2, 3))
      val bytes2 = MintedPimpedBytes(Array[Byte](1, 2, 3))
      bytes1.hashCode shouldEqual bytes2.hashCode
    }

    "be different for different content (probabilistic)" in {
      val bytes1 = MintedPimpedBytes(Array[Byte](1, 2, 3))
      val bytes2 = MintedPimpedBytes(Array[Byte](4, 5, 6))
      bytes1.hashCode should not equal bytes2.hashCode
    }
  }

  "MintedPimpedBytes immutability" should {
    "not be affected by mutations to source array after apply" in {
      val source = Array[Byte](1, 2, 3)
      val bytes = MintedPimpedBytes(source)
      source(0) = 99
      bytes.underlying shouldEqual Array[Byte](1, 2, 3)
    }

    "not be affected by mutations to result of underlying" in {
      val bytes = MintedPimpedBytes(Array[Byte](1, 2, 3))
      val result = bytes.underlying
      result(0) = 99
      bytes.underlying shouldEqual Array[Byte](1, 2, 3)
    }
  }

  "MintedPimpedBytes toHex and fromHex" should {
    "round-trip correctly" in {
      val original = MintedPimpedBytes(Array[Byte](10, 27, -1))
      val hex = original.toHex
      val decoded = MintedPimpedBytes.fromHex(hex)
      decoded shouldEqual original
    }

    "encode known value with zero-padding and lowercase" in {
      val bytes = MintedPimpedBytes(Array[Byte](0x0a, 0x1b, 0xff.toByte))
      bytes.toHex shouldEqual "0a1bff"
    }

    "decode known hex string correctly" in {
      val bytes = MintedPimpedBytes.fromHex("0a1bff")
      bytes.underlying shouldEqual Array[Byte](0x0a, 0x1b, 0xff.toByte)
    }

    "fail on odd-length hex string" in {
      assertThrows[IllegalArgumentException] {
        MintedPimpedBytes.fromHex("abc")
      }
    }

    "fail on null hex string" in {
      assertThrows[IllegalArgumentException] {
        MintedPimpedBytes.fromHex(null)
      }
    }

    "fail on invalid hex characters" in {
      assertThrows[NumberFormatException] {
        MintedPimpedBytes.fromHex("zzzz")
      }
    }
  }

  "MintedPimpedBytes.empty" should {
    "have isEmpty return true" in {
      MintedPimpedBytes.empty.isEmpty shouldBe true
    }

    "have length return 0" in {
      MintedPimpedBytes.empty.length shouldEqual 0
    }

    "be equal to factory-created empty instance" in {
      MintedPimpedBytes(Array.emptyByteArray) shouldEqual MintedPimpedBytes.empty
    }

    "be the same singleton instance" in {
      MintedPimpedBytes(Array.emptyByteArray) should be theSameInstanceAs MintedPimpedBytes.empty
    }
  }

  "MintedPimpedBytes length and isEmpty" should {
    "report correct length for non-empty bytes" in {
      val bytes = MintedPimpedBytes(Array[Byte](1, 2, 3))
      bytes.length shouldEqual 3
      bytes.isEmpty shouldBe false
      bytes.nonEmpty shouldBe true
    }

    "report correct length for empty bytes" in {
      val bytes = MintedPimpedBytes(Array.emptyByteArray)
      bytes.length shouldEqual 0
      bytes.isEmpty shouldBe true
      bytes.nonEmpty shouldBe false
    }
  }

  "MintedPimpedBytes toString" should {
    "format as MintedPimpedBytes(hex)" in {
      val bytes = MintedPimpedBytes(Array[Byte](0x0a, 0x1b, 0xff.toByte))
      bytes.toString shouldEqual "MintedPimpedBytes(0a1bff)"
    }

    "format empty as MintedPimpedBytes()" in {
      MintedPimpedBytes.empty.toString shouldEqual "MintedPimpedBytes()"
    }
  }

  "Array[Byte] extension toMintedPimpedBytes" should {
    "be equivalent to factory apply" in {
      val array = Array[Byte](1, 2, 3)
      array.toMintedPimpedBytes shouldEqual MintedPimpedBytes(Array[Byte](1, 2, 3))
    }

    "create defensive copy" in {
      val array = Array[Byte](1, 2, 3)
      val bytes = array.toMintedPimpedBytes
      array(0) = 99
      bytes.underlying shouldEqual Array[Byte](1, 2, 3)
    }
  }

  "MintedPimpedBytes Jackson CBOR serialization" should {
    val mapper = new ObjectMapper(new CBORFactory())
    mapper.registerModule(com.fasterxml.jackson.module.scala.DefaultScalaModule)

    "round-trip correctly" in {
      val original = MintedPimpedBytes(Array[Byte](1, 2, 3, 4, 5))
      val cbor = mapper.writeValueAsBytes(original)
      val deserialized = mapper.readValue(cbor, classOf[MintedPimpedBytes])
      deserialized shouldEqual original
    }

    "round-trip empty bytes" in {
      val original = MintedPimpedBytes.empty
      val cbor = mapper.writeValueAsBytes(original)
      val deserialized = mapper.readValue(cbor, classOf[MintedPimpedBytes])
      deserialized shouldEqual original
    }

    "work correctly in case class equality" in {
      case class Wrapper(id: String, data: MintedPimpedBytes)

      val wrapper1 = Wrapper("test", MintedPimpedBytes(Array[Byte](10, 20, 30)))
      val wrapper2 = Wrapper("test", MintedPimpedBytes(Array[Byte](10, 20, 30)))
      val wrapper3 = Wrapper("test", MintedPimpedBytes(Array[Byte](10, 20, 31)))

      wrapper1 shouldEqual wrapper2
      wrapper1 should not equal wrapper3
    }

    "serialize identically to raw Array[Byte]" in {
      val bytes = Array[Byte](1, 2, 3, 4, 5)
      val mintedBytes = MintedPimpedBytes(bytes.clone())

      val rawCbor = mapper.writeValueAsBytes(bytes)
      val mintedCbor = mapper.writeValueAsBytes(mintedBytes)

      rawCbor shouldEqual mintedCbor
    }
  }
}
