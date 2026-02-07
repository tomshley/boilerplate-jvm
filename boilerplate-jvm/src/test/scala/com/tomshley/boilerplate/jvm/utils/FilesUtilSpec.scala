package com.tomshley.boilerplate.jvm.utils

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import java.io.File
import java.nio.file.Files

final class FilesUtilSpec extends AnyWordSpec with Matchers {

  private object UnderTest extends FilesUtil

  "FilesUtil" should {
    "nameWithoutExtension strip extension" in {
      UnderTest.nameWithoutExtension("file.txt") shouldBe "file"
    }

    "nameWithoutExtension handle no extension" in {
      UnderTest.nameWithoutExtension("file") shouldBe "file"
    }

    "nameAndExtensionPair split correctly" in {
      UnderTest.nameAndExtensionPair("file.txt") shouldBe ("file", ".txt")
    }

    "recursiveFileList list nested files" in {
      val dir = Files.createTempDirectory("filesutil").toFile
      val nestedDir = new File(dir, "nested")
      nestedDir.mkdir()

      val f1 = new File(dir, "a.txt")
      val f2 = new File(nestedDir, "b.txt")
      Files.writeString(f1.toPath, "a")
      Files.writeString(f2.toPath, "b")

      val files = UnderTest.recursiveFileList() (dir)
      files.filter(_.isFile).map(_.getName).toSet shouldBe Set("a.txt", "b.txt")
    }

    "recursiveFileList apply filter" in {
      val dir = Files.createTempDirectory("filesutil-filter").toFile
      val f1 = new File(dir, "a.txt")
      val f2 = new File(dir, "b.bin")
      Files.writeString(f1.toPath, "a")
      Files.writeString(f2.toPath, "b")

      val onlyTxt = UnderTest.recursiveFileList(_.getName.endsWith(".txt"))(dir)
      onlyTxt.map(_.getName) shouldBe Seq("a.txt")
    }
  }
}
