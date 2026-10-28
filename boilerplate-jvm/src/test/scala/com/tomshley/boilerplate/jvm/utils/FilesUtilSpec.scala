/*
 * copyright 2017–present tomshley llc
 *
 * licensed under the apache license, version 2.0 (the "license");
 * you may not use this file except in compliance with the license.
 * you may obtain a copy of the license at
 *
 * http://www.apache.org/licenses/license-2.0
 *
 * unless required by applicable law or agreed to in writing, software
 * distributed under the license is distributed on an "as is" basis,
 * without warranties or conditions of any kind, either express or implied.
 * see the license for the specific language governing permissions and
 * limitations under the license.
 *
 * @author thomas schena @sgoggles <https://github.com/sgoggles> | <https://gitlab.com/sgoggles>
 *
 */

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
