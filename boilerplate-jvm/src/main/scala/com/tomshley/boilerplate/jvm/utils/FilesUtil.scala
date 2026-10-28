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

import java.io.File

trait FilesUtil {
  private final val extensionSeparator = "."

  def recursiveFileList(fileFilter: File => Boolean = f => true)(f: File): Seq[File] = {
    lazy val files = f.list()
    lazy val filesData = {
      files match
        case null => Seq()
        case _ => files.toSeq
    }.map(new File(f, _))

    filesData.filter(fileFilter) ++ filesData.filter(_.isDirectory).flatMap(recursiveFileList(fileFilter))
  }

  def nameWithoutExtension(pathAsString: String): String = {
    if (pathAsString.indexOf(extensionSeparator) > 0) pathAsString.substring(0, pathAsString.lastIndexOf(extensionSeparator)) else pathAsString
  }

  def nameAndExtensionPair(pathAsString: String): (String, String) = {
    if (pathAsString.indexOf(extensionSeparator) > 0) {
      val extensionIndex = pathAsString.lastIndexOf(extensionSeparator)
      (pathAsString.substring(0, extensionIndex), pathAsString.substring(extensionIndex, pathAsString.length))
    } else {
      (pathAsString, "")
    }
  }
}
