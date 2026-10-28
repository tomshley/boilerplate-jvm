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

package com.tomshley.boilerplate.jvm.reqreply.forms

import com.tomshley.boilerplate.jvm.reqreply.models.{ExpiringSuccessPathView, IdempotentView, RedirectPathView, ViewModel}

trait FormEnvelope[T <: FormFieldNameMap] extends ViewModel with IdempotentView with ExpiringSuccessPathView with RedirectPathView {
  val getRenderPath: String = "/"
  val successPathPrefix: String = "/"
  val postPathPrefix: String = "/"
  val formFieldModelMap: Option[T] = Option.empty
  val messages: List[String] = List.empty
  val errors: List[String] = List.empty
  val fieldErrors: Map[String, List[String]] = Map.empty

  override def successValue: Option[String] =
    Some(s"$successPathPrefix$requestIdHmac")

  override def redirectPath: Option[String] =
    Some(getRenderPath)
}

