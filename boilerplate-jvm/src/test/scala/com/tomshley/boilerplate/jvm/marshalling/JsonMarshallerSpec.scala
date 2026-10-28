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

package com.tomshley.boilerplate.jvm.marshalling

import com.tomshley.boilerplate.jvm.marshalling.models.MarshallModel
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.ExecutionContext

final class JsonMarshallerSpec extends AnyWordSpec with Matchers with ScalaFutures {

  given ExecutionContext = ExecutionContext.global

  final case class ExampleModel(a: String, b: Int) extends MarshallModel[ExampleModel]

  "JsonMarshaller" should {
    "serializeWithDefaults and deserializeWithDefaults round-trip" in {
      val m = ExampleModel("x", 1)
      val json = JsonMarshaller.serializeWithDefaults(m)
      val back = JsonMarshaller.deserializeWithDefaults[ExampleModel](json)
      back.a shouldBe m.a
      back.b shouldBe m.b
    }

    "serializeWithDefaultsAsync and deserializeWithDefaultsAsync round-trip" in {
      val m = ExampleModel("x", 1)
      val jsonF = JsonMarshaller.serializeWithDefaultsAsync(m, summon[ExecutionContext])
      val json = jsonF.futureValue
      val backF = JsonMarshaller.deserializeWithDefaultsAsync[ExampleModel](json, summon[ExecutionContext])
      val back = backF.futureValue
      back.a shouldBe m.a
      back.b shouldBe m.b
    }

    "serializeCustomMap and deserializeCustomMap round-trip" in {
      val input = Map[String, Any]("a" -> "x", "b" -> 1)
      val json = JsonMarshaller.serializeCustomMap(input)
      val output = JsonMarshaller.deserializeCustomMap(json)

      output("a") shouldBe "x"
      output("b") shouldBe 1
    }
  }
}
