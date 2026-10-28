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

package com.tomshley.boilerplate.jvm.cloudevents

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import java.util.Base64

final class CloudEventSpec extends AnyWordSpec with Matchers {

  "CloudEvent" should {
    "store required fields" in {
      val ce = CloudEvent(
        id = "1",
        source = "/source",
        specversion = "1.0",
        `type` = "com.example.event"
      )

      ce.id shouldBe "1"
      ce.source shouldBe "/source"
      ce.specversion shouldBe "1.0"
      ce.`type` shouldBe "com.example.event"
    }

    "store optional fields" in {
      val ce = CloudEvent(
        id = "1",
        source = "/source",
        specversion = "1.0",
        `type` = "com.example.event",
        datacontenttype = Some("application/json"),
        dataschema = Some("schema:v1"),
        subject = Some("subj"),
        time = Some("2018-04-05T17:31:00Z"),
        data = Some("{\"a\":1}"),
        extensions = Some(Map("x" -> "y"))
      )

      ce.datacontenttype shouldBe Some("application/json")
      ce.dataschema shouldBe Some("schema:v1")
      ce.subject shouldBe Some("subj")
      ce.time shouldBe Some("2018-04-05T17:31:00Z")
      ce.data shouldBe Some("{\"a\":1}")
      ce.extensions shouldBe Some(Map("x" -> "y"))
    }

    "dataBase64String encodes bytes correctly" in {
      val bytes = "hello".getBytes("UTF-8")
      val ce = CloudEvent(
        id = "1",
        source = "/source",
        specversion = "1.0",
        `type` = "com.example.event",
        data_base64 = Some(bytes)
      )

      ce.dataBase64String shouldBe Some(Base64.getEncoder.encodeToString(bytes))
    }
  }
}
