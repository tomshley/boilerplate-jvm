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

package com.tomshley.boilerplate.jvm.kafka.util

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.duration.*

final class SchemaPublicationSpec extends AnyWordSpec with Matchers {

  "SchemaPublication.TopicSchemaSettings" should {
    "use default values when options are empty" in {
      val settings = SchemaPublication.TopicSchemaSettings(
        topicSchemas = Map.empty,
        schemaRegistry = None,
        identityMapCapacity = None,
        retriesNum = None,
        retriesInterval = None
      )
      settings.schemaRegistry shouldBe SchemaRegistryConfig("http://localhost:8081")
      settings.identityMapCapacity shouldBe 200
      settings.retriesNum shouldBe 5
      settings.retriesInterval shouldBe 500.milliseconds
    }

    "use provided values when options are defined" in {
      val settings = SchemaPublication.TopicSchemaSettings(
        topicSchemas = Map.empty,
        schemaRegistry = Some(SchemaRegistryConfig("http://custom:8081")),
        identityMapCapacity = Some(100),
        retriesNum = Some(3),
        retriesInterval = Some(1.second)
      )
      settings.schemaRegistry shouldBe SchemaRegistryConfig("http://custom:8081")
      settings.identityMapCapacity shouldBe 100
      settings.retriesNum shouldBe 3
      settings.retriesInterval shouldBe 1.second
    }
  }
}
