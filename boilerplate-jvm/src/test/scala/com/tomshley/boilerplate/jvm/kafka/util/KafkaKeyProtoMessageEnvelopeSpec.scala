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

import com.google.protobuf.wrappers.StringValue
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

final class KafkaKeyProtoMessageEnvelopeSpec extends AnyWordSpec with Matchers {

  "KafkaKeyProtoMessageEnvelope" should {
    "store serviceName and key" in {
      val testMessage = StringValue("test-value")
      val envelope = KafkaKeyProtoMessageEnvelope("service", "key123", testMessage)
      envelope.serviceName shouldBe "service"
      envelope.key shouldBe "key123"
    }

    "generate messageBytes from pbValue" in {
      val testMessage = StringValue("test-value")
      val envelope = KafkaKeyProtoMessageEnvelope("service", "key123", testMessage)
      envelope.messageBytes should not be empty
    }
  }
}
