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
