package com.tomshley.boilerplate.jvm.cloudevents

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

final class CloudEventKafkaAvroSerializerSpec extends AnyWordSpec with Matchers {

  "CloudEventKafkaAvroSerializer.init" should {
    "return a serializer that produces empty bytes for null data" in {
      val ser = CloudEventKafkaAvroSerializer.init("mock://schema-registry")
      val bytes = ser.serialize("topic", null)
      bytes shouldBe empty
    }
  }
}
