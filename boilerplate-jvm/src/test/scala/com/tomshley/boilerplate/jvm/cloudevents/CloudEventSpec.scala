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
