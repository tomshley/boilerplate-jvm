package com.tomshley.boilerplate.jvm.kafka.util

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.duration.*

final class SchemaPublicationSpec extends AnyWordSpec with Matchers {

  "SchemaPublication.TopicSchemaSettings" should {
    "use default values when options are empty" in {
      val settings = SchemaPublication.TopicSchemaSettings(
        topicSchema = Map.empty,
        registryURL = None,
        identityMapCapacity = None,
        retriesNum = None,
        retriesInterval = None
      )
      settings.registryURL shouldBe "http://localhost:8081"
      settings.identityMapCapacity shouldBe 200
      settings.retriesNum shouldBe 5
      settings.retriesInterval shouldBe 500.milliseconds
    }

    "use provided values when options are defined" in {
      val settings = SchemaPublication.TopicSchemaSettings(
        topicSchema = Map.empty,
        registryURL = Some("http://custom:8081"),
        identityMapCapacity = Some(100),
        retriesNum = Some(3),
        retriesInterval = Some(1.second)
      )
      settings.registryURL shouldBe "http://custom:8081"
      settings.identityMapCapacity shouldBe 100
      settings.retriesNum shouldBe 3
      settings.retriesInterval shouldBe 1.second
    }
  }
}
