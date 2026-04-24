package com.tomshley.boilerplate.jvm.kafka.util

import com.typesafe.config.ConfigFactory
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

final class SchemaRegistryConfigSpec extends AnyWordSpec with Matchers {

  "SchemaRegistryConfig" should {
    "default autoRegisterSchemas to false and useLatestVersion to true" in {
      val cfg = SchemaRegistryConfig(url = "http://localhost:8081")
      cfg.autoRegisterSchemas shouldBe false
      cfg.useLatestVersion shouldBe true
    }
  }

  "SchemaRegistryConfig.toSerializerConfig" should {
    "emit the schemas-as-code serde flags by default" in {
      val cfg = SchemaRegistryConfig(url = "http://localhost:8081")
      val serde = cfg.toSerializerConfig
      serde(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG) shouldBe "http://localhost:8081"
      serde(AbstractKafkaSchemaSerDeConfig.AUTO_REGISTER_SCHEMAS) shouldBe "false"
      serde(AbstractKafkaSchemaSerDeConfig.USE_LATEST_VERSION) shouldBe "true"
      serde.contains(AbstractKafkaSchemaSerDeConfig.BASIC_AUTH_CREDENTIALS_SOURCE) shouldBe false
    }

    "propagate basic auth credentials when provided" in {
      val cfg = SchemaRegistryConfig(
        url = "https://psrc.example.com",
        auth = Some(SchemaRegistryConfig.BasicAuth("key", "secret"))
      )
      val serde = cfg.toSerializerConfig
      serde(AbstractKafkaSchemaSerDeConfig.BASIC_AUTH_CREDENTIALS_SOURCE) shouldBe "USER_INFO"
      serde(AbstractKafkaSchemaSerDeConfig.USER_INFO_CONFIG) shouldBe "key:secret"
    }

    "honour autoRegisterSchemas = true when explicitly opted in (local dev)" in {
      val cfg = SchemaRegistryConfig(
        url = "http://localhost:8081",
        autoRegisterSchemas = true,
        useLatestVersion = false
      )
      val serde = cfg.toSerializerConfig
      serde(AbstractKafkaSchemaSerDeConfig.AUTO_REGISTER_SCHEMAS) shouldBe "true"
      serde(AbstractKafkaSchemaSerDeConfig.USE_LATEST_VERSION) shouldBe "false"
    }

    "emit auth and override flags together without interference" in {
      val cfg = SchemaRegistryConfig(
        url = "https://psrc.example.com",
        auth = Some(SchemaRegistryConfig.BasicAuth("key", "secret")),
        autoRegisterSchemas = true,
        useLatestVersion = false
      )
      val serde = cfg.toSerializerConfig
      serde(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG) shouldBe "https://psrc.example.com"
      serde(AbstractKafkaSchemaSerDeConfig.BASIC_AUTH_CREDENTIALS_SOURCE) shouldBe "USER_INFO"
      serde(AbstractKafkaSchemaSerDeConfig.USER_INFO_CONFIG) shouldBe "key:secret"
      serde(AbstractKafkaSchemaSerDeConfig.AUTO_REGISTER_SCHEMAS) shouldBe "true"
      serde(AbstractKafkaSchemaSerDeConfig.USE_LATEST_VERSION) shouldBe "false"
    }
  }

  "SchemaRegistryConfig.toClientConfig" should {
    "emit URL only when no auth is configured" in {
      val cfg = SchemaRegistryConfig(url = "http://localhost:8081")
      val client = cfg.toClientConfig
      client(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG) shouldBe "http://localhost:8081"
      client.contains(AbstractKafkaSchemaSerDeConfig.BASIC_AUTH_CREDENTIALS_SOURCE) shouldBe false
    }

    "emit URL + auth and NEVER the serializer gate flags" in {
      val cfg = SchemaRegistryConfig(
        url = "https://psrc.example.com",
        auth = Some(SchemaRegistryConfig.BasicAuth("key", "secret")),
        autoRegisterSchemas = true,
        useLatestVersion = false
      )
      val client = cfg.toClientConfig
      client(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG) shouldBe "https://psrc.example.com"
      client(AbstractKafkaSchemaSerDeConfig.BASIC_AUTH_CREDENTIALS_SOURCE) shouldBe "USER_INFO"
      client(AbstractKafkaSchemaSerDeConfig.USER_INFO_CONFIG) shouldBe "key:secret"
      // Critical: gate flags MUST NOT leak into client/deserializer surfaces
      // even when the config carries non-default values for them.
      client.contains(AbstractKafkaSchemaSerDeConfig.AUTO_REGISTER_SCHEMAS) shouldBe false
      client.contains(AbstractKafkaSchemaSerDeConfig.USE_LATEST_VERSION) shouldBe false
    }
  }

  "SchemaRegistryConfig.toConfluentConfig (deprecated)" should {
    "remain a source-compatible alias for toSerializerConfig" in {
      val cfg = SchemaRegistryConfig(
        url = "http://localhost:8081",
        auth = Some(SchemaRegistryConfig.BasicAuth("k", "s")),
        autoRegisterSchemas = true,
        useLatestVersion = false
      )
      @annotation.nowarn("cat=deprecation")
      val legacy = cfg.toConfluentConfig
      legacy shouldBe cfg.toSerializerConfig
    }
  }

  "SchemaRegistryConfig.fromConfig" should {
    "apply schemas-as-code defaults when HOCON omits the flags" in {
      val config = ConfigFactory.parseString(
        """
          |schema-registry {
          |  url = "https://psrc.example.com"
          |}
          |""".stripMargin
      )
      val cfg = SchemaRegistryConfig.fromConfig(config)
      cfg.url shouldBe "https://psrc.example.com"
      cfg.auth shouldBe None
      cfg.autoRegisterSchemas shouldBe false
      cfg.useLatestVersion shouldBe true
    }

    "read auto-register-schemas and use-latest-version overrides from HOCON" in {
      val config = ConfigFactory.parseString(
        """
          |schema-registry {
          |  url = "http://localhost:8081"
          |  auto-register-schemas = true
          |  use-latest-version = false
          |}
          |""".stripMargin
      )
      val cfg = SchemaRegistryConfig.fromConfig(config)
      cfg.autoRegisterSchemas shouldBe true
      cfg.useLatestVersion shouldBe false
    }

    "require api-key and api-secret to be set together or omitted together" in {
      val config = ConfigFactory.parseString(
        """
          |schema-registry {
          |  url = "https://psrc.example.com"
          |  api-key = "only-key"
          |}
          |""".stripMargin
      )
      an[IllegalArgumentException] should be thrownBy SchemaRegistryConfig.fromConfig(config)
    }

    "parse basic auth when both api-key and api-secret are present" in {
      val config = ConfigFactory.parseString(
        """
          |schema-registry {
          |  url = "https://psrc.example.com"
          |  api-key = "KEY"
          |  api-secret = "SECRET"
          |}
          |""".stripMargin
      )
      val cfg = SchemaRegistryConfig.fromConfig(config)
      cfg.auth shouldBe Some(SchemaRegistryConfig.BasicAuth("KEY", "SECRET"))
    }
  }
}
