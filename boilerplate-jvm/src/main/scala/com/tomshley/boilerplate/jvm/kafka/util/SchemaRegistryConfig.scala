package com.tomshley.boilerplate.jvm.kafka.util

import com.typesafe.config.Config
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig

/** Confluent Schema Registry connection configuration.
 *
 * Encapsulates registry URL and optional API-key authentication
 * for Confluent Cloud. Use `fromConfig` to load from Typesafe Config,
 * or construct directly for programmatic configuration.
 */
final case class SchemaRegistryConfig(
    url: String,
    auth: Option[SchemaRegistryConfig.BasicAuth] = None
):
  def toConfluentConfig: Map[String, Any] =
    val base = Map[String, Any](
      AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG -> url
    )
    auth.fold(base) { a =>
      base ++ Map[String, Any](
        AbstractKafkaSchemaSerDeConfig.BASIC_AUTH_CREDENTIALS_SOURCE -> "USER_INFO",
        AbstractKafkaSchemaSerDeConfig.USER_INFO_CONFIG -> s"${a.apiKey}:${a.apiSecret}"
      )
    }

object SchemaRegistryConfig:
  final case class BasicAuth(apiKey: String, apiSecret: String)

  private def optionalString(config: Config, path: String): Option[String] =
    Option.when(config.hasPath(path))(config.getString(path)).map(_.trim).filter(_.nonEmpty)

  def fromConfig(config: Config): SchemaRegistryConfig =
    val sr = config.getConfig("schema-registry")
    val apiKey = optionalString(sr, "api-key")
    val apiSecret = optionalString(sr, "api-secret")

    val auth = (apiKey, apiSecret) match
      case (Some(key), Some(secret)) => Some(BasicAuth(key, secret))
      case (None, None)              => None
      case _ => throw new IllegalArgumentException(
        "schema-registry.api-key and schema-registry.api-secret must either both be set or both be empty"
      )

    SchemaRegistryConfig(url = sr.getString("url"), auth = auth)
