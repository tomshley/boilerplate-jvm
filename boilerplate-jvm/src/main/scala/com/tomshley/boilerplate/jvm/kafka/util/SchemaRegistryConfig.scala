package com.tomshley.boilerplate.jvm.kafka.util

import com.typesafe.config.Config
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig

/** Confluent Schema Registry connection configuration.
 *
 * Encapsulates registry URL, optional API-key authentication for Confluent
 * Cloud, and opinionated serializer policy for schemas-as-code pipelines.
 *
 * Defaults (`autoRegisterSchemas = false`, `useLatestVersion = true`) reflect
 * the intended pattern: schemas live in a schema repository and are registered
 * by a runbook / CI job; producers encode against whatever SR currently
 * advertises as the latest version of the subject. This prevents silent
 * schema drift caused by avro4s deriving a schema that differs from the
 * hand-registered `.avsc` (e.g., missing `doc` strings, invalid enum
 * defaults such as `""` emitted by avro4s 5.0.15). Auto-register is
 * intentionally off by default so that the runtime cannot mint new versions
 * behind operators' backs.
 *
 * For local/dev workflows without a pre-registered schema, set
 * `auto-register-schemas = true` in HOCON or pass `autoRegisterSchemas =
 * true` programmatically.
 *
 * '''Runtime compatibility check.''' With `useLatestVersion = true` and
 * Confluent's default `latest.compatibility.strict = true`, the serializer
 * runs a backward-compatibility check between the writer's schema and the
 * registered latest on first serialize of each subject. To prevent that
 * check from firing in production, downstream producers are expected to
 * maintain a `SchemaParitySpec`-style contract test that asserts the
 * avro4s-derived schema is structurally identical to the hand-registered
 * `.avsc` (see `ami-platform-ingress-server` and
 * `ami-platform-structuring-server` for references). This moves the
 * drift-detection from runtime failure to build-time failure.
 *
 * Use `fromConfig` to load from Typesafe Config, or construct directly for
 * programmatic configuration.
 */
final case class SchemaRegistryConfig(
    url: String,
    auth: Option[SchemaRegistryConfig.BasicAuth] = None,
    autoRegisterSchemas: Boolean = SchemaRegistryConfig.DefaultAutoRegisterSchemas,
    useLatestVersion: Boolean = SchemaRegistryConfig.DefaultUseLatestVersion
):
  def toConfluentConfig: Map[String, Any] =
    val base = Map[String, Any](
      AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG -> url,
      AbstractKafkaSchemaSerDeConfig.AUTO_REGISTER_SCHEMAS -> autoRegisterSchemas.toString,
      AbstractKafkaSchemaSerDeConfig.USE_LATEST_VERSION -> useLatestVersion.toString
    )
    auth.fold(base) { a =>
      base ++ Map[String, Any](
        AbstractKafkaSchemaSerDeConfig.BASIC_AUTH_CREDENTIALS_SOURCE -> "USER_INFO",
        AbstractKafkaSchemaSerDeConfig.USER_INFO_CONFIG -> s"${a.apiKey}:${a.apiSecret}"
      )
    }

object SchemaRegistryConfig:
  final case class BasicAuth(apiKey: String, apiSecret: String)

  val DefaultAutoRegisterSchemas: Boolean = false
  val DefaultUseLatestVersion: Boolean = true

  private def optionalString(config: Config, path: String): Option[String] =
    Option.when(config.hasPath(path))(config.getString(path)).map(_.trim).filter(_.nonEmpty)

  private def optionalBoolean(config: Config, path: String): Option[Boolean] =
    Option.when(config.hasPath(path))(config.getBoolean(path))

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

    SchemaRegistryConfig(
      url = sr.getString("url"),
      auth = auth,
      autoRegisterSchemas = optionalBoolean(sr, "auto-register-schemas").getOrElse(DefaultAutoRegisterSchemas),
      useLatestVersion = optionalBoolean(sr, "use-latest-version").getOrElse(DefaultUseLatestVersion)
    )
