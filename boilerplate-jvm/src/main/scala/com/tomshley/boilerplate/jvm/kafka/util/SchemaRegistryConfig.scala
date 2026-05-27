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
 * `.avsc`. This moves the drift-detection from runtime failure to
 * build-time failure.
 *
 * '''Config surfaces.''' Two projections of this config are emitted:
 *   - [[toSerializerConfig]] — full set including the
 *     `auto.register.schemas` / `use.latest.version` gate flags. Consumed
 *     by `KafkaAvroSerializer.configure`.
 *   - [[toClientConfig]] — URL + auth only. Consumed by
 *     `KafkaAvroDeserializer.configure` and
 *     `CachedSchemaRegistryClient`, neither of which honour the
 *     serializer-side gate flags; emitting those keys to those call sites
 *     would be noise at best and forward-incompatible at worst.
 *
 * Use [[SchemaRegistryConfig.fromConfig]] to load from Typesafe Config, or
 * construct directly for programmatic configuration.
 */
final case class SchemaRegistryConfig(
    url: String,
    auth: Option[SchemaRegistryConfig.BasicAuth] = None,
    autoRegisterSchemas: Boolean = SchemaRegistryConfig.DefaultAutoRegisterSchemas,
    useLatestVersion: Boolean = SchemaRegistryConfig.DefaultUseLatestVersion
):

  /** URL + auth only — safe input for every Confluent SR surface (client,
   *  deserializer, serializer). Use this anywhere the serializer-only
   *  `auto.register.schemas` / `use.latest.version` gate flags are either
   *  ignored (deserializer, client) or undesired (e.g., registry publication
   *  code paths that register unconditionally). */
  def toClientConfig: Map[String, Any] =
    val base = Map[String, Any](
      AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG -> url
    )
    auth.fold(base) { a =>
      base ++ Map[String, Any](
        AbstractKafkaSchemaSerDeConfig.BASIC_AUTH_CREDENTIALS_SOURCE -> "USER_INFO",
        AbstractKafkaSchemaSerDeConfig.USER_INFO_CONFIG -> s"${a.apiKey}:${a.apiSecret}"
      )
    }

  /** Full serializer config — URL + auth + `auto.register.schemas` +
   *  `use.latest.version`. Consumed by `KafkaAvroSerializer.configure`.
   *  Downstream callers should prefer this over [[toClientConfig]] only
   *  when the call site is the serializer itself. */
  def toSerializerConfig: Map[String, Any] =
    toClientConfig ++ Map[String, Any](
      AbstractKafkaSchemaSerDeConfig.AUTO_REGISTER_SCHEMAS -> autoRegisterSchemas.toString,
      AbstractKafkaSchemaSerDeConfig.USE_LATEST_VERSION -> useLatestVersion.toString
    )

  /** @deprecated since 2.1.0 — alias for [[toSerializerConfig]]. Kept for
   *  source compatibility with 2.0.x call sites; will be removed in 3.0.0.
   *  New code should choose [[toSerializerConfig]] or [[toClientConfig]]
   *  explicitly based on which Confluent surface is being configured. */
  @deprecated("use toSerializerConfig (serializer) or toClientConfig (deserializer/SR-client) instead", "2.1.0")
  def toConfluentConfig: Map[String, Any] = toSerializerConfig

object SchemaRegistryConfig:
  final case class BasicAuth(apiKey: String, apiSecret: String)

  /** Schemas-as-code default: do NOT let the producer mint new registry
   *  versions at runtime. Flip to `true` only when the registry is
   *  deliberately unseeded (local dev, test harnesses).
   *
   *  When both `autoRegisterSchemas = true` AND `useLatestVersion = true`
   *  are set, Confluent's `AbstractKafkaAvroSerializer.serializeImpl`
   *  resolves the ambiguity in favour of the auto-register path (verified
   *  against `kafka-avro-serializer` 7.5.x bytecode — the
   *  `if (autoRegisterSchema) … else if (useLatestVersion) …` branch is
   *  linear). Callers opting into auto-registration should therefore also
   *  set `useLatestVersion = false` to make intent explicit at the call
   *  site. */
  val DefaultAutoRegisterSchemas: Boolean = false

  /** Schemas-as-code default: encode every record against whatever
   *  version the registry currently advertises as latest for the subject,
   *  rather than the avro4s-derived schema. This is what neutralizes
   *  case-class-vs-`.avsc` drift at runtime. See
   *  [[DefaultAutoRegisterSchemas]] for precedence when both flags are
   *  `true`. */
  val DefaultUseLatestVersion: Boolean = true

  private def optionalString(config: Config, path: String): Option[String] =
    Option.when(config.hasPath(path))(config.getString(path)).map(_.trim).filter(_.nonEmpty)

  private def optionalBoolean(config: Config, path: String): Option[Boolean] =
    Option.when(config.hasPath(path))(config.getBoolean(path))

  /** Read configuration from a Typesafe Config rooted at `schema-registry`.
   *
   *  '''HOCON key convention.''' Keys use Lightbend/Pekko dashed style
   *  (`auto-register-schemas`, `use-latest-version`, `api-key`,
   *  `api-secret`, `url`) — NOT the Confluent dot-delimited client
   *  property names (`auto.register.schemas`). HOCON interprets
   *  `a.b.c = x` as the nested object `{ a { b { c = x } } }`, so pasting
   *  Confluent's native key names under `schema-registry { … }` will
   *  silently create nested objects instead of leaf overrides — the
   *  override is then ignored and the default applies. The conversion to
   *  Confluent's dotted property names happens inside [[toClientConfig]]
   *  and [[toSerializerConfig]]. */
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
