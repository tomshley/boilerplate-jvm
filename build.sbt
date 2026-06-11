lazy val boilerplateProjectName = "boilerplate-jvm"
lazy val boilerplateProjectOrgName = "com.tomshley.boilerplate"

ThisBuild / versionScheme := Some("semver-spec")

lazy val boilerplateRoot = (project in file("."))
  .settings(
    publish / skip := true,
    publishArtifact := false
  )
  .aggregate(`boilerplate-jvm`)

lazy val `boilerplate-jvm` = (Project("boilerplate-jvm", file("boilerplate-jvm")))
  .enablePlugins(
    LibProjectPekkoFullPlugin,
    LibProjectPekkoMessagingPlugin,
    LibProjectPekkoStoragePlugin,
    LibProjectProtobufPlugin,
    // BouncyCastle lightweight API (bcprov) for utils.RestorableDigestUtil —
    // the JDK MessageDigest cannot export/restore midstate. Version is
    // centralized in magicroot's cryptoLibraries.
    LibProjectCryptoPlugin,
    VersionFilePlugin,
    TomshleyCIBuildVersionPlugin,
    PublishGitLabPlugin,
    GitLabSourceDependencyPlugin
  )
  .settings(
    name := boilerplateProjectName,
    organization := boilerplateProjectOrgName,
    magicRootPublishGitLabProjectId := 70100980,
    // Credential file lives in .secure_files/ (not project root)
    credentials += Credentials(file(".secure_files/.credentials.gitlab"))
    // TODO(upgrade): LibProjectPekkoFullPlugin pulls magicroot's pekkoKafkaLibraries,
    // which bring the Confluent Platform 7.6.0 Avro serdes (kafka-avro-serializer,
    // kafka-streams-avro-serde). Those depend on kafka-clients:7.6.0-ccs (Apache
    // Kafka 3.6.x), so every downstream Kafka Streams app inherits a kafka-clients
    // that is OLDER than the 3.8.0 Streams engine and must re-pin it (handled in
    // magicroot's kafkaStreamsProject). When the Confluent serde line is bumped to
    // a Platform release whose kafka-clients matches the engine (CP 7.8 = Apache
    // Kafka 3.8) — or this module excludes the transitive kafka-clients from the
    // Confluent serdes — that downstream override can be dropped.
  )
