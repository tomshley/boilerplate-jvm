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
    licenses := {
      val tagOrBranch =
        if (version.value.endsWith("SNAPSHOT")) "main"
        else "v" + version.value
      Seq(
        ("Apache-2.0", url("https://raw.githubusercontent.com/tomshley/boilerplate-jvm/" + tagOrBranch + "/LICENSE"))
      )
    },
    magicRootPublishGitLabProjectId := 70100980,
    // Credential file lives in .secure_files/ (not project root)
    credentials += Credentials(file(".secure_files/.credentials.gitlab")),
    libraryDependencies += "ch.qos.logback" % "logback-classic" % "1.6.3" % Test,
    Test / fork := true,
    Test / javaOptions += "-Dslf4j.provider=ch.qos.logback.classic.spi.LogbackServiceProvider"
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
