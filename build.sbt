import sbt.file

lazy val boilerplateProjectName = "boilerplate-jvm"
lazy val boilerplateProjectOrgName = "com.tomshley.boilerplate"

// Versions aligned with magicroot-sbt PekkoProjectSettings
val PekkoVersion = "1.1.0-M1"
val PekkoManagementVersion = "1.0.0"
val PekkoKafkaConnector = "1.1.0-M1"
val PekkoHttpVersion = "1.1.0"

lazy val boilerplateProject = publishableProject(boilerplateProjectName)
  .enablePlugins(ValueAddProjectPlugin, VersionFilePlugin, PublishGitLabPlugin)
  .settings(
    organization := boilerplateProjectOrgName,
    publishGitLabProjectId := 70100980,
    resolvers ++= Seq(
      Resolver.ApacheMavenSnapshotsRepo,
      "Confluent Maven Repository" at "https://packages.confluent.io/maven/"
    ),
    libraryDependencies ++= Seq(
      // Core/Java
      "com.twilio.sdk" % "twilio" % "10.6.3",
      "org.slf4j" % "slf4j-api" % "2.0.5",
      "org.slf4j" % "slf4j-simple" % "2.0.5",
      "org.json4s" %% "json4s-jackson" % "4.0.7",
      "com.typesafe" % "config" % "1.4.2",
      "com.github.nscala-time" %% "nscala-time" % "2.32.0",
      "org.apache.commons" % "commons-lang3" % "3.12.0",
      "commons-io" % "commons-io" % "2.15.1",
      "org.apache.commons" % "commons-digester3" % "3.2",
      "org.jetbrains" % "annotations" % "24.0.1",
      "joda-time" % "joda-time" % "2.12.5",
      "com.google.guava" % "guava" % "23.0",
      "com.fasterxml.jackson.core" % "jackson-databind" % "2.15.2",
      // Pekko Actor
      "org.apache.pekko" %% "pekko-stream" % PekkoVersion,
      "org.apache.pekko" %% "pekko-actor-typed" % PekkoVersion,
      "org.apache.pekko" %% "pekko-management" % PekkoManagementVersion,
      "org.apache.pekko" %% "pekko-management-cluster-http" % PekkoManagementVersion,
      "org.apache.pekko" %% "pekko-discovery-kubernetes-api" % PekkoManagementVersion,
      "org.apache.pekko" %% "pekko-discovery" % PekkoVersion,
      "org.apache.pekko" %% "pekko-management-cluster-bootstrap" % PekkoManagementVersion,
      "org.apache.pekko" %% "pekko-http" % PekkoHttpVersion,
      "org.apache.pekko" %% "pekko-http-spray-json" % PekkoHttpVersion,
      "org.apache.pekko" %% "pekko-serialization-jackson" % PekkoVersion,
      "org.apache.pekko" %% "pekko-slf4j" % PekkoVersion,
      "org.apache.pekko" %% "pekko-cluster-sharding-typed" % PekkoVersion,
      // Pekko Persistence
      "org.apache.pekko" %% "pekko-persistence-r2dbc" % PekkoManagementVersion,
      "org.apache.pekko" %% "pekko-persistence-typed" % PekkoVersion,
      // Pekko Projection
      "org.apache.pekko" %% "pekko-projection-core" % PekkoManagementVersion,
      "org.apache.pekko" %% "pekko-projection-r2dbc" % PekkoManagementVersion,
      "org.apache.pekko" %% "pekko-projection-eventsourced" % PekkoManagementVersion,
      "org.apache.pekko" %% "pekko-persistence-query" % PekkoVersion,
      // Pekko Kafka
      "com.sksamuel.avro4s" %% "avro4s-core" % "5.0.13",
      "io.confluent" % "kafka-streams-avro-serde" % "5.2.1",
      "io.confluent" % "kafka-avro-serializer" % "6.2.0",
      // Protobuf/ScalaPB
      "com.thesamet.scalapb" %% "scalapb-runtime" % "0.11.15",
      "com.google.protobuf" % "protobuf-java" % "3.25.1",
      "org.apache.pekko" %% "pekko-connectors-kafka" % PekkoKafkaConnector,
      "org.apache.pekko" %% "pekko-connectors-kafka-cluster-sharding" % PekkoKafkaConnector,
      // Logging
      "ch.qos.logback" % "logback-classic" % "1.5.6",
      // Test
      "org.scalatest" %% "scalatest" % "3.2.19" % Test,
      "org.apache.pekko" %% "pekko-actor-testkit-typed" % PekkoVersion % Test,
      "org.apache.pekko" %% "pekko-stream-testkit" % PekkoVersion % Test,
      "org.apache.pekko" %% "pekko-persistence-testkit" % PekkoVersion % Test
    )
  )

lazy val boilerplateJvm = (project in file("."))
  .enablePlugins(
    ProjectsHelperPlugin
  )
  .aggregate(boilerplateProject)
  .settings(publish / skip := true)
