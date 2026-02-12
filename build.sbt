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
    credentials += Credentials(file(".secure_files/.credentials.gitlab")),
    // Dependencies NOT covered by magicroot's composable plugins:
    // Override ancient commons-io from magicroot's javaProject
    dependencyOverrides += "commons-io" % "commons-io" % "2.15.1",
    libraryDependencies ++= Seq(
      "com.twilio.sdk" % "twilio" % "10.6.3",
      "com.thesamet.scalapb" %% "scalapb-runtime" % "0.11.15",
      "com.google.protobuf" % "protobuf-java" % "3.25.1",
      "org.apache.pekko" %% "pekko-projection-durable-state" % "1.0.0",
      "org.apache.pekko" %% "pekko-http-testkit" % "1.1.0" % Test
    )
  )
