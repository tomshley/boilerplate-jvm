import sbt.file

lazy val boilerplateProjectName = "boilerplate-jvm"
lazy val boilerplateProjectOrgName = "com.tomshley.boilerplate"

lazy val boilerplateProject = publishableProject(boilerplateProjectName)
  .enablePlugins(ValueAddProjectPlugin, VersionFilePlugin, PublishGitLabPlugin)
  .settings(
    organization := boilerplateProjectOrgName,
    publishGitLabProjectId := 70100980,
    libraryDependencies += "com.twilio.sdk" % "twilio" % "10.6.3"
  )

lazy val boilerplateJvm = (project in file("."))
  .enablePlugins(
    ProjectsHelperPlugin
  )
  .aggregate(boilerplateProject)
  .settings(publish / skip := true)
