resolvers +=
  "gitlab-maven" at "https://gitlab.com/api/v4/projects/70100400/packages/maven"

addSbtPlugin(
  "com.tomshley.magicroot" % "magicroot-sbt-projectsettings" % "2024.12.30.1"
)