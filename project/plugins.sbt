resolvers +=
  "gitlab-maven" at "https://gitlab.com/api/v4/projects/70100400/packages/maven"

addSbtPlugin(
  "com.tomshley.magicroot" % "magicroot-sbt-projectsettings" % "2.1.0"
)

addSbtPlugin("org.scoverage" % "sbt-scoverage" % "2.0.11")
