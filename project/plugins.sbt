resolvers +=
  "gitlab-maven" at "https://gitlab.com/api/v4/projects/70100400/packages/maven"

addSbtPlugin(
  "com.tomshley.magicroot" % "magicroot-sbt-projectsettings" % "1.3.22"
)

addSbtPlugin("org.scoverage" % "sbt-scoverage" % "2.0.11")
