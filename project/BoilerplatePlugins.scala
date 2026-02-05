import sbt._
import sbt.Keys._

import scala.io.Source
import java.time.LocalDate
import java.time.format.DateTimeFormatter

/**
 * Local plugin implementations for boilerplate-jvm. For dependency isolation
 *  on this project only.
 * Based on magicroot-sbt (MagicRoot), with local modifications.
 * https://github.com/tomshley/magicroot-sbt
 */

// =============================================================================
// Version File Plugin - reads VERSION file for project version
// =============================================================================
object VersionFilePlugin extends AutoPlugin {
  override def trigger = noTrigger
  
  override def projectSettings: Seq[Setting[_]] = Seq(
    version := {
      val versionFile = baseDirectory.value / ".." / "VERSION"
      if (versionFile.exists()) {
        Source.fromFile(versionFile).getLines().mkString.trim.stripPrefix("v")
      } else {
        "0.0.1-SNAPSHOT"
      }
    }
  )
}

// =============================================================================
// Publish GitLab Plugin - publishes to GitLab Maven registry
// =============================================================================
object PublishGitLabPlugin extends AutoPlugin {
  override def trigger = noTrigger
  
  object autoImport {
    val publishGitLabProjectId = settingKey[Int]("GitLab project ID for publishing")
  }
  
  import autoImport._
  
  override def projectSettings: Seq[Setting[_]] = Seq(
    publishGitLabProjectId := 0,
    publishTo := {
      val projectId = publishGitLabProjectId.value
      if (projectId > 0) {
        Some("gitlab-maven" at s"https://gitlab.com/api/v4/projects/$projectId/packages/maven")
      } else {
        None
      }
    },
    credentials ++= {
      val projectCredentials = file(".secure_files") / ".credentials.gitlab"
      val homeCredentials = Path.userHome / ".sbt" / "gitlab-token"
      
      if (projectCredentials.exists()) {
        Seq(Credentials(projectCredentials))
      } else if (homeCredentials.exists()) {
        Seq(Credentials(
          "GitLab Packages Registry",
          "gitlab.com",
          "Private-Token",
          Source.fromFile(homeCredentials).getLines().mkString.trim
        ))
      } else {
        sys.env.get("GITLAB_TOKEN").map { token =>
          Credentials("GitLab Packages Registry", "gitlab.com", "Private-Token", token)
        }.toSeq
      }
    }
  )
}

// =============================================================================
// Value Add Project Plugin - common settings for library projects
// =============================================================================
object ValueAddProjectPlugin extends AutoPlugin {
  override def trigger = noTrigger
  
  override def projectSettings: Seq[Setting[_]] = Seq(
    scalaVersion := "3.4.2",
    scalacOptions ++= Seq(
      "-deprecation",
      "-feature",
      "-unchecked"
    ),
    javacOptions ++= Seq(
      "-source", "21",
      "-target", "21"
    ),
    Test / fork := true
  )
}

// =============================================================================
// Projects Helper Plugin - utility for defining publishable projects
// =============================================================================
object ProjectsHelperPlugin extends AutoPlugin {
  override def trigger = noTrigger
  
  object autoImport {
    def publishableProject(name: String): Project = {
      Project(name, file(name))
    }
  }
}
