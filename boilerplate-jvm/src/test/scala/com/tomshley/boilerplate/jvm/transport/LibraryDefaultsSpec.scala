package com.tomshley.boilerplate.jvm.transport

import com.typesafe.config.{ConfigFactory, ConfigResolveOptions}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import java.net.{URL, URLClassLoader}
import java.nio.file.{Files, Path}
import java.time.Duration

final class LibraryDefaultsSpec extends AnyWordSpec with Matchers {

  private def withProductionLoader[A](test: ClassLoader => A): A = {
    val location =
      GrpcServerBoilerplate.getClass.getProtectionDomain.getCodeSource.getLocation
    location.getProtocol shouldBe "file"
    val classesDir = Path.of(location.toURI)
    Files.isDirectory(classesDir) shouldBe true
    val resourceRoot =
      if (location.getPath.endsWith("/")) location
      else new URL(location.toString + "/")
    val loader = new URLClassLoader(
      Array(resourceRoot),
      ClassLoader.getPlatformClassLoader
    )
    try test(loader)
    finally loader.close()
  }

  private def referenceConfig(loader: ClassLoader) =
    ConfigFactory
      .parseResources(loader, "reference.conf")
      .resolve(
        ConfigResolveOptions
          .defaults()
          .setUseSystemEnvironment(false)
          .setAllowUnresolved(true)
      )

  "Library packaging" should {
    "ship reference.conf and no application.conf" in withProductionLoader {
      loader =>
        Option(loader.getResource("reference.conf")) shouldBe defined
        Option(loader.getResource("application.conf")) shouldBe None
    }

    "resolve library defaults from reference.conf" in withProductionLoader {
      loader =>
        val config = referenceConfig(loader)
        config.getDuration(
          "tomshley-boilerplate-reqreply-idempotency.ask-timeout"
        ) shouldBe Duration.ofSeconds(5)
        config.getDuration(
          "tomshley-boilerplate-reqreply-signing.grace"
        ) shouldBe Duration.ofSeconds(0)
        config.getDuration(
          "tomshley-boilerplate-transport.http.shutdown-timeout"
        ) shouldBe Duration.ofSeconds(3)
        config.getDuration(
          "tomshley-boilerplate-managedmain.startup-timeout"
        ) shouldBe Duration.ofSeconds(30)
    }

    "keep application defaults from shadowing resolved reference values" in withProductionLoader {
      loader =>
        val config = ConfigFactory
          .defaultApplication(loader)
          .withFallback(
            ConfigFactory.parseString(
              """tomshley-boilerplate-reqreply-idempotency.ask-timeout = 9 s
                |tomshley-boilerplate-reqreply-signing.grace = 11 s
                |""".stripMargin
            )
          )
          .resolve()
        config.getDuration(
          "tomshley-boilerplate-reqreply-idempotency.ask-timeout"
        ) shouldBe Duration.ofSeconds(9)
        config.getDuration(
          "tomshley-boilerplate-reqreply-signing.grace"
        ) shouldBe Duration.ofSeconds(11)
    }

    "allow overriding the transport shutdown deadline" in withProductionLoader {
      loader =>
        val config = ConfigFactory
          .parseString(
            "tomshley-boilerplate-transport.http.shutdown-timeout = 9 s"
          )
          .withFallback(
            ConfigFactory.parseResources(loader, "reference.conf")
          )
          .resolve(
            ConfigResolveOptions
              .defaults()
              .setUseSystemEnvironment(false)
              .setAllowUnresolved(true)
          )
        config.getDuration(
          "tomshley-boilerplate-transport.http.shutdown-timeout"
        ) shouldBe Duration.ofSeconds(9)
    }

    "allow overriding the managed startup timeout" in withProductionLoader {
      loader =>
        val config = ConfigFactory
          .parseString(
            "tomshley-boilerplate-managedmain.startup-timeout = 7 s"
          )
          .withFallback(
            ConfigFactory.parseResources(loader, "reference.conf")
          )
          .resolve(
            ConfigResolveOptions
              .defaults()
              .setUseSystemEnvironment(false)
              .setAllowUnresolved(true)
          )
        config.getDuration(
          "tomshley-boilerplate-managedmain.startup-timeout"
        ) shouldBe Duration.ofSeconds(7)
    }
  }
}
