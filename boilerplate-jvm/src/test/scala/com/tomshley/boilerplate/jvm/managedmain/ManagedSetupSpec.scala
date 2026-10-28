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

package com.tomshley.boilerplate.jvm.managedmain

import ch.qos.logback.classic.Logger as LogbackLogger
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.AppenderBase
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.pekko.Done
import org.apache.pekko.actor.{BootstrapSetup, CoordinatedShutdown}
import org.apache.pekko.actor.setup.ActorSystemSetup
import org.apache.pekko.actor.testkit.typed.scaladsl.{ActorTestKit, LogCapturing, TestProbe}
import org.apache.pekko.actor.typed.ActorSystem
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{Millis, Seconds, Span}
import org.scalatest.wordspec.AnyWordSpec
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.{Future, Promise}
import scala.concurrent.duration.*

final class ManagedSetupSpec
    extends AnyWordSpec
    with Matchers
    with ScalaFutures
    with LogCapturing {

  override given patienceConfig: PatienceConfig =
    PatienceConfig(timeout = Span(15, Seconds), interval = Span(100, Millis))

  private val quietWindow = 300.millis

  private val localConfig =
    ConfigFactory.parseString(
      """
      pekko.actor.provider = local
      pekko.coordinated-shutdown.run-by-jvm-shutdown-hook = off
      """
    )

  private def systemConfig(extra: String = ""): Config =
    ConfigFactory
      .parseString(extra)
      .withFallback(localConfig)
      .withFallback(ConfigFactory.load())

  private def systemSetup(managed: ManagedSetup, extra: String = ""): ActorSystemSetup =
    ActorSystemSetup(BootstrapSetup(systemConfig(extra)), managed)

  private def withKit[A](name: String)(test: ActorTestKit => A): A = {
    val kit = ActorTestKit(name, localConfig)
    try test(kit)
    finally kit.shutdownTestKit()
  }

  private def withLogCapture[A](kit: ActorTestKit, loggerName: String, marker: String)(
      test: TestProbe[String] => A
  ): A = {
    val probe = kit.createTestProbe[String]()
    val logger = LoggerFactory.getLogger(loggerName).asInstanceOf[LogbackLogger]
    val appender = new AppenderBase[ILoggingEvent]() {
      override def append(event: ILoggingEvent): Unit = {
        val formatted = event.getFormattedMessage
        val thrown =
          Option(event.getThrowableProxy).map(_.getMessage).getOrElse("")
        if (formatted.contains(marker) || thrown.contains(marker))
          probe.ref ! s"$formatted$thrown"
      }
    }
    appender.setContext(logger.getLoggerContext)
    appender.start()
    logger.addAppender(appender)
    try test(probe)
    finally {
      logger.detachAppender(appender)
      appender.stop()
    }
  }

  private final class Fixture(kit: ActorTestKit) {
    val systems = kit.createTestProbe[ActorSystem[?]]()
    val acquired = kit.createTestProbe[String]()
    val released = kit.createTestProbe[String]()
    val bootstrapped = kit.createTestProbe[ActorSystem[?]]()
    val bodyRan = kit.createTestProbe[ActorSystem[?]]()

    def main(
        onBootstrap: (ActorSystem[?], Logger) => Unit =
          (system, _) => bootstrapped.ref ! system
    ): ManagedMain =
      new ManagedMain {
        override protected[managedmain] lazy val name: String = "RecordingMain"
        override protected[managedmain] def bootstrap(
            system: ActorSystem[?],
            logger: Logger
        ): Unit =
          onBootstrap(system, logger)
      }

    val body: ActorSystem[?] => Startup = system => {
      bodyRan.ref ! system
      Startup.Ready
    }

    def task[A](name: String, resource: => Future[A]): StartupTask[A] =
      StartupTask(
        name,
        system => {
          systems.ref ! system
          acquired.ref ! name
          resource
        },
        (system, _) => {
          released.ref ! name
          Future.successful(Done)
        }
      )

    def awaitTerminated(system: ActorSystem[?]): Unit =
      system.whenTerminated.futureValue

    def stop(system: ActorSystem[?]): Unit = {
      system.terminate()
      system.whenTerminated.futureValue
    }
  }

  "ManagedMain with ManagedSetup" should {
    "run bootstrap and body unchanged through the two-argument overload" in
      withKit("managed-default") { kit =>
        val fixture = new Fixture(kit)
        val main = fixture.main()
        main.apply("default-svc", fixture.body)
        val bootSystem = fixture.bootstrapped.receiveMessage()
        try {
          val bodySystem = fixture.bodyRan.receiveMessage()
          bodySystem should be theSameInstanceAs(bootSystem)
          fixture.acquired.expectNoMessage(quietWindow)
        } finally fixture.stop(bootSystem)
      }

    "construct and compose setups without running acquire or transforms" in
      withKit("managed-construct") { kit =>
        val fixture = new Fixture(kit)
        val transforms = kit.createTestProbe[String]()
        val task: StartupTask[Done] = fixture.task("idle", Future.successful(Done))
        val setup = ManagedSetup(
          startup = Vector(task),
          http = setup => {
            transforms.ref ! "http"
            setup
          },
          tcp = setup => {
            transforms.ref ! "tcp"
            setup
          }
        )
        val composed = setup.andThen(ManagedSetup.empty).andThen(setup)
        composed.startup.size shouldBe 2
        fixture.acquired.expectNoMessage(quietWindow)
        transforms.expectNoMessage(quietWindow)
        fixture.released.expectNoMessage(quietWindow)
      }

    "run startup tasks in declared order before bootstrap and body" in
      withKit("managed-order") { kit =>
        val fixture = new Fixture(kit)
        val gate = Promise[String]()
        val setup = ManagedSetup(startup =
          Vector(fixture.task("one", gate.future), fixture.task("two", Future.successful("r2")))
        )
        val managed =
          fixture.main().apply("ordered-svc", setup, fixture.body)
        try {
          fixture.acquired.expectMessage("one")
          fixture.acquired.expectNoMessage(quietWindow)
          fixture.bootstrapped.expectNoMessage(quietWindow)
          fixture.bodyRan.expectNoMessage(quietWindow)
          gate.success("r1")
          fixture.acquired.expectMessage("two")
          fixture.bootstrapped.expectMessageType[ActorSystem[?]]
          fixture.bodyRan.expectMessageType[ActorSystem[?]]
        } finally {
          gate.trySuccess("r1")
          fixture.stop(managed)
        }
        fixture.released.receiveMessages(2) should contain theSameElementsAs List("one", "two")
        fixture.released.expectNoMessage(quietWindow)
      }

    "release a resource whose acquire triggered shutdown mid-flight" in
      withKit("managed-inacquire") { kit =>
        val fixture = new Fixture(kit)
        val task: StartupTask[String] = StartupTask(
          "selfshutdown",
          system => {
            fixture.systems.ref ! system
            fixture.acquired.ref ! "selfshutdown"
            CoordinatedShutdown(system).run(CoordinatedShutdown.UnknownReason)
            Future.successful("r1")
          },
          (_, _) => {
            fixture.released.ref ! "selfshutdown"
            Future.successful(Done)
          }
        )
        val managed =
          fixture.main().apply("self-svc", ManagedSetup(startup = Vector(task)), fixture.body)
        try {
          fixture.acquired.expectMessage("selfshutdown")
          fixture.released.expectMessage("selfshutdown")
          fixture.bootstrapped.expectNoMessage(quietWindow)
          fixture.bodyRan.expectNoMessage(quietWindow)
          fixture.awaitTerminated(managed)
          fixture.released.expectNoMessage(quietWindow)
        } finally fixture.stop(managed)
      }

    "release acquired resources and skip remaining tasks when a later acquire fails" in
      withKit("managed-latefailure") { kit =>
        val fixture = new Fixture(kit)
        val sentinel = new RuntimeException("acquire two exploded")
        val setup = ManagedSetup(startup = Vector(
          fixture.task("one", Future.successful("r1")),
          fixture.task("two", Future.failed(sentinel)),
          fixture.task("three", Future.successful("r3"))
        ))
        val managed = fixture.main().apply("fail-svc", setup, fixture.body)
        try {
          fixture.acquired.receiveMessages(2) shouldBe List("one", "two")
          fixture.awaitTerminated(managed)
          fixture.bootstrapped.expectNoMessage(quietWindow)
          fixture.bodyRan.expectNoMessage(quietWindow)
          fixture.released.receiveMessages(1) shouldBe List("one")
          fixture.released.expectNoMessage(quietWindow)
          fixture.acquired.expectNoMessage(quietWindow)
        } finally fixture.stop(managed)
      }

    "treat a synchronous acquire throw like a failed future" in
      withKit("managed-syncfailure") { kit =>
        val fixture = new Fixture(kit)
        val sentinel = new RuntimeException("acquire two threw inline")
        val setup = ManagedSetup(startup = Vector(
          fixture.task("one", Future.successful("r1")),
          fixture.task[String]("two", throw sentinel),
          fixture.task("three", Future.successful("r3"))
        ))
        val managed = fixture.main().apply("throw-svc", setup, fixture.body)
        try {
          fixture.acquired.receiveMessages(2) shouldBe List("one", "two")
          fixture.awaitTerminated(managed)
          fixture.bootstrapped.expectNoMessage(quietWindow)
          fixture.bodyRan.expectNoMessage(quietWindow)
          fixture.released.receiveMessages(1) shouldBe List("one")
          fixture.released.expectNoMessage(quietWindow)
          fixture.acquired.expectNoMessage(quietWindow)
        } finally fixture.stop(managed)
      }

    "wait for a pending acquisition to release it during shutdown" in
      withKit("managed-pendingrelease") { kit =>
        val fixture = new Fixture(kit)
        val pending = Promise[String]()
        val holdBegun = kit.createTestProbe[Done]()
        val releaseHold = Promise[Done]()
        val first: StartupTask[String] = StartupTask(
          "one",
          system => {
            CoordinatedShutdown(system).addTask(
              CoordinatedShutdown.PhaseBeforeServiceUnbind,
              "hold-shutdown"
            )(() => {
              holdBegun.ref ! Done
              releaseHold.future
            })
            fixture.systems.ref ! system
            fixture.acquired.ref ! "one"
            pending.future
          },
          (_, _) => {
            fixture.released.ref ! "one"
            Future.successful(Done)
          }
        )
        val setup = ManagedSetup(startup =
          Vector(first, fixture.task("two", Future.successful("r2")))
        )
        val managed = fixture.main().apply(
          "pending-svc",
          systemSetup(setup, "tomshley-boilerplate-managedmain.startup-timeout = 1 s"),
          fixture.body
        )
        try {
          fixture.acquired.expectMessage("one")
          holdBegun.expectMessage(Done)
          pending.success("r1")
          releaseHold.success(Done)
          fixture.released.expectMessage("one")
          fixture.awaitTerminated(managed)
          fixture.acquired.expectNoMessage(quietWindow)
          fixture.released.expectNoMessage(quietWindow)
          fixture.bootstrapped.expectNoMessage(quietWindow)
          fixture.bodyRan.expectNoMessage(quietWindow)
        } finally {
          pending.trySuccess("r1")
          releaseHold.trySuccess(Done)
          fixture.stop(managed)
        }
      }

    "let sibling cleanup run when one release fails" in
      withKit("managed-releasefailure") { kit =>
        val fixture = new Fixture(kit)
        val sentinel = new RuntimeException("release one exploded")
        val failing: StartupTask[String] = StartupTask(
          "badrelease",
          system => {
            fixture.systems.ref ! system
            fixture.acquired.ref ! "badrelease"
            Future.successful("r1")
          },
          (_, _) => {
            fixture.released.ref ! "badrelease"
            Future.failed(sentinel)
          }
        )
        val setup = ManagedSetup(startup =
          Vector(failing, fixture.task("sibling", Future.successful("r2")))
        )
        withLogCapture(
          kit,
          org.slf4j.Logger.ROOT_LOGGER_NAME,
          "managed-badrelease"
        ) { logs =>
          val managed = fixture.main().apply("release-svc", setup, fixture.body)
          try {
            fixture.bootstrapped.expectMessageType[ActorSystem[?]]
            fixture.bodyRan.expectMessageType[ActorSystem[?]]
            managed.terminate()
            fixture.awaitTerminated(managed)
            fixture.released.receiveMessages(2) should contain theSameElementsAs
              List("badrelease", "sibling")
            fixture.released.expectNoMessage(quietWindow)
            logs.expectMessageType[String]
          } finally fixture.stop(managed)
        }
      }

    "acquire and release per system when the same setup is used twice" in
      withKit("managed-persystem") { kit =>
        val fixture = new Fixture(kit)
        val setup = ManagedSetup(startup =
          Vector(fixture.task("shared", Future.successful("r1")))
        )
        val first = fixture.main().apply("svc-a", setup, fixture.body)
        val second = fixture.main().apply("svc-b", setup, fixture.body)
        try {
          fixture.acquired.receiveMessages(2) shouldBe List("shared", "shared")
          fixture.bodyRan.receiveMessages(2)
          first.terminate()
          fixture.awaitTerminated(first)
          second.whenTerminated.isCompleted shouldBe false
          fixture.released.receiveMessages(1) shouldBe List("shared")
          fixture.released.expectNoMessage(quietWindow)
          second.terminate()
          fixture.awaitTerminated(second)
          fixture.released.receiveMessages(1) shouldBe List("shared")
          fixture.released.expectNoMessage(quietWindow)
        } finally {
          first.terminate()
          second.terminate()
          fixture.awaitTerminated(first)
          fixture.awaitTerminated(second)
        }
      }

    "honor a positive configured startup timeout override" in
      withKit("managed-timeout-ok") { kit =>
        val fixture = new Fixture(kit)
        val gate = Promise[String]()
        val setup = ManagedSetup(startup = Vector(fixture.task("one", gate.future)))
        val managed = fixture.main().apply(
          "timeout-svc",
          systemSetup(setup, "tomshley-boilerplate-managedmain.startup-timeout = 10 s"),
          fixture.body
        )
        try {
          fixture.acquired.expectMessage("one")
          gate.success("r1")
          fixture.bootstrapped.expectMessageType[ActorSystem[?]]
          fixture.bodyRan.expectMessageType[ActorSystem[?]]
        } finally {
          gate.trySuccess("r1")
          fixture.stop(managed)
        }
      }

    "reject a non-positive configured startup timeout before acquiring" in
      withKit("managed-timeout-bad") { kit =>
        val fixture = new Fixture(kit)
        val task: StartupTask[Done] = fixture.task("never", Future.successful(Done))
        withLogCapture(
          kit,
          "badtimeout-svc-RecordingMain",
          "startup timeout must be positive"
        ) { logs =>
          val managed = fixture.main().apply(
            "badtimeout-svc",
            systemSetup(
              ManagedSetup(startup = Vector(task)),
              "tomshley-boilerplate-managedmain.startup-timeout = 0 s"
            ),
            fixture.body
          )
          try {
            fixture.awaitTerminated(managed)
            fixture.acquired.expectNoMessage(quietWindow)
            fixture.bootstrapped.expectNoMessage(quietWindow)
            fixture.bodyRan.expectNoMessage(quietWindow)
            logs.expectMessageType[String]
          } finally fixture.stop(managed)
        }
      }

    "release acquired resources when bootstrap throws" in
      withKit("managed-bootthrow") { kit =>
        val fixture = new Fixture(kit)
        val sentinel = new RuntimeException("bootstrap exploded")
        val main = fixture.main((system, _) => {
          fixture.bootstrapped.ref ! system
          throw sentinel
        })
        val managed = main.apply(
          "bootthrow-svc",
          ManagedSetup(startup = Vector(fixture.task("one", Future.successful("r1")))),
          fixture.body
        )
        try {
          fixture.acquired.expectMessage("one")
          fixture.bootstrapped.expectMessageType[ActorSystem[?]]
          fixture.awaitTerminated(managed)
          fixture.bodyRan.expectNoMessage(quietWindow)
          fixture.released.receiveMessages(1) shouldBe List("one")
          fixture.released.expectNoMessage(quietWindow)
        } finally fixture.stop(managed)
      }

    "release acquired resources when the body throws" in
      withKit("managed-bodythrow") { kit =>
        val fixture = new Fixture(kit)
        val sentinel = new RuntimeException("body exploded")
        val managed = fixture.main().apply(
          "bodythrow-svc",
          ManagedSetup(startup = Vector(fixture.task("one", Future.successful("r1")))),
          system => {
            fixture.bodyRan.ref ! system
            throw sentinel
          }
        )
        try {
          fixture.acquired.expectMessage("one")
          fixture.bootstrapped.expectMessageType[ActorSystem[?]]
          fixture.bodyRan.expectMessageType[ActorSystem[?]]
          fixture.awaitTerminated(managed)
          fixture.released.receiveMessages(1) shouldBe List("one")
          fixture.released.expectNoMessage(quietWindow)
        } finally fixture.stop(managed)
      }
  }
}
