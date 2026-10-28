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

package com.tomshley.boilerplate.jvm.kafka.util

import com.tomshley.boilerplate.jvm.kafka.util.KafkaTestSupport.{ProducerEvent, RecordingProducer, TestProducerFactory}
import com.typesafe.config.ConfigFactory
import org.apache.kafka.clients.producer.{MockProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.pekko.Done
import org.apache.pekko.actor.CoordinatedShutdown
import org.apache.pekko.actor.testkit.typed.scaladsl.{ActorTestKit, LogCapturing, ScalaTestWithActorTestKit, TestProbe}
import org.apache.pekko.actor.typed.ActorSystem
import org.apache.pekko.kafka.ProducerSettings
import org.scalatest.wordspec.AnyWordSpecLike

import scala.concurrent.{Future, Promise}
import scala.concurrent.duration.*
import scala.util.{Failure, Success, Try}

/** `CreateProducer` construction contract: `init` shares one managed
  * `SendProducer` per `ActorSystem` built by a dedicated owner actor that
  * closes it with coordinated shutdown, never blocks the caller, retries a
  * construction that failed instead of reusing it, and `sendProducer`
  * gives callers an explicitly owned build with the same enrichment and
  * dispatcher semantics.
  *
  * `MockProducer` is injected through `ProducerSettings.withProducerFactory`,
  * which `createKafkaProducerAsync` honours, so no broker is required.
  */
final class CreateProducerSpec
    extends ScalaTestWithActorTestKit(ConfigFactory.parseString(
      """
        |producer-enrichment-dispatcher {
        |  executor = "default-executor"
        |}
        |producer-construction-dispatcher {
        |  executor = "default-executor"
        |}
        |""".stripMargin))
    with AnyWordSpecLike
    with LogCapturing {

  private val assertionTimeout = 15.seconds
  private val quietWindow = 300.millis
  private val record = new ProducerRecord[String, String]("topic", "key", "value")

  private def observe[A](future: Future[A]): TestProbe[Try[A]] = {
    val probe = createTestProbe[Try[A]]()
    future.onComplete(probe.ref ! _)(using system.executionContext)
    probe
  }

  private def resultOf[A](future: Future[A]): A =
    observe(future).receiveMessage(assertionTimeout) match {
      case Success(value) => value
      case Failure(cause) => fail("expected a completed Future", cause)
    }

  private def failedWith[A](future: Future[A]): Throwable =
    observe(future).receiveMessage(assertionTimeout) match {
      case Failure(cause) => cause
      case Success(value) => fail(s"expected a failed Future, got $value")
    }

  private def fixture(settingsFor: ActorSystem[?] => ProducerSettings[String, String]): CreateProducer[String, String] =
    new TestProducerFactory(settingsFor)

  private def baseSettings(
      system: ActorSystem[?],
      factory: ProducerSettings[String, String] => org.apache.kafka.clients.producer.Producer[String, String]
  ): ProducerSettings[String, String] =
    KafkaTestSupport.producerSettings(system, factory)

  private def mock(): MockProducer[String, String] =
    new MockProducer[String, String](true, new StringSerializer(), new StringSerializer())

  private type EnrichmentGate = (ProducerSettings[String, String], Promise[ProducerSettings[String, String]])

  private def gatedEnrichment(attempts: TestProbe[EnrichmentGate])(
      current: ProducerSettings[String, String]
  ): Future[ProducerSettings[String, String]] = {
    val gate = Promise[ProducerSettings[String, String]]()
    attempts.ref ! (current -> gate)
    gate.future
  }

  "CreateProducer.init" should {

    "share one managed producer across calls and build it exactly once" in {
      val builds = createTestProbe[Done]()
      val producer = fixture(sys => baseSettings(sys, _ => { builds.ref ! Done; mock() }))

      val first = resultOf(producer.init(system))
      resultOf(producer.init(system)) shouldBe theSameInstanceAs(first)
      builds.expectMessage(Done)
      builds.expectNoMessage(quietWindow)
    }

    "surface the same construction failure and let the next init rebuild" in {
      val builds = createTestProbe[Done]()
      val events = createTestProbe[ProducerEvent]()
      val cause = new IllegalStateException("unreachable")
      val succeeded = Promise[Unit]()
      val producer = fixture(sys => baseSettings(sys, _ => {
        builds.ref ! Done
        if (succeeded.isCompleted) new RecordingProducer(events.ref)
        else throw cause
      }))

      failedWith(producer.init(system)) shouldBe theSameInstanceAs(cause)
      builds.expectMessage(Done)

      succeeded.success(())
      val rebuilt = resultOf(producer.init(system))
      resultOf(rebuilt.send(record))
      resultOf(producer.init(system)) shouldBe theSameInstanceAs(rebuilt)
      builds.expectMessage(Done)
      builds.expectNoMessage(quietWindow)
    }

    "retry settings and enrichment after a failed async enrichment" in {
      val settingsCalls = createTestProbe[Done]()
      val attempts = createTestProbe[EnrichmentGate]()
      val builds = createTestProbe[Done]()
      val producer = fixture(sys => {
        settingsCalls.ref ! Done
        baseSettings(sys, _ => { builds.ref ! Done; mock() })
          .withEnrichAsync(gatedEnrichment(attempts))
      })

      val first = producer.init(system)
      val (_, firstGate) = attempts.receiveMessage(assertionTimeout)
      val cause = new IllegalStateException("enrichment down")
      firstGate.failure(cause)
      failedWith(first) shouldBe theSameInstanceAs(cause)
      builds.expectNoMessage(quietWindow)

      val second = producer.init(system)
      val (current, secondGate) = attempts.receiveMessage(assertionTimeout)
      secondGate.success(current)
      val rebuilt = resultOf(second)
      resultOf(rebuilt.send(record))
      builds.expectMessage(Done)

      resultOf(producer.init(system)) shouldBe theSameInstanceAs(rebuilt)
      settingsCalls.expectMessage(Done)
      settingsCalls.expectMessage(Done)
      attempts.expectNoMessage(quietWindow)
      builds.expectNoMessage(quietWindow)
    }

    "return a synchronous enrichment throw through the Future and retry" in {
      val settingsCalls = createTestProbe[Done]()
      val builds = createTestProbe[Done]()
      val cause = new IllegalStateException("enrich threw")
      val allowSuccess = Promise[Unit]()
      val producer = fixture(sys => {
        settingsCalls.ref ! Done
        baseSettings(sys, _ => { builds.ref ! Done; mock() })
          .withEnrichAsync { current =>
            if (allowSuccess.isCompleted) Future.successful(current) else throw cause
          }
      })

      failedWith(producer.init(system)) shouldBe theSameInstanceAs(cause)
      builds.expectNoMessage(quietWindow)

      allowSuccess.success(())
      val rebuilt = resultOf(producer.init(system))
      resultOf(rebuilt.send(record))
      builds.expectMessage(Done)
      settingsCalls.expectMessage(Done)
      settingsCalls.expectMessage(Done)
    }

    "share a single in-flight construction across concurrent callers" in {
      val attempts = createTestProbe[EnrichmentGate]()
      val builds = createTestProbe[Done]()
      val producer = fixture(sys => baseSettings(sys, _ => { builds.ref ! Done; mock() })
        .withEnrichAsync(gatedEnrichment(attempts)))

      val inits = Vector.fill(8)(producer.init(system))
      val (current, gate) = attempts.receiveMessage(assertionTimeout)
      inits.foreach(future => future.isCompleted shouldBe false)
      gate.success(current)

      val resolved = inits.map(resultOf(_))
      resolved.foreach(_ shouldBe theSameInstanceAs(resolved.head))
      builds.expectMessage(Done)
      builds.expectNoMessage(quietWindow)
      attempts.expectNoMessage(quietWindow)
    }

    "return settings failures through the Future rather than throwing during the call" in {
      val cause = new IllegalStateException("settings blew up")
      val producer = fixture(_ => throw cause)

      failedWith(producer.init(system)) shouldBe theSameInstanceAs(cause)
      failedWith(producer.sendProducer(system)) shouldBe theSameInstanceAs(cause)
    }

    "run the configured enrichment exactly once, including across send and close" in {
      val enriches = createTestProbe[Done]()
      val raws = createTestProbe[RecordingProducer]()
      val events = createTestProbe[ProducerEvent]()
      val producer = fixture(sys => baseSettings(sys, _ => {
        val raw = new RecordingProducer(events.ref)
        raws.ref ! raw
        raw
      }).withEnrichAsync { current =>
        enriches.ref ! Done
        Future.successful(current)
      })

      val built = resultOf(producer.sendProducer(system))
      val raw = raws.receiveMessage(assertionTimeout)
      try {
        resultOf(built.send(record))
        resultOf(built.close())
        enriches.expectMessage(Done)
        enriches.expectNoMessage(quietWindow)
      } finally if (!raw.closed()) resultOf(built.close())
    }

    "keep the enriched close settings and use the enrichment's replacement factory" in {
      val builds = createTestProbe[Done]()
      val raws = createTestProbe[RecordingProducer]()
      val events = createTestProbe[ProducerEvent]()
      val producer = fixture(sys => baseSettings(sys, _ =>
          throw new IllegalStateException("original factory must not be invoked"))
        .withEnrichAsync { current =>
          Future.successful(current
            .withProducerFactory { _ =>
              builds.ref ! Done
              val raw = new RecordingProducer(events.ref)
              raws.ref ! raw
              raw
            }
            .withCloseTimeout(7.seconds)
            .withCloseProducerOnStop(true)
            .withDispatcher("producer-construction-dispatcher"))
        })

      val built = resultOf(producer.sendProducer(system))
      val raw = raws.receiveMessage(assertionTimeout)
      try {
        built.settings.closeTimeout shouldBe 7.seconds
        built.settings.closeProducerOnStop shouldBe true
        built.settings.dispatcher shouldBe "producer-construction-dispatcher"
        resultOf(built.send(record))
        builds.expectMessage(Done)
        builds.expectNoMessage(quietWindow)

        resultOf(built.close())
        events.expectMessage(ProducerEvent.Flushed(raw))
        events.expectMessage(ProducerEvent.Closed(raw))
      } finally if (!raw.closed()) resultOf(built.close())
    }

    "run settings, enrichment, and construction on their configured dispatchers" in {
      val settingsThreads = createTestProbe[String]()
      val enrichmentThreads = createTestProbe[String]()
      val constructionThreads = createTestProbe[String]()
      val producer = fixture(sys => {
        settingsThreads.ref ! Thread.currentThread().getName
        baseSettings(sys, _ => {
          constructionThreads.ref ! Thread.currentThread().getName
          mock()
        })
          .withDispatcher("producer-enrichment-dispatcher")
          .withEnrichAsync { current =>
            enrichmentThreads.ref ! Thread.currentThread().getName
            Future.successful(current.withDispatcher("producer-construction-dispatcher"))
          }
      })

      val built = resultOf(producer.sendProducer(system))
      try {
        settingsThreads.receiveMessage() should include("default-blocking-io-dispatcher")
        enrichmentThreads.receiveMessage() should include("producer-enrichment-dispatcher")
        constructionThreads.receiveMessage() should include("producer-construction-dispatcher")
      } finally resultOf(built.close())
    }

    "fail the Future without invoking the factory when the configured dispatcher does not exist" in {
      val builds = createTestProbe[Done]()
      val producer = fixture(sys => baseSettings(sys, _ => { builds.ref ! Done; mock() })
        .withDispatcher("missing-dispatcher"))

      val cause = failedWith(producer.sendProducer(system))
      cause.getMessage should include("missing-dispatcher")
      builds.expectNoMessage(quietWindow)
    }

    "scope the managed producer to its ActorSystem and close only that system's producer" in {
      val kitB = ActorTestKit("CreateProducerSpecPeer")
      try {
        val raws = createTestProbe[RecordingProducer]()
        val events = createTestProbe[ProducerEvent]()
        val producer = fixture(sys => baseSettings(sys, _ => {
          val raw = new RecordingProducer(events.ref)
          raws.ref ! raw
          raw
        }))

        val producerA = resultOf(producer.init(system))
        val producerB = resultOf(producer.init(kitB.system))
        producerA should not be theSameInstanceAs(producerB)
        val rawA = raws.receiveMessage(assertionTimeout)
        val rawB = raws.receiveMessage(assertionTimeout)

        kitB.system.terminate()
        resultOf(kitB.system.whenTerminated)
        events.expectMessage(ProducerEvent.Flushed(rawB))
        events.expectMessage(ProducerEvent.Closed(rawB))
        events.expectNoMessage(quietWindow)
        resultOf(producerA.send(record))
        rawA.closed() shouldBe false
      } finally kitB.shutdownTestKit()
    }

    "keep managed producers separate per factory instance within one ActorSystem" in {
      val buildsA = createTestProbe[Done]()
      val buildsB = createTestProbe[Done]()
      val producerA = fixture(sys => baseSettings(sys, _ => { buildsA.ref ! Done; mock() }))
      val producerB = fixture(sys => baseSettings(sys, _ => { buildsB.ref ! Done; mock() }))

      val first = resultOf(producerA.init(system))
      val second = resultOf(producerB.init(system))
      first should not be theSameInstanceAs(second)
      buildsA.expectMessage(Done)
      buildsB.expectMessage(Done)
    }

    "flush and close the managed producer exactly once when its ActorSystem terminates" in {
      val kit = ActorTestKit("CreateProducerSpecShutdown")
      try {
        val raws = createTestProbe[RecordingProducer]()
        val events = createTestProbe[ProducerEvent]()
        val producer = fixture(sys => baseSettings(sys, _ => {
          val raw = new RecordingProducer(events.ref)
          raws.ref ! raw
          raw
        }))

        resultOf(producer.init(kit.system))
        val raw = raws.receiveMessage(assertionTimeout)
        kit.system.terminate()
        resultOf(kit.system.whenTerminated)
        events.expectMessage(ProducerEvent.Flushed(raw))
        events.expectMessage(ProducerEvent.Closed(raw))
        events.expectNoMessage(quietWindow)
      } finally kit.shutdownTestKit()
    }

    "wait for a pending construction during shutdown and close the completed producer" in {
      val kit = ActorTestKit("CreateProducerSpecPendingClose")
      try {
        val attempts = createTestProbe[EnrichmentGate]()
        val raws = createTestProbe[RecordingProducer]()
        val events = createTestProbe[ProducerEvent]()
        val closing = createTestProbe[Done]()
        val producer = fixture(sys => baseSettings(sys, _ => {
          val raw = new RecordingProducer(events.ref)
          raws.ref ! raw
          raw
        }).withEnrichAsync(gatedEnrichment(attempts)))

        val initiated = producer.init(kit.system)
        val (current, gate) = attempts.receiveMessage(assertionTimeout)
        CoordinatedShutdown(kit.system).addTask(
          CoordinatedShutdown.PhaseBeforeActorSystemTerminate,
          "signal-producer-shutdown"
        )(() => { closing.ref ! Done; Future.successful(Done) })
        kit.system.terminate()
        closing.expectMessage(Done)
        val terminated = observe(kit.system.whenTerminated)
        terminated.expectNoMessage(quietWindow)

        gate.success(current)
        val raw = raws.receiveMessage(assertionTimeout)
        events.expectMessage(ProducerEvent.Flushed(raw))
        events.expectMessage(ProducerEvent.Closed(raw))
        terminated.receiveMessage(assertionTimeout) shouldBe Success(Done)
        resultOf(initiated)
      } finally kit.shutdownTestKit()
    }

    "finish shutdown without closing when a pending construction fails" in {
      val kit = ActorTestKit("CreateProducerSpecPendingFailure")
      try {
        val attempts = createTestProbe[EnrichmentGate]()
        val raws = createTestProbe[RecordingProducer]()
        val events = createTestProbe[ProducerEvent]()
        val closing = createTestProbe[Done]()
        val cause = new IllegalStateException("broker gone")
        val producer = fixture(sys => baseSettings(sys, _ => {
          val raw = new RecordingProducer(events.ref)
          raws.ref ! raw
          raw
        }).withEnrichAsync(gatedEnrichment(attempts)))

        val initiated = producer.init(kit.system)
        val (_, gate) = attempts.receiveMessage(assertionTimeout)
        CoordinatedShutdown(kit.system).addTask(
          CoordinatedShutdown.PhaseBeforeActorSystemTerminate,
          "signal-producer-shutdown"
        )(() => { closing.ref ! Done; Future.successful(Done) })
        kit.system.terminate()
        closing.expectMessage(Done)

        gate.failure(cause)
        failedWith(initiated) shouldBe theSameInstanceAs(cause)
        raws.expectNoMessage(quietWindow)
        events.expectNoMessage(quietWindow)
        resultOf(kit.system.whenTerminated)
      } finally kit.shutdownTestKit()
    }

    "reject the first init after coordinated shutdown has begun without invoking the factory" in {
      val kit = ActorTestKit("CreateProducerSpecLateInit")
      try {
        val begun = createTestProbe[Done]()
        val releaseShutdown = Promise[Done]()
        val builds = createTestProbe[Done]()
        val producer = fixture(sys => baseSettings(sys, _ => { builds.ref ! Done; mock() }))

        CoordinatedShutdown(kit.system).addTask(
          CoordinatedShutdown.PhaseBeforeServiceUnbind,
          "hold-shutdown"
        )(() => { begun.ref ! Done; releaseShutdown.future })
        kit.system.terminate()
        begun.expectMessage(Done)

        try {
          failedWith(producer.init(kit.system)).getMessage shouldBe "ActorSystem is shutting down"
          builds.expectNoMessage(quietWindow)
        } finally releaseShutdown.trySuccess(Done)
        resultOf(kit.system.whenTerminated)
      } finally kit.shutdownTestKit()
    }

    "reject a subsequent init during shutdown rather than returning the retained producer" in {
      val kit = ActorTestKit("CreateProducerSpecLateRetry")
      try {
        val begun = createTestProbe[Done]()
        val releaseShutdown = Promise[Done]()
        val builds = createTestProbe[Done]()
        val events = createTestProbe[ProducerEvent]()
        val producer = fixture(sys => baseSettings(sys, _ => {
          builds.ref ! Done
          new RecordingProducer(events.ref)
        }))

        resultOf(producer.init(kit.system))
        builds.expectMessage(Done)

        CoordinatedShutdown(kit.system).addTask(
          CoordinatedShutdown.PhaseBeforeServiceUnbind,
          "hold-shutdown"
        )(() => { begun.ref ! Done; releaseShutdown.future })
        kit.system.terminate()
        begun.expectMessage(Done)

        try {
          failedWith(producer.init(kit.system)).getMessage shouldBe "ActorSystem is shutting down"
          builds.expectNoMessage(quietWindow)
        } finally releaseShutdown.trySuccess(Done)
        resultOf(kit.system.whenTerminated)
      } finally kit.shutdownTestKit()
    }

    "leave a close failure as a failed shutdown task" in {
      val kit = ActorTestKit("CreateProducerSpecCloseFailure", ConfigFactory.parseString(
        """
          |pekko.coordinated-shutdown.phases.before-actor-system-terminate.recover = off
          |pekko.coordinated-shutdown.run-by-actor-system-terminate = off
          |""".stripMargin))
      try {
        val cause = new IllegalStateException("close blew up")
        val raws = createTestProbe[RecordingProducer]()
        val events = createTestProbe[ProducerEvent]()
        val producer = fixture(sys => baseSettings(sys, _ => {
          val raw = new RecordingProducer(events.ref, Some(cause))
          raws.ref ! raw
          raw
        }))

        resultOf(producer.init(kit.system))
        val raw = raws.receiveMessage(assertionTimeout)
        try {
          val shutdown = CoordinatedShutdown(kit.system).run(CoordinatedShutdown.UnknownReason)
          events.expectMessage(ProducerEvent.Flushed(raw))
          events.expectMessage(ProducerEvent.Closed(raw))
          failedWith(shutdown) shouldBe theSameInstanceAs(cause)
        } finally raw.closeUnderlying()
      } finally kit.shutdownTestKit()
    }

    "operate under serialize-messages via the local protocol exemption" in {
      val kit = ActorTestKit("CreateProducerSpecSerialized", ConfigFactory.parseString(
        "pekko.actor.serialize-messages = on"))
      try {
        val builds = createTestProbe[Done]()
        val producer = fixture(sys => baseSettings(sys, _ => { builds.ref ! Done; mock() }))

        val built = resultOf(producer.init(kit.system))
        resultOf(built.send(record))
        builds.expectMessage(Done)
        builds.expectNoMessage(quietWindow)
      } finally kit.shutdownTestKit()
    }
  }

  "CreateProducer.sendProducer" should {

    "build distinct caller-owned producers that do not touch the managed one" in {
      val raws = createTestProbe[RecordingProducer]()
      val events = createTestProbe[ProducerEvent]()
      val producer = fixture(sys => baseSettings(sys, _ => {
        val raw = new RecordingProducer(events.ref)
        raws.ref ! raw
        raw
      }))

      val first = resultOf(producer.sendProducer(system))
      val firstRaw = raws.receiveMessage(assertionTimeout)
      try {
        val second = resultOf(producer.sendProducer(system))
        val secondRaw = raws.receiveMessage(assertionTimeout)
        try {
          first should not be theSameInstanceAs(second)

          resultOf(first.close())
          resultOf(second.close())
          events.expectMessage(ProducerEvent.Flushed(firstRaw))
          events.expectMessage(ProducerEvent.Closed(firstRaw))
          events.expectMessage(ProducerEvent.Flushed(secondRaw))
          events.expectMessage(ProducerEvent.Closed(secondRaw))

          val managed = resultOf(producer.init(system))
          managed should not be theSameInstanceAs(first)
          managed should not be theSameInstanceAs(second)
          raws.receiveMessage(assertionTimeout)
        } finally if (!secondRaw.closed()) resultOf(second.close())
      } finally if (!firstRaw.closed()) resultOf(first.close())
    }

    "register no shutdown work for caller-owned producers" in {
      val kit = ActorTestKit("CreateProducerSpecCallerOwned")
      try {
        val raws = createTestProbe[RecordingProducer]()
        val events = createTestProbe[ProducerEvent]()
        val producer = fixture(sys => baseSettings(sys, _ => {
          val raw = new RecordingProducer(events.ref)
          raws.ref ! raw
          raw
        }))

        resultOf(producer.sendProducer(kit.system))
        val raw = raws.receiveMessage(assertionTimeout)
        kit.system.terminate()
        try {
          resultOf(kit.system.whenTerminated)
          events.expectNoMessage(quietWindow)
          raw.closed() shouldBe false
        } finally raw.closeUnderlying()
      } finally kit.shutdownTestKit()
    }

    "honor an enriched closeProducerOnStop = false and leave the raw producer open" in {
      val raws = createTestProbe[RecordingProducer]()
      val events = createTestProbe[ProducerEvent]()
      val producer = fixture(sys => baseSettings(sys, _ => {
        val raw = new RecordingProducer(events.ref)
        raws.ref ! raw
        raw
      }).withEnrichAsync(current => Future.successful(current.withCloseProducerOnStop(false))))

      val built = resultOf(producer.sendProducer(system))
      val raw = raws.receiveMessage(assertionTimeout)
      try {
        resultOf(built.close())
        events.expectNoMessage(quietWindow)
        raw.closed() shouldBe false
      } finally raw.closeUnderlying()
    }
  }
}
