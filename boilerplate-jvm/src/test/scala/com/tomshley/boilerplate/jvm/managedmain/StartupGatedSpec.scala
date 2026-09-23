package com.tomshley.boilerplate.jvm.managedmain

import com.tomshley.boilerplate.jvm.kafka.util.KafkaTestSupport
import com.tomshley.boilerplate.jvm.kafka.util.KafkaTestSupport.{ProducerEvent, RecordingProducer, TestProducerFactory}
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.pekko.Done
import org.apache.pekko.actor.CoordinatedShutdown
import org.apache.pekko.actor.testkit.typed.scaladsl.{ActorTestKit, LogCapturing, LoggingTestKit, ScalaTestWithActorTestKit, TestProbe}
import org.apache.pekko.kafka.ProducerSettings
import org.apache.pekko.kafka.scaladsl.SendProducer
import org.scalatest.wordspec.AnyWordSpecLike

import scala.concurrent.{Future, Promise}
import scala.concurrent.duration.*
import scala.util.{Failure, Success, Try}

final class StartupGatedSpec extends ScalaTestWithActorTestKit with AnyWordSpecLike with LogCapturing {

  private val assertionTimeout = 10.seconds
  private val quietWindow = 300.millis

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

  "Startup.gated" should {

    "invoke onReady exactly once and only after the prerequisite completes" in {
      val kit = ActorTestKit("StartupGatedSpecOnce")
      try {
        val gate = Promise[Int]()
        val ready = createTestProbe[Int]()
        Startup.gated("once", gate.future, 5.seconds) { value => ready.ref ! value }
          .run(using kit.system)

        ready.expectNoMessage(quietWindow)
        gate.success(7)
        ready.expectMessage(7)
        ready.expectNoMessage(quietWindow)
      } finally kit.shutdownTestKit()
    }

    "terminate the ActorSystem when the prerequisite Future fails" in {
      val kit = ActorTestKit("StartupGatedSpecFailure")
      try {
        val ready = createTestProbe[Int]()
        Startup.gated("failing", Future.failed[Int](new IllegalStateException("prerequisite down")), 5.seconds) {
          value => ready.ref ! value
        }.run(using kit.system)

        ready.expectNoMessage(quietWindow)
        resultOf(kit.system.whenTerminated)
      } finally kit.shutdownTestKit()
    }

    "terminate the ActorSystem when the prerequisite throws synchronously" in {
      val kit = ActorTestKit("StartupGatedSpecThrow")
      try {
        val ready = createTestProbe[Int]()
        Startup.gated[Int]("throwing", throw new IllegalStateException("blew up inline"), 5.seconds) {
          value => ready.ref ! value
        }.run(using kit.system)

        ready.expectNoMessage(quietWindow)
        resultOf(kit.system.whenTerminated)
      } finally kit.shutdownTestKit()
    }

    "terminate without reporting ready when the onReady callback throws" in {
      val kit = ActorTestKit("StartupGatedSpecCallback")
      try {
        LoggingTestKit.info("ready").withOccurrences(0).expect {
          LoggingTestKit.error("onReady callback failed").expect {
            Startup.gated("callback", Future.successful(7), 5.seconds) { _ =>
              throw new IllegalStateException("bind failed")
            }.run(using kit.system)
            resultOf(kit.system.whenTerminated)
          }(using kit.system)
        }(using kit.system)
      } finally kit.shutdownTestKit()
    }

    "terminate the ActorSystem when the prerequisite times out" in {
      val kit = ActorTestKit("StartupGatedSpecTimeout")
      try {
        val ready = createTestProbe[Int]()
        val gate = Promise[Int]()
        Startup.gated("slow", gate.future, 200.millis) { value => ready.ref ! value }
          .run(using kit.system)

        ready.expectNoMessage(quietWindow)
        resultOf(kit.system.whenTerminated)
      } finally kit.shutdownTestKit()
    }

    "ignore a prerequisite that completes after the gate timed out" in {
      val kit = ActorTestKit("StartupGatedSpecLate")
      try {
        val gate = Promise[Int]()
        val ready = createTestProbe[Int]()
        val begun = createTestProbe[Done]()
        val releaseShutdown = Promise[Done]()
        CoordinatedShutdown(kit.system).addTask(
          CoordinatedShutdown.PhaseBeforeServiceUnbind,
          "hold-shutdown"
        )(() => { begun.ref ! Done; releaseShutdown.future })
        Startup.gated("late", gate.future, 200.millis) { value => ready.ref ! value }
          .run(using kit.system)

        begun.expectMessage(Done)
        try {
          gate.success(7)
          ready.expectNoMessage(quietWindow)
        } finally releaseShutdown.trySuccess(Done)
        resultOf(kit.system.whenTerminated)
      } finally kit.shutdownTestKit()
    }

    "run concurrent gates that reuse and freely format display names" in {
      val kit = ActorTestKit("StartupGatedSpecNames")
      try {
        val gateA = Promise[Int]()
        val gateB = Promise[Int]()
        val gateC = Promise[Int]()
        val ready = createTestProbe[Int]()
        Startup.gated("shared name", gateA.future, 5.seconds) { value => ready.ref ! (value + 1) }
          .run(using kit.system)
        Startup.gated("shared name", gateB.future, 5.seconds) { value => ready.ref ! (value + 2) }
          .run(using kit.system)
        Startup.gated("a name with spaces and ünicode", gateC.future, 5.seconds) { value => ready.ref ! (value + 3) }
          .run(using kit.system)

        gateA.success(1)
        gateB.success(10)
        gateC.success(100)
        ready.receiveMessages(3, assertionTimeout) should contain theSameElementsAs List(2, 12, 103)
      } finally kit.shutdownTestKit()
    }

    "gate readiness on the managed producer and let shutdown close it" in {
      val kit = ActorTestKit("StartupGatedSpecManaged")
      try {
        val attempts = createTestProbe[(ProducerSettings[String, String], Promise[ProducerSettings[String, String]])]()
        val raws = createTestProbe[RecordingProducer]()
        val events = createTestProbe[ProducerEvent]()
        val started = createTestProbe[SendProducer[String, String]]()
        val producer = new TestProducerFactory(sys =>
          KafkaTestSupport.producerSettings(sys, _ => {
            val raw = new RecordingProducer(events.ref)
            raws.ref ! raw
            raw
          }).withEnrichAsync { current =>
            val gate = Promise[ProducerSettings[String, String]]()
            attempts.ref ! (current -> gate)
            gate.future
          })

        Startup.gated("kafka-producer", producer.init(kit.system), 10.seconds) { ready =>
          started.ref ! ready
        }.run(using kit.system)

        val (current, gate) = attempts.receiveMessage(assertionTimeout)
        started.expectNoMessage(quietWindow)
        gate.success(current)
        val ready = started.receiveMessage(assertionTimeout)
        resultOf(ready.send(new ProducerRecord("topic", "key", "value")))

        val raw = raws.receiveMessage(assertionTimeout)
        kit.system.terminate()
        resultOf(kit.system.whenTerminated)
        events.expectMessage(ProducerEvent.Flushed(raw))
        events.expectMessage(ProducerEvent.Closed(raw))
      } finally kit.shutdownTestKit()
    }
  }
}
