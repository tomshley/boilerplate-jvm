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

import org.apache.pekko.{Done, NotUsed}
import org.apache.pekko.actor.testkit.typed.scaladsl.{LogCapturing, ScalaTestWithActorTestKit, TestProbe}
import org.apache.pekko.actor.typed.ActorSystem
import org.apache.pekko.stream.{KillSwitches, Materializer, RestartSettings}
import org.apache.pekko.stream.scaladsl.{Keep, RestartSource, Sink, Source}
import org.scalatest.concurrent.Eventually
import org.scalatest.time.{Millis, Seconds, Span}
import org.scalatest.wordspec.AnyWordSpecLike

import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.concurrent.duration.*
import scala.util.{Failure, Success, Try}

/** `ProduceStallGuard.guarded` end-to-end contract on real streams: a
  * stage that settles nothing fails the stream with a `Stall` naming
  * the last submitted position, a healthy stream completes and stays
  * quiet, a `RestartSource` restart re-materializes a fresh guard, a
  * throwing `build` fails the materialization, and two materializations
  * of one blueprint never share a guard.
  */
final class ProduceStallGuardStreamSpec
    extends ScalaTestWithActorTestKit
    with AnyWordSpecLike
    with LogCapturing
    with Eventually {

  override implicit val patienceConfig: PatienceConfig =
    PatienceConfig(timeout = Span(10, Seconds), interval = Span(50, Millis))

  private given ActorSystem[?] = testKit.system
  private given Materializer = Materializer(testKit.system)

  private val bound = 200.millis
  private val quietWindow = 500.millis
  private val assertionTimeout = 10.seconds

  private def positionOf(i: Int): RecordPosition = RecordPosition("orders", 0, i.toLong)

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

  "ProduceStallGuard.guarded" should {

    "fail a stream whose produce stage settles nothing with a Stall naming the last position" in {
      val result = ProduceStallGuard.guarded("orders", bound) { guard =>
        Source(1 to 3)
          .map(positionOf)
          .via(guard.submitting)
          .mapAsync(1)(position =>
            if (position.offset == 2L) Promise[RecordPosition]().future
            else Future.successful(position))
          .via(guard.settling)
      }.runWith(Sink.seq)

      failedWith(result) match {
        case stall: ProduceStallGuard.Stall =>
          stall.label shouldBe "orders"
          stall.bound shouldBe bound
          stall.lastSubmitted shouldBe Some(positionOf(2))
        case other => fail(s"expected a Stall, got $other")
      }
    }

    "complete a healthy stream and raise no Stall after the bound window" in {
      val errors = createTestProbe[Throwable]()
      val result = ProduceStallGuard
        .guarded("orders", bound) { guard =>
          Source(1 to 3)
            .map(positionOf)
            .via(guard.submitting)
            .mapAsync(1)(Future.successful)
            .via(guard.settling)
        }
        .mapError { case error =>
          errors.ref ! error
          error
        }
        .runWith(Sink.seq)

      resultOf(result) shouldBe (1 to 3).map(positionOf)
      errors.expectNoMessage(quietWindow)
    }

    "re-materialize a fresh guard when a RestartSource restarts after a stall" in {
      val materializations = new AtomicInteger(0)
      val (killSwitch, _) = RestartSource
        .onFailuresWithBackoff(RestartSettings(50.millis, 200.millis, 0.0)) { () =>
          materializations.incrementAndGet()
          ProduceStallGuard.guarded("orders", bound) { guard =>
            Source(1 to 3)
              .map(positionOf)
              .via(guard.submitting)
              .mapAsync(1)(_ => Promise[RecordPosition]().future)
              .via(guard.settling)
          }
        }
        .viaMat(KillSwitches.single)(Keep.right)
        .toMat(Sink.ignore)(Keep.both)
        .run()

      eventually {
        materializations.get() should be >= 2
      }
      killSwitch.shutdown()
    }

    "fail the stream with the exception a throwing build raises" in {
      val cause = new IllegalStateException("blueprint blew up")
      val (materialized, result) =
        ProduceStallGuard.guarded[RecordPosition, NotUsed]("orders", bound)(_ => throw cause)
          .toMat(Sink.seq)(Keep.both)
          .run()

      failedWith(result) shouldBe theSameInstanceAs(cause)
      failedWith(materialized) shouldBe theSameInstanceAs(cause)
    }

    "keep guards independent across materializations of one blueprint" in {
      val materializations = new AtomicInteger(0)
      val release = Promise[Done]()
      val blueprint = ProduceStallGuard.guarded("orders", bound) { guard =>
        val first = materializations.incrementAndGet() == 1
        Source(1 to 3)
          .mapAsync(1)(i =>
            if (i == 3) release.future.map(_ => i)(using ExecutionContext.parasitic)
            else Future.successful(i))
          .map(positionOf)
          .via(guard.submitting)
          .mapAsync(1)(position =>
            if (first && position.offset == 2L) Promise[RecordPosition]().future
            else Future.successful(position))
          .via(guard.settling)
      }

      val (firstMaterialized, stalled) = blueprint.toMat(Sink.seq)(Keep.both).run()
      resultOf(firstMaterialized)
      val (secondMaterialized, healthy) = blueprint.toMat(Sink.seq)(Keep.both).run()
      resultOf(secondMaterialized)

      failedWith(stalled) match {
        case stall: ProduceStallGuard.Stall =>
          stall.lastSubmitted shouldBe Some(positionOf(2))
        case other => fail(s"expected a Stall, got $other")
      }
      release.success(Done)
      resultOf(healthy) shouldBe (1 to 3).map(positionOf)
    }
  }
}
