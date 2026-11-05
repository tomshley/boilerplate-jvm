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

package com.tomshley.boilerplate.jvm.kafka.internal

import com.tomshley.boilerplate.jvm.kafka.util.{ProduceStallGuard, RecordPosition}
import ProduceStallWatcher.{Effect, Event, State}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.duration.*

/** `ProduceStallWatcher.step` transition contract: every cell of the
  * `Idle`/`InFlight`/`Lapsed`/`Tripped` table, the arm/disarm/re-arm
  * sequences, and the `Stall` text a tripped guard delivers. Pure — no
  * actor and no clock.
  */
final class ProduceStallWatcherSpec extends AnyWordSpec with Matchers {

  private val bound = 1.second
  private val first = RecordPosition("orders", 0, 10L)
  private val second = RecordPosition("orders", 0, 11L)

  private def trace(events: Event*): (State, Vector[Effect]) = {
    val transit = ProduceStallWatcher.step("orders", bound)
    events.foldLeft((State.Idle(None): State, Vector.empty[Effect])) {
      case ((state, effects), event) =>
        val (next, effect) = transit(state, event)
        (next, effects ++ effect)
    }
  }

  private def trips(effects: Vector[Effect]): Vector[Effect.Trip] =
    effects.collect { case trip: Effect.Trip => trip }

  "ProduceStallWatcher.step" when {

    "idle" should {

      "arm and enter flight on the first submission" in {
        val (state, effects) = trace(Event.Submitted(first))
        state shouldBe State.InFlight(None, 1, first)
        effects shouldBe Vector(Effect.Arm)
      }

      "clamp a stray settle at zero" in {
        val (state, effects) = trace(Event.Settled)
        state shouldBe State.Idle(None)
        effects shouldBe empty
      }

      "hold a bound abort without an effect" in {
        val abort: Throwable => Unit = _ => ()
        val (state, effects) = trace(Event.Bind(abort))
        state shouldBe State.Idle(Some(abort))
        effects shouldBe empty
      }

      "ignore an expiry" in {
        val (state, effects) = trace(Event.Expired)
        state shouldBe State.Idle(None)
        effects shouldBe empty
      }
    }

    "in flight" should {

      "count further submissions without re-arming" in {
        val (state, effects) = trace(Event.Submitted(first), Event.Submitted(second))
        state shouldBe State.InFlight(None, 2, second)
        effects shouldBe Vector(Effect.Arm)
      }

      "re-arm when a settle leaves elements in flight" in {
        val (state, effects) = trace(Event.Submitted(first), Event.Submitted(second), Event.Settled)
        state shouldBe State.InFlight(None, 1, second)
        effects shouldBe Vector(Effect.Arm, Effect.Arm)
      }

      "disarm on the last settle" in {
        val (state, effects) = trace(Event.Submitted(first), Event.Settled)
        state shouldBe State.Idle(None)
        effects shouldBe Vector(Effect.Arm, Effect.Disarm)
      }

      "swap the bound abort without an effect" in {
        val abort: Throwable => Unit = _ => ()
        val (state, effects) = trace(Event.Submitted(first), Event.Bind(abort))
        state shouldBe State.InFlight(Some(abort), 1, first)
        effects shouldBe Vector(Effect.Arm)
      }

      "trip once on expiry with a bound abort" in {
        val abort: Throwable => Unit = _ => ()
        val (state, effects) = trace(Event.Submitted(first), Event.Bind(abort), Event.Expired)
        state shouldBe State.Tripped
        val all = trips(effects)
        all should have size 1
        all.head.target shouldBe theSameInstanceAs(abort)
        all.head.stall.label shouldBe "orders"
        all.head.stall.bound shouldBe bound
        all.head.stall.lastSubmitted shouldBe Some(first)
        effects shouldBe Vector(Effect.Arm, all.head)
      }

      "warn and lapse on expiry without a bound abort" in {
        val (state, effects) = trace(Event.Submitted(first), Event.Expired)
        state shouldBe State.Lapsed(first)
        effects shouldBe Vector(Effect.Arm, Effect.Warn)
      }
    }

    "lapsed" should {

      "trip once on a late bind" in {
        val abort: Throwable => Unit = _ => ()
        val (state, effects) = trace(Event.Submitted(first), Event.Expired, Event.Bind(abort))
        state shouldBe State.Tripped
        val all = trips(effects)
        all should have size 1
        all.head.target shouldBe theSameInstanceAs(abort)
        all.head.stall.lastSubmitted shouldBe Some(first)
        effects shouldBe Vector(Effect.Arm, Effect.Warn, all.head)
      }

      "track the last submission until a late bind" in {
        val abort: Throwable => Unit = _ => ()
        val (state, _) = trace(
          Event.Submitted(first),
          Event.Expired,
          Event.Submitted(second))
        state shouldBe State.Lapsed(second)
        val (_, effects) = trace(
          Event.Submitted(first),
          Event.Expired,
          Event.Submitted(second),
          Event.Bind(abort))
        val all = trips(effects)
        all should have size 1
        all.head.stall.lastSubmitted shouldBe Some(second)
      }

      "ignore settles" in {
        val (state, effects) = trace(Event.Submitted(first), Event.Expired, Event.Settled)
        state shouldBe State.Lapsed(first)
        effects shouldBe Vector(Effect.Arm, Effect.Warn)
      }

      "ignore expiries" in {
        val (state, effects) = trace(Event.Submitted(first), Event.Expired, Event.Expired)
        state shouldBe State.Lapsed(first)
        effects shouldBe Vector(Effect.Arm, Effect.Warn)
      }
    }

    "tripped" should {

      "stay inert under every event" in {
        val abort: Throwable => Unit = _ => ()
        val (state, effects) = trace(
          Event.Submitted(first),
          Event.Bind(abort),
          Event.Expired,
          Event.Submitted(second),
          Event.Settled,
          Event.Expired,
          Event.Bind(_ => ()))
        state shouldBe State.Tripped
        trips(effects) should have size 1
        effects shouldBe Vector(Effect.Arm, trips(effects).head)
      }
    }

    "across states" should {

      "arm only once across consecutive submissions" in {
        val (_, effects) = trace(
          Event.Submitted(first),
          Event.Submitted(second),
          Event.Submitted(first))
        effects shouldBe Vector(Effect.Arm)
      }

      "still arm when a stray settle precedes the first submission" in {
        val (state, effects) = trace(Event.Settled, Event.Submitted(first))
        state shouldBe State.InFlight(None, 1, first)
        effects shouldBe Vector(Effect.Arm)
      }
    }
  }

  "ProduceStallGuard.Stall" should {

    "render the label, bound, and last submitted position" in {
      new ProduceStallGuard.Stall("orders", 1.second, Some(RecordPosition("orders", 3, 42L)))
        .getMessage shouldBe
        "orders transactional produce settled nothing within 1 second — last submitted orders-3@42"
    }

    "render the label and bound alone with no last submission" in {
      new ProduceStallGuard.Stall("orders", 1.second, None)
        .getMessage shouldBe
        "orders transactional produce settled nothing within 1 second"
    }
  }
}
