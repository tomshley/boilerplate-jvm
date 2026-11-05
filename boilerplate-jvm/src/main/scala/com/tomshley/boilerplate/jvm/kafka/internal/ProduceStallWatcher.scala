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
import org.apache.pekko.actor.NoSerializationVerificationNeeded
import org.apache.pekko.actor.typed.Behavior
import org.apache.pekko.actor.typed.scaladsl.Behaviors

import scala.concurrent.duration.FiniteDuration

/** Deadline bookkeeping behind `ProduceStallGuard`, expressed as a pure
  * transition function `step` over `State`/`Event`/`Effect` plus a thin
  * interpreter behavior. The single `Deadline` timer is armed while at
  * least one submitted element is unsettled: a settle that drains the
  * count disarms it, and a settle that leaves elements in flight re-arms
  * it, restarting the bound from the last settle. Restarting a single
  * timer under the same key is safe — the scheduler cancels the
  * superseded registration and drops its message even if it was already
  * enqueued.
  *
  * An expiry with a bound target trips once and goes inert; an expiry
  * with no target bound warns once and lapses, holding the last
  * submission so a later `Bind` still delivers exactly one `Stall`.
  */
private[kafka] object ProduceStallWatcher:
  type Abort = Throwable => Unit

  sealed trait Command extends NoSerializationVerificationNeeded

  enum Event extends Command:
    case Bind(target: Abort)
    case Submitted(at: RecordPosition)
    case Settled
    case Expired

  case object Shutdown extends Command

  enum State:
    case Idle(target: Option[Abort])
    case InFlight(target: Option[Abort], count: Int, last: RecordPosition)
    case Lapsed(last: RecordPosition)
    case Tripped

  enum Effect:
    case Arm
    case Disarm
    case Trip(target: Abort, stall: ProduceStallGuard.Stall)
    case Warn

  private case object Deadline

  def step(label: String, bound: FiniteDuration): (State, Event) => (State, Option[Effect]) =
    (state, event) =>
      state match
        case State.Idle(target) =>
          event match
            case Event.Submitted(at) => (State.InFlight(target, 1, at), Some(Effect.Arm))
            case Event.Settled       => (state, None)
            case Event.Bind(abort)   => (State.Idle(Some(abort)), None)
            case Event.Expired       => (state, None)
        case State.InFlight(target, count, last) =>
          event match
            case Event.Submitted(at) => (State.InFlight(target, count + 1, at), None)
            case Event.Settled =>
              if (count == 1) (State.Idle(target), Some(Effect.Disarm))
              else (State.InFlight(target, count - 1, last), Some(Effect.Arm))
            case Event.Bind(abort) => (State.InFlight(Some(abort), count, last), None)
            case Event.Expired =>
              target match
                case Some(abort) =>
                  (
                    State.Tripped,
                    Some(Effect.Trip(abort, new ProduceStallGuard.Stall(label, bound, Some(last)))))
                case None => (State.Lapsed(last), Some(Effect.Warn))
        case State.Lapsed(last) =>
          event match
            case Event.Bind(abort) =>
              (
                State.Tripped,
                Some(Effect.Trip(abort, new ProduceStallGuard.Stall(label, bound, Some(last)))))
            case Event.Submitted(at) => (State.Lapsed(at), None)
            case Event.Settled       => (state, None)
            case Event.Expired       => (state, None)
        case State.Tripped =>
          (state, None)

  def apply(label: String, bound: FiniteDuration): Behavior[Command] =
    Behaviors.withTimers { timers =>
      Behaviors.setup { context =>
        val transit = step(label, bound)

        def run(effect: Effect): Unit =
          effect match
            case Effect.Arm                 => timers.startSingleTimer(Deadline, Event.Expired, bound)
            case Effect.Disarm              => timers.cancel(Deadline)
            case Effect.Trip(target, stall) => target(stall)
            case Effect.Warn =>
              context.log.warn("produce stall expired with no abort hook, {}", label)

        def watching(state: State): Behavior[Command] =
          Behaviors.receiveMessage {
            case Shutdown => Behaviors.stopped
            case event: Event =>
              val (next, effect) = transit(state, event)
              effect.foreach(run)
              watching(next)
          }

        watching(State.Idle(None))
      }
    }
