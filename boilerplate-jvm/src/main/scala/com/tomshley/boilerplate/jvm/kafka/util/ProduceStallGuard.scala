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

import com.tomshley.boilerplate.jvm.kafka.internal.ProduceStallWatcher
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.pekko.NotUsed
import org.apache.pekko.actor.typed.{ActorRef, ActorSystem}
import org.apache.pekko.kafka.{ConsumerMessage, ProducerMessage}
import org.apache.pekko.stream.KillSwitches
import org.apache.pekko.stream.scaladsl.{Flow, Keep, Source}

import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration.{Duration, FiniteDuration}
import scala.util.control.NoStackTrace

/** The stream coordinates of a record a caller handed to a produce
  * stage — topic, partition, and offset only. `toString` renders
  * `topic-partition@offset` for logging; no record content is carried.
  */
final case class RecordPosition(topic: String, partition: Int, offset: Long):
  override def toString: String = s"$topic-$partition@$offset"

/** Evidence that a value can state where it sits in the stream. The
  * instances in `Positioned` cover the shapes that flow through a
  * `Transactional.source`/`Transactional.flow` pipeline: the committed
  * `PartitionOffset`, the consumed `ConsumerRecord`, the connector's
  * `TransactionalMessage`, and any `ProducerMessage.Envelope` whose
  * pass-through is itself positioned. Callers add an instance for their
  * own element types by writing a `given Positioned[TheirType]`. The
  * trait is invariant on purpose — positional evidence is not
  * substitutable across element types.
  */
trait Positioned[A]:
  extension (a: A) def position: RecordPosition

object Positioned:
  given Positioned[RecordPosition] with
    extension (a: RecordPosition) def position: RecordPosition = a

  given Positioned[ConsumerMessage.PartitionOffset] with
    extension (a: ConsumerMessage.PartitionOffset) def position: RecordPosition =
      RecordPosition(a.key.topic, a.key.partition, a.offset)

  given [K, V]: Positioned[ConsumerRecord[K, V]] with
    extension (a: ConsumerRecord[K, V]) def position: RecordPosition =
      RecordPosition(a.topic, a.partition, a.offset)

  given [K, V]: Positioned[ConsumerMessage.TransactionalMessage[K, V]] with
    extension (a: ConsumerMessage.TransactionalMessage[K, V]) def position: RecordPosition =
      a.partitionOffset.position

  given [K, V, P: Positioned]: Positioned[ProducerMessage.Envelope[K, V, P]] with
    extension (a: ProducerMessage.Envelope[K, V, P]) def position: RecordPosition =
      a.passThrough.position

/** A defensive bound on how long a produce stage may hold submitted
  * elements without settling any of them.
  *
  * A `Transactional.flow` stage learns each send's outcome through
  * asynchronous callbacks. If one never arrives, the stream neither
  * fails nor progresses: nothing is produced or committed, the source
  * offset never advances, and a surrounding `RestartSource` never
  * restarts. The guard turns that silent hang into a failure — it
  * aborts the materialization once with a `Stall`, so the caller's
  * restart policy rebuilds the pipeline with a fresh producer. Kafka's
  * client timeouts already bound every producer outcome; this covers
  * only a callback lost inside the connector's own stage.
  *
  * `submitted` records an element entering the produce stage and
  * `settled` records one leaving it. While at least one element is
  * unsettled a deadline runs; if it expires, the function given to
  * `bind` is invoked exactly once with a `ProduceStallGuard.Stall`. A
  * bound reached with no abort function bound warns and keeps the last
  * submitted position so a later `bind` still delivers one `Stall`.
  * `submitting`/`settling` expose the same signals as pass-through
  * flows, for callers that wrap a `Transactional.flow` directly.
  *
  * `shutdown` is fire-and-forget: an expiry already enqueued may still
  * call the abort function on an already-terminated kill switch, which
  * is a no-op. The caller owns the bound, the restart policy, and any
  * error mapping downstream of the kill switch.
  */
final class ProduceStallGuard private (watcher: ActorRef[ProduceStallWatcher.Command]):

  /** Binds the abort hook the guard invokes on expiry — typically a
    * kill switch's `abort`. The hook runs at most once, on the guard's
    * actor, and must not throw. A later `bind` before the bound expires
    * replaces the hook; once the guard has tripped, `bind` is ignored. */
  def bind(abortWith: Throwable => Unit): Unit =
    watcher ! ProduceStallWatcher.Event.Bind(abortWith)

  /** Marks `a` as handed to the produce stage and unsettled. */
  def submitted[A: Positioned](a: A): Unit =
    watcher ! ProduceStallWatcher.Event.Submitted(a.position)

  /** Marks one submitted element as settled by the produce stage. */
  def settled(): Unit =
    watcher ! ProduceStallWatcher.Event.Settled

  /** Stops the guard. Inert afterwards; see the class scaladoc for the
    * enqueued-expiry caveat. */
  def shutdown(): Unit =
    watcher ! ProduceStallWatcher.Shutdown

  /** Pass-through flow that marks each element submitted. */
  def submitting[A: Positioned]: Flow[A, A, NotUsed] =
    Flow[A].map { a => submitted(a); a }

  /** Pass-through flow that marks each element settled. */
  def settling[A]: Flow[A, A, NotUsed] =
    Flow[A].map { a => settled(); a }

object ProduceStallGuard:

  /** Raised once through the abort hook when the bound expires with
    * unsettled elements. Carries the caller's label, the bound, and the
    * position of the last submitted element — never record contents. */
  final class Stall(val label: String, val bound: FiniteDuration, val lastSubmitted: Option[RecordPosition])
      extends RuntimeException(
        s"$label transactional produce settled nothing within $bound" +
          lastSubmitted.fold("")(position => s" — last submitted $position"))
      with NoStackTrace

  /** Spawns the guard's watcher on `system` and returns the caller's
    * handle. `bound` must be positive. */
  def apply(label: String, bound: FiniteDuration)(using system: ActorSystem[?]): ProduceStallGuard =
    require(bound > Duration.Zero, "stall bound must be positive")
    new ProduceStallGuard(
      system.systemActorOf(ProduceStallWatcher(label, bound), s"produce-stall-guard-${UUID.randomUUID()}"))

  extension [Out, Mat](source: Source[Out, Mat])
    /** Wraps `source` in a kill switch bound to `guard`: an expiry
      * aborts the stream with `Stall`, and stream termination shuts the
      * guard down. Error mapping must sit downstream of this kill
      * switch so a `Stall` reaches it. Materialize the returned source
      * once: a second materialization re-binds the guard's single hook
      * to its own kill switch, and whichever run ends first shuts the
      * shared guard down. Use `guarded` for a guard per
      * materialization. */
    def guardedBy(guard: ProduceStallGuard): Source[Out, Mat] =
      source
        .viaMat(KillSwitches.single[Out])(Keep.both)
        .watchTermination() { case ((materialized, switch), done) =>
          guard.bind(switch.abort)
          done.onComplete(_ => guard.shutdown())(using ExecutionContext.parasitic)
          materialized
        }

  /** Builds a guarded source at materialization time. `build` receives
    * a fresh `ProduceStallGuard` for each inner materialization, so a
    * blueprint that is never materialized spawns nothing and restarts
    * (e.g. under `RestartSource`) never share a guard. A `build` that
    * throws fails the materialization with that exception. */
  def guarded[Out, Mat](label: String, bound: FiniteDuration)(
      build: ProduceStallGuard => Source[Out, Mat]
  )(using system: ActorSystem[?]): Source[Out, Future[Mat]] =
    Source.fromMaterializer { (_, _) =>
      val guard = ProduceStallGuard(label, bound)
      try build(guard).guardedBy(guard)
      catch
        case failure: Throwable =>
          guard.shutdown()
          throw failure
    }
