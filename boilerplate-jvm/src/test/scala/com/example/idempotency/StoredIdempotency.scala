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

package com.example.idempotency

import com.tomshley.boilerplate.jvm.reqreply.Idempotent
import org.apache.pekko.persistence.typed.{EventAdapter, EventSeq, SnapshotAdapter}

import java.time.Instant

object StoredIdempotency {
  sealed trait TestSerializable

  sealed trait StoredEvent extends TestSerializable

  final case class Started(key: String,
                           on: Instant,
                           headers: Option[Map[String, String]],
                           body: Option[String]) extends StoredEvent

  final case class RepeatedRequest() extends StoredEvent

  final case class SavedReply(on: Instant,
                              headers: Option[Map[String, String]],
                              body: Option[String]) extends StoredEvent

  final case class RepeatedReply() extends StoredEvent

  final case class Abandoned() extends StoredEvent

  final case class StoredState(idempotencyKey: Option[String],
                               requestedOn: Option[Instant],
                               repliedOn: Option[Instant],
                               requestCount: Int,
                               replyCount: Int,
                               requestHeaders: Option[Map[String, String]],
                               requestBody: Option[String],
                               replyHeaders: Option[Map[String, String]],
                               replyBody: Option[String]) extends TestSerializable

  val eventAdapter: EventAdapter[Idempotent.Event, StoredEvent] =
    new EventAdapter[Idempotent.Event, StoredEvent] {
      override def toJournal(event: Idempotent.Event): StoredEvent =
        event match {
          case Idempotent.RequestRecorded(key, on, headers, body) =>
            Started(key, on, headers, body)
          case Idempotent.RequestRepeated => RepeatedRequest()
          case Idempotent.ReplyRecorded(on, headers, body) =>
            SavedReply(on, headers, body)
          case Idempotent.ReplyRepeated => RepeatedReply()
          case Idempotent.RequestAbandoned => Abandoned()
        }

      override def manifest(event: Idempotent.Event): String = "1"

      override def fromJournal(stored: StoredEvent,
                               manifest: String): EventSeq[Idempotent.Event] =
        (stored, manifest) match {
          case (Started(key, on, headers, body), "1") =>
            EventSeq.single(Idempotent.RequestRecorded(key, on, headers, body))
          case (RepeatedRequest(), "1") =>
            EventSeq.single(Idempotent.RequestRepeated)
          case (SavedReply(on, headers, body), "1") =>
            EventSeq.single(Idempotent.ReplyRecorded(on, headers, body))
          case (RepeatedReply(), "1") =>
            EventSeq.single(Idempotent.ReplyRepeated)
          case (Abandoned(), "1") =>
            EventSeq.single(Idempotent.RequestAbandoned)
          case other =>
            throw new IllegalArgumentException(s"unknown stored event $other")
        }
    }

  val snapshotAdapter: SnapshotAdapter[Idempotent.State] =
    new SnapshotAdapter[Idempotent.State] {
      override def toJournal(state: Idempotent.State): Any =
        StoredState(
          state.idempotencyKey,
          state.requestedOn,
          state.repliedOn,
          state.requestCount,
          state.replyCount,
          state.requestHeaders,
          state.requestBody,
          state.replyHeaders,
          state.replyBody
        )

      override def fromJournal(from: Any): Idempotent.State =
        from match {
          case StoredState(key, requestedOn, repliedOn, requestCount,
                           replyCount, requestHeaders, requestBody,
                           replyHeaders, replyBody) =>
            Idempotent.State(key, requestedOn, repliedOn, requestCount,
                             replyCount, requestHeaders, requestBody,
                             replyHeaders, replyBody)
          case other =>
            throw new IllegalArgumentException(s"unknown stored state $other")
        }
    }
}
