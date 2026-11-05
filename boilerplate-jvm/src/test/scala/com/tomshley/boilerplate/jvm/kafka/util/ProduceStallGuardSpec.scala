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

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.pekko.actor.testkit.typed.scaladsl.{LogCapturing, LoggingTestKit, ScalaTestWithActorTestKit}
import org.apache.pekko.actor.typed.ActorSystem
import org.apache.pekko.kafka.{ConsumerMessage, ProducerMessage}
import org.scalatest.wordspec.AnyWordSpecLike

import Positioned.given

import scala.concurrent.duration.*

/** `ProduceStallGuard` actor contract against real timers: a settled
  * element disarms the bound, an unsettled element past the bound trips
  * the abort hook exactly once per guard, an unbound expiry warns once
  * and still trips on a late bind, `shutdown` silences an in-flight
  * deadline, and `Positioned` resolves stream coordinates for the
  * connector's record shapes.
  */
final class ProduceStallGuardSpec extends ScalaTestWithActorTestKit with AnyWordSpecLike with LogCapturing {

  private given ActorSystem[?] = testKit.system

  private val bound = 150.millis
  private val quietWindow = 400.millis
  private val assertionTimeout = 5.seconds

  "ProduceStallGuard" should {

    "abort the bound hook once with a Stall carrying the last submitted position" in {
      val guard = ProduceStallGuard("orders", bound)
      val aborts = createTestProbe[Throwable]()
      guard.bind(aborts.ref ! _)
      guard.submitted(RecordPosition("orders", 3, 42L))

      aborts.receiveMessage(assertionTimeout) match {
        case stall: ProduceStallGuard.Stall =>
          stall.label shouldBe "orders"
          stall.bound shouldBe bound
          stall.lastSubmitted shouldBe Some(RecordPosition("orders", 3, 42L))
        case other => fail(s"expected a Stall, got $other")
      }
      aborts.expectNoMessage(quietWindow)
    }

    "keep a stall in one guard away from another guard's hook" in {
      val orders = ProduceStallGuard("orders", bound)
      val payments = ProduceStallGuard("payments", bound)
      val ordersAborts = createTestProbe[Throwable]()
      val paymentsAborts = createTestProbe[Throwable]()
      orders.bind(ordersAborts.ref ! _)
      payments.bind(paymentsAborts.ref ! _)

      payments.submitted(RecordPosition("payments", 1, 9L))
      paymentsAborts.receiveMessage(assertionTimeout) match {
        case stall: ProduceStallGuard.Stall =>
          stall.label shouldBe "payments"
          stall.lastSubmitted shouldBe Some(RecordPosition("payments", 1, 9L))
        case other => fail(s"expected a Stall, got $other")
      }
      ordersAborts.expectNoMessage(quietWindow)
    }

    "never abort after shutdown while an element is in flight" in {
      val guard = ProduceStallGuard("orders", bound)
      val aborts = createTestProbe[Throwable]()
      guard.bind(aborts.ref ! _)
      guard.submitted(RecordPosition("orders", 0, 1L))
      guard.shutdown()

      aborts.expectNoMessage(quietWindow)
    }

    "warn once on an unbound expiry and abort once on a later bind" in {
      val guard = ProduceStallGuard("orders", bound)
      val aborts = createTestProbe[Throwable]()

      LoggingTestKit
        .warn("produce stall expired with no abort hook, orders")
        .withOccurrences(1)
        .expect {
          guard.submitted(RecordPosition("orders", 0, 7L))
          Thread.sleep(quietWindow.toMillis)
        }

      guard.bind(aborts.ref ! _)
      aborts.receiveMessage(assertionTimeout) match {
        case stall: ProduceStallGuard.Stall =>
          stall.lastSubmitted shouldBe Some(RecordPosition("orders", 0, 7L))
        case other => fail(s"expected a Stall, got $other")
      }
      aborts.expectNoMessage(quietWindow)
    }

    "resolve positions for the connector's record shapes" in {
      val partitionOffset = ConsumerMessage.PartitionOffset(
        ConsumerMessage.GroupTopicPartition("group", "orders", 3), 42L)
      val record = new ConsumerRecord[String, String]("orders", 3, 42L, "key", "value")
      val transactional = ConsumerMessage.TransactionalMessage(record, partitionOffset)
      val envelope = ProducerMessage.single(
        new ProducerRecord[String, String]("payments", "key", "value"),
        partitionOffset)

      val expected = RecordPosition("orders", 3, 42L)
      expected.position shouldBe expected
      expected.position.toString shouldBe "orders-3@42"
      partitionOffset.position shouldBe expected
      record.position shouldBe expected
      transactional.position shouldBe expected
      envelope.position shouldBe expected
      ProducerMessage
        .passThrough[String, String, ConsumerMessage.PartitionOffset](partitionOffset)
        .position shouldBe expected
    }

    "reject a non-positive bound" in {
      assertThrows[IllegalArgumentException] {
        ProduceStallGuard("orders", 0.seconds)
      }
      assertThrows[IllegalArgumentException] {
        ProduceStallGuard("orders", (-1).seconds)
      }
    }
  }
}
