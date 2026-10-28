/*
 * copyright 2026 tomshley llc
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

package com.tomshley.boilerplate.jvm.durablebufferedflush

import org.apache.pekko.actor.testkit.typed.scaladsl.ActorTestKit
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

final class AdmissionControllerSpec
    extends AnyWordSpec
    with Matchers
    with ScalaFutures
    with BeforeAndAfterAll {

  private val testKit = ActorTestKit("AdmissionControllerSpec")

  override def afterAll(): Unit = {
    testKit.shutdownTestKit()
    super.afterAll()
  }

  "AdmissionController" should {

    // Defaults to open. The first observation that callers make is via
    // `isOpen()`, and it MUST resolve `true` so that workflows can admit
    // sessions out of the box without first wiring a pressure monitor.
    "default to open after construction" in {
      val controller = AdmissionController(testKit.system)
      controller.isOpen().futureValue shouldBe true
    }

    // Both transitions (close and open) are idempotent. A subscriber that
    // calls `close` on every monitor tick while at Critical must produce
    // the same state regardless of how many ticks fire. We await each
    // transition so the assertion observes a stable post-state regardless
    // of mailbox ordering across cores.
    "be idempotent on repeated close/open transitions" in {
      val controller = AdmissionController(testKit.system)

      controller.close("first").futureValue
      controller.close("second-no-op").futureValue
      controller.close("third-no-op").futureValue
      controller.isOpen().futureValue shouldBe false

      controller.open().futureValue
      controller.open().futureValue
      controller.open().futureValue
      controller.isOpen().futureValue shouldBe true
    }

    // The AlwaysOpen sentinel never closes — it's the safe default for
    // workflows wired without a real admission controller. Its futures
    // are pre-completed, so no actor traffic is involved.
    "AlwaysOpen sentinel never closes regardless of close() calls" in {
      val sentinel = AdmissionController.AlwaysOpen
      sentinel.isOpen().futureValue shouldBe true

      sentinel.close("trying to close a sentinel").futureValue
      sentinel.isOpen().futureValue shouldBe true

      sentinel.open().futureValue
      sentinel.isOpen().futureValue shouldBe true
    }
  }
}
