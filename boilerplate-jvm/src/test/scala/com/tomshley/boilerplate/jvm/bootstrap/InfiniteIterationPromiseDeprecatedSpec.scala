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

package com.tomshley.boilerplate.jvm.bootstrap

import org.apache.pekko.actor.testkit.typed.scaladsl.ActorTestKit
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.Eventually
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.time.{Milliseconds, Seconds, Span}

import java.util.concurrent.atomic.AtomicInteger

final class InfiniteIterationPromiseDeprecatedSpec
    extends AnyWordSpec
    with Matchers
    with ScalaFutures
    with Eventually
    with BeforeAndAfterAll {

  override implicit val patienceConfig: PatienceConfig =
    PatienceConfig(timeout = Span(3, Seconds), interval = Span(25, Milliseconds))

  private val testKit = ActorTestKit("InfiniteIterationPromiseDeprecatedSpec")

  override def afterAll(): Unit = {
    testKit.shutdownTestKit()
    super.afterAll()
  }

  "InfiniteIterationPromise" should {
    "keep looping until interrupted by a failure" in {
      val counter = new AtomicInteger(0)

      val result = InfiniteIterationPromise(testKit.system, {
        val current = counter.incrementAndGet()
        if (current >= 5) throw new RuntimeException("stop")
      })

      eventually {
        counter.get() should be >= 5
      }

      result.failed.futureValue.getMessage shouldBe "stop"
    }
  }
}
