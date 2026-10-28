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

package com.tomshley.boilerplate.jvm.durablebufferedflush

import org.apache.pekko.actor.testkit.typed.scaladsl.ActorTestKit
import org.apache.pekko.actor.typed.ActorSystem
import org.apache.pekko.util.Timeout
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.duration.*

final class ExpectedCountRegistrySpec
    extends AnyWordSpec
    with Matchers
    with BeforeAndAfterAll
    with ScalaFutures {

  private val testKit = ActorTestKit("ExpectedCountRegistrySpec")
  private given ActorSystem[?] = testKit.system
  private given Timeout = Timeout(3.seconds)

  override def afterAll(): Unit = {
    testKit.shutdownTestKit()
    super.afterAll()
  }

  "ExpectedCountRegistry" should {

    "store and clear expected counts by entity id" in {
      val registry = new ExpectedCountRegistry(testKit.system, "expected-count-registry")

      registry.get("entity-1").futureValue shouldBe None

      registry.put("entity-1", 3L).futureValue
      registry.get("entity-1").futureValue shouldBe Some(3L)

      registry.put("entity-1", 4L).futureValue
      registry.get("entity-1").futureValue shouldBe Some(4L)

      registry.remove("entity-1").futureValue
      registry.get("entity-1").futureValue shouldBe None
    }

    "keep counts isolated across entity ids" in {
      val registry = new ExpectedCountRegistry(testKit.system, "expected-count-registry")

      registry.put("entity-1", 2L).futureValue
      registry.put("entity-2", 5L).futureValue

      registry.get("entity-1").futureValue shouldBe Some(2L)
      registry.get("entity-2").futureValue shouldBe Some(5L)
    }
  }
}
