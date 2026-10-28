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

package com.tomshley.boilerplate.jvm.health

import com.typesafe.config.ConfigFactory
import org.apache.pekko.actor.testkit.typed.scaladsl.ActorTestKit
import org.apache.pekko.cluster.Cluster
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.Eventually
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.time.{Seconds, Span}

final class TypedClusterHealthCheckSpec extends AnyWordSpec with Matchers with ScalaFutures with Eventually with BeforeAndAfterAll {
  private val config = ConfigFactory.parseString(
    """
      |pekko.actor.provider = cluster
      |pekko.remote.artery.canonical.hostname = "127.0.0.1"
      |pekko.remote.artery.canonical.port = 0
      |pekko.cluster.jmx.enabled = off
      |pekko.cluster.seed-nodes = []
      |""".stripMargin
  )
  private val testKit = ActorTestKit("typed-cluster-health", config)
  private given org.apache.pekko.actor.typed.ActorSystem[?] = testKit.system

  override def afterAll(): Unit = {
    testKit.shutdownTestKit()
    super.afterAll()
  }

  "TypedClusterHealthCheck" should {
    "eventually return true when a single-node cluster is Up" in {
      val cluster = Cluster(testKit.system.classicSystem)
      cluster.join(cluster.selfAddress)
      val hc = new TypedClusterHealthCheck

      eventually(timeout(Span(3, Seconds))) {
        hc().futureValue shouldBe true
      }
    }
  }
}
