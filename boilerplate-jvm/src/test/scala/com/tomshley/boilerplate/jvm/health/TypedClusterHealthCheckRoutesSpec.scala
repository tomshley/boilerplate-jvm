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
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.cluster.Cluster
import org.apache.pekko.http.scaladsl.model.ContentTypes
import org.apache.pekko.http.scaladsl.model.StatusCodes
import org.apache.pekko.http.scaladsl.testkit.ScalatestRouteTest
import org.scalatest.concurrent.Eventually
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.time.{Seconds, Span}

final class TypedClusterHealthCheckRoutesSpec
    extends AnyWordSpec
    with Matchers
    with ScalatestRouteTest
    with Eventually {

  import org.apache.pekko.actor.typed.scaladsl.adapter.*

  override protected def createActorSystem(): ActorSystem = {
    val config = ConfigFactory.parseString(
      """
        |pekko.actor.provider = cluster
        |pekko.remote.artery.canonical.hostname = "127.0.0.1"
        |pekko.remote.artery.canonical.port = 0
        |pekko.cluster.jmx.enabled = off
        |pekko.cluster.seed-nodes = []
        |""".stripMargin
    )
    ActorSystem("typed-cluster-health-routes", config)
  }

  private given org.apache.pekko.actor.typed.ActorSystem[?] = system.toTyped
  private val routesUnderTest = new TypedClusterHealthCheckRoutes("127.0.0.1", 0).routes

  "TypedClusterHealthCheckRoutes" should {
    "return 404 for /ready before the member is Up" in {
      Get("/ready") ~> routesUnderTest ~> check {
        status shouldBe StatusCodes.NotFound
      }
    }

    "return 404 for /alive before the member is Up" in {
      Get("/alive") ~> routesUnderTest ~> check {
        status shouldBe StatusCodes.NotFound
      }
    }

    "return 200 for /ready when member becomes Up" in {
      val cluster = Cluster(system)
      cluster.join(cluster.selfAddress)

      eventually(timeout(Span(3, Seconds))) {
        Get("/ready") ~> routesUnderTest ~> check {
          status shouldBe StatusCodes.OK
          contentType shouldBe ContentTypes.`application/json`
        }
      }
    }

    "return 200 for /alive when member becomes Up" in {
      val cluster = Cluster(system)
      cluster.join(cluster.selfAddress)

      eventually(timeout(Span(3, Seconds))) {
        Get("/alive") ~> routesUnderTest ~> check {
          status shouldBe StatusCodes.OK
          contentType shouldBe ContentTypes.`application/json`
        }
      }
    }
  }
}
