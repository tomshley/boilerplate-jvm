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

final class ClusterHealthCheckRoutesSpec
    extends AnyWordSpec
    with Matchers
    with ScalatestRouteTest
    with Eventually {

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
    ActorSystem("cluster-health-routes", config)
  }

  private val routesUnderTest = new ClusterHealthCheckRoutes("127.0.0.1", 0)(system).routes

  "ClusterHealthCheckRoutes" should {
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
