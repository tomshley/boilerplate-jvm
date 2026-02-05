package com.tomshley.boilerplate.jvm.health

import com.typesafe.config.ConfigFactory
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.cluster.Cluster
import org.scalatest.concurrent.Eventually
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.time.{Seconds, Span}

final class ClusterHealthCheckSpec extends AnyWordSpec with Matchers with ScalaFutures with Eventually {

  "ClusterHealthCheck" should {
    "eventually return true when a single-node cluster is Up" in {
      val config = ConfigFactory.parseString(
        """
          |pekko.actor.provider = cluster
          |pekko.remote.artery.canonical.hostname = "127.0.0.1"
          |pekko.remote.artery.canonical.port = 0
          |pekko.cluster.jmx.enabled = off
          |pekko.cluster.seed-nodes = []
          |""".stripMargin
      )
      val system = ActorSystem("cluster-health", config)
      try {
        val cluster = Cluster(system)
        cluster.join(cluster.selfAddress)

        val hc = new ClusterHealthCheck(system)

        eventually(timeout(Span(3, Seconds))) {
          hc().futureValue shouldBe true
        }
      } finally {
        system.terminate()
      }
    }
  }
}
