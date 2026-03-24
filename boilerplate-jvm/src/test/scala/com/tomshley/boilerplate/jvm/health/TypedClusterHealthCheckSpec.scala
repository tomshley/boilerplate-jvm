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
