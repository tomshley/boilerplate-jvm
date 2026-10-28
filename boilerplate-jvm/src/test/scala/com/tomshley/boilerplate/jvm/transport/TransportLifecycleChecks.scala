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

package com.tomshley.boilerplate.jvm.transport

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.pekko.actor.CoordinatedShutdown
import org.apache.pekko.actor.testkit.typed.scaladsl.ActorTestKit
import org.apache.pekko.actor.typed.ActorSystem
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import java.net.{InetSocketAddress, ServerSocket}
import scala.concurrent.Future

private[transport] trait TransportLifecycleChecks {
  self: AnyWordSpec & Matchers & ScalaFutures =>

  protected def baseKitConfig: Config = ConfigFactory.parseString(
    """
    pekko.actor.provider = local
    pekko.http.server.enable-http2 = on
    pekko.coordinated-shutdown.run-by-jvm-shutdown-hook = off
    """
  )

  protected def withKit[A](name: String, config: Config = ConfigFactory.empty())(
      test: ActorTestKit => A
  ): A = {
    val kit = ActorTestKit(name, config.withFallback(baseKitConfig))
    try test(kit)
    finally kit.shutdownTestKit()
  }

  protected def causes(error: Throwable): List[Throwable] =
    error :: Option(error.getCause).toList.flatMap(causes)

  protected def serverLifecycleCases[B](
      label: String,
      start: (ActorSystem[?], Int) => Future[B],
      localAddress: B => InetSocketAddress,
      awaitReleased: (ActorSystem[?], B) => Future[?]
  ): Unit = {

    s"terminate the actor system when the port is occupied ($label)" in
      withKit(s"occupied-$label") { kit =>
        val occupying = new ServerSocket()
        try {
          occupying.bind(new InetSocketAddress("127.0.0.1", 0))
          val binding = start(kit.system, occupying.getLocalPort)
          binding.failed.futureValue
          kit.system.whenTerminated.futureValue
        } finally occupying.close()
      }

    s"release the port during coordinated shutdown ($label)" in
      withKit(s"release-$label") { kit =>
        val binding = start(kit.system, 0).futureValue
        val address = localAddress(binding)
        CoordinatedShutdown(kit.system)
          .run(CoordinatedShutdown.UnknownReason)
          .futureValue
        awaitReleased(kit.system, binding).futureValue
        val released = new ServerSocket()
        try {
          released.setReuseAddress(true)
          released.bind(address)
        } finally released.close()
      }
  }
}
