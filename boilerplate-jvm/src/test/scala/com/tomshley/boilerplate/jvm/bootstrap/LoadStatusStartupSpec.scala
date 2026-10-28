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

import org.scalatest.concurrent.Eventually
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.time.{Milliseconds, Seconds, Span}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Promise
import scala.concurrent.Future

final class LoadStatusStartupSpec
    extends AnyWordSpec
    with Matchers
    with ScalaFutures
    with Eventually {

  override implicit val patienceConfig: PatienceConfig =
    PatienceConfig(timeout = Span(3, Seconds), interval = Span(25, Milliseconds))

  "LoadStatusStartup.start" should {
    "block until the provided Future completes" in {
      val p = Promise[Boolean]()

      val startF: Future[Unit] = Future {
        LoadStatusStartup.start(p.future)
      }

      Thread.sleep(150)
      startF.isCompleted shouldBe false

      p.success(true)

      eventually {
        startF.isCompleted shouldBe true
      }
    }

    "return normally when the provided Future fails" in {
      val p = Promise[Boolean]()

      val startF: Future[Unit] = Future {
        LoadStatusStartup.start(p.future)
      }

      Thread.sleep(150)
      startF.isCompleted shouldBe false

      p.failure(new RuntimeException("boom"))

      eventually {
        startF.isCompleted shouldBe true
      }

      startF.futureValue shouldBe ()
    }
  }
}
