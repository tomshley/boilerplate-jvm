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

package com.tomshley.boilerplate.jvm.claimcheck

import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.ExecutionContext

final class ClaimCheckSpec extends AnyWordSpec with Matchers with ScalaFutures {

  given ExecutionContext = ExecutionContext.global

  "ClaimCheck" should {
    "check returns a ticket with correct fields" in {
      val claimCheck = new InMemoryClaimCheck("bucket-a")

      val ticket = claimCheck.check("hello".getBytes("UTF-8"), tag = "tag-1").futureValue
      ticket.number shouldBe "tag-1"
      ticket.location shouldBe "bucket-a"
      ticket.storeType shouldBe "in-memory"
      ticket.sizeBytes shouldBe Some(5L)
    }

    "claim retrieves original data" in {
      val claimCheck = new InMemoryClaimCheck("bucket-a")

      val payload = "hello".getBytes("UTF-8")
      val ticket = claimCheck.check(payload, tag = "tag-1").futureValue

      val claimed = claimCheck.claim(ticket).futureValue
      claimed.isDefined shouldBe true
      new String(claimed.get, "UTF-8") shouldBe "hello"
    }

    "claim returns None for unknown ticket" in {
      val claimCheck = new InMemoryClaimCheck("bucket-a")

      val ticket: ClaimTicket = ClaimTicket(number = "missing", location = "bucket-a", storeType = "in-memory")
      claimCheck.claim(ticket).futureValue shouldBe None
    }

    "discard removes data" in {
      val claimCheck = new InMemoryClaimCheck("bucket-a")

      val payload = "hello".getBytes("UTF-8")
      val ticket = claimCheck.check(payload, tag = "tag-1").futureValue

      claimCheck.discard(ticket).futureValue shouldBe true
      claimCheck.claim(ticket).futureValue shouldBe None
    }

    "exists returns true for checked data and false after discard" in {
      val claimCheck = new InMemoryClaimCheck("bucket-a")

      val payload = "hello".getBytes("UTF-8")
      val ticket = claimCheck.check(payload, tag = "tag-1").futureValue

      claimCheck.exists(ticket).futureValue shouldBe true
      claimCheck.discard(ticket).futureValue shouldBe true
      claimCheck.exists(ticket).futureValue shouldBe false
    }
  }
}
