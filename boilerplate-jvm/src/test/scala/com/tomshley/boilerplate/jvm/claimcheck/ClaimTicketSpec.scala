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

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

final class ClaimTicketSpec extends AnyWordSpec with Matchers {

  "ClaimTicket" should {
    "store all fields" in {
      val ticket = ClaimTicket(
        number = "n",
        location = "loc",
        storeType = "type",
        checkedAt = None,
        expiresAt = None,
        sizeBytes = Some(123L)
      )

      ticket.number shouldBe "n"
      ticket.location shouldBe "loc"
      ticket.storeType shouldBe "type"
      ticket.sizeBytes shouldBe Some(123L)
    }

    "apply creates a simple ticket" in {
      val ticket = ClaimTicket("n", "loc", "type")
      ticket.number shouldBe "n"
      ticket.location shouldBe "loc"
      ticket.storeType shouldBe "type"
    }

    "withTimestamp sets checkedAt" in {
      val ticket = ClaimTicket.withTimestamp("n", "loc", "type")
      ticket.checkedAt.isDefined shouldBe true
    }
  }
}
