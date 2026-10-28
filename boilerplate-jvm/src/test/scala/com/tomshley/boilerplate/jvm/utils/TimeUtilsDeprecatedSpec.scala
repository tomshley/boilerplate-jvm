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

package com.tomshley.boilerplate.jvm.utils

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

final class TimeUtilsDeprecatedSpec extends AnyWordSpec with Matchers {
  private object UnderTest extends TimeUtils

  "TimeUtils" should {
    "resolve deprecated type aliases" in {
      val now: UnderTest.DateTime = UnderTest.DateTime.now()
      now.getYear should be > 2000
    }

    "resolve deprecated forwarders" in {
      val formatted = UnderTest.DateTimeFormat.forPattern("yyyy-MM-dd").print(UnderTest.DateTime.now())
      formatted.length shouldBe 10
    }
  }
}
