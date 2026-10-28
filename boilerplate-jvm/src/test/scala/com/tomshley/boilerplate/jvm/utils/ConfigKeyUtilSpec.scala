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

final class ConfigKeyUtilSpec extends AnyWordSpec with Matchers {

  private object UnderTest extends ConfigKeyUtil

  "ConfigKeyUtil" should {
    "getValueWithDefault return None when key missing" in {
      UnderTest.getValueWithDefault[String]("definitely.missing.key", None) shouldBe None
    }

    "getValueWithDefault return the configured value when key is present" in {
      UnderTest.getValueWithDefault[String]("config-key-util-fixture.present", Some("x")) shouldBe Some("0123456789abcdef")
    }

    "getValueWithDefault return default when key is null" in {
      UnderTest.getValueWithDefault[String]("config-key-util-fixture.explicitly-null", Some("x")) shouldBe Some("x")
    }
  }
}
