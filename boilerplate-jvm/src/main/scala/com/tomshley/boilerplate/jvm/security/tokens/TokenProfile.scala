/*
 * copyright 2023 tomshley llc
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

package com.tomshley.boilerplate.jvm.security.tokens

import scala.concurrent.duration.{Duration, FiniteDuration}

/**
 * Immutable parameters of one token family. All values are caller-supplied — the
 * library ships no defaults:
 *
 *   - `domainSeparator`: mixed into every MAC. Families that share a keyring MUST
 *     each use a distinct separator.
 *   - `version`: 3-bit format version carried in the token's top bits (0..7).
 *   - `payloadLength`: fixed compact-token payload size in bytes (ignored by
 *     [[ExpiringSignedValue]], which is variable-length).
 *   - `grace`: how long past nominal expiry a token still verifies. Expired means
 *     STRICTLY after `expiry + grace` — exactly at the boundary is valid.
 */
final case class TokenProfile(
    domainSeparator: String,
    version: Int,
    payloadLength: Int,
    grace: FiniteDuration
) {
  require(domainSeparator.nonEmpty, "domainSeparator must not be empty")
  require(version >= 0 && version <= 7, s"version must be 0..7 (3 bits), got $version")
  require(payloadLength >= 0 && payloadLength <= 1024, s"payloadLength must be 0..1024, got $payloadLength")
  require(grace >= Duration.Zero, s"grace must be non-negative, got $grace")
}
