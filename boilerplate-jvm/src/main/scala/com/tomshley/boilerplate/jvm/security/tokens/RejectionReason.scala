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

/**
 * Why verification failed, as the `Left` of every `verify`.
 *
 * The reasons are deliberately distinct — a tampered token ([[SignatureMismatch]])
 * must never be conflated with a stale one ([[Expired]]): they mean different
 * things operationally and typically alert differently.
 */
enum RejectionReason {

  /** Not the expected shape: bad alphabet, padding, or wrong decoded length. */
  case Malformed

  /** Shape is right but the version bits are not this family's version. */
  case UnknownVersion

  /** The key id has no slot in the keyring (never minted here, or retired). */
  case UnknownKey

  /** MAC did not verify — tampered with, or minted with another key. */
  case SignatureMismatch

  /** Authentic, but strictly past expiry + grace. */
  case Expired
}
