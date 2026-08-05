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
 * Immutable set of keyed slots (`kid` 0..31) for rotating MAC keys.
 *
 * The surface is metadata and rotation only — key bytes never leave the `tokens`
 * package, and no method returns them: a keyring is what you hand to `mint` and
 * `verify`, never a key container the caller can read.
 *
 * Rotation is functional: [[withKey]] and [[retire]] return new keyrings, and the
 * active kid is simply the highest occupied slot ("add the next slot, let
 * verification honor both, retire the old slot when its tokens have drained").
 *
 * `toString` shows slot ids only. Safe to share across threads.
 */
final class Keyring private (private val slots: Map[Int, SecretBytes]) {

  /** Occupied slot ids. */
  def kids: Set[Int] = slots.keySet

  /** Highest occupied slot, if any — the slot minting uses by default. */
  def activeKid: Option[Int] = slots.keys.maxOption

  /** True if no slot is occupied. */
  def isEmpty: Boolean = slots.isEmpty

  /** Opposite of [[isEmpty]]. */
  def nonEmpty: Boolean = slots.nonEmpty

  /** New keyring with `kid` bound to the hex-encoded key. */
  def withKey(kid: Int, hexKey: String): Keyring = {
    Keyring.requireSlot(kid)
    new Keyring(slots.updated(kid, SecretBytes.fromHex(hexKey)))
  }

  /** New keyring without `kid`; verification of that slot's tokens stops. */
  def retire(kid: Int): Keyring = new Keyring(slots - kid)

  private[tokens] def secretFor(kid: Int): Option[SecretBytes] = slots.get(kid)

  override def toString: String =
    s"Keyring(kids=${slots.keys.toSeq.sorted.mkString(",")})"
}

object Keyring {

  /** A keyring with no slots. Minting fails; every verification is `UnknownKey`. */
  val empty: Keyring = new Keyring(Map.empty)

  /** Build from hex-encoded keys by slot (0..31). */
  def fromHex(slotsHex: Map[Int, String]): Keyring = {
    slotsHex.keys.foreach(requireSlot)
    new Keyring(slotsHex.map((kid, hex) => kid -> SecretBytes.fromHex(hex)))
  }

  private def requireSlot(kid: Int): Unit =
    require(kid >= 0 && kid <= 31, s"keyring slot must be 0..31 (5 bits), got $kid")
}
