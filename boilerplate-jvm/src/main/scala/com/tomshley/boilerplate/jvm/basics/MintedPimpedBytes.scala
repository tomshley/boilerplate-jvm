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

package com.tomshley.boilerplate.jvm.basics

import com.fasterxml.jackson.annotation.{JsonCreator, JsonValue}
import com.fasterxml.jackson.annotation.JsonCreator.Mode.DELEGATING

import java.util.Arrays

/**
 * Minted, pimped byte array — immutable with content-based equality, cached hashCode,
 * and transparent Jackson CBOR serialization.
 *
 * Solves the problem that Array[Byte] uses identity comparison, which breaks:
 * - Case class equals/hashCode/copy
 * - Set/Map membership
 * - Test assertions
 *
 * Provides transparent Jackson CBOR serialization via @JsonValue/@JsonCreator.
 * Serializes identically to raw Array[Byte] (CBOR major type 2 binary blob).
 *
 * @param bytes The wrapped byte array (defensive copy returned via underlying)
 */
final class MintedPimpedBytes private (private val bytes: Array[Byte])
    extends PimpedType[Array[Byte]] {

  /**
   * Returns a defensive copy of the wrapped bytes.
   * Satisfies PimpedType contract and Jackson CBOR serialization.
   */
  @JsonValue
  override def underlying: Array[Byte] = bytes.clone()

  /** Number of bytes */
  def length: Int = bytes.length

  /** True if zero-length */
  def isEmpty: Boolean = bytes.length == 0

  /** Opposite of isEmpty */
  def nonEmpty: Boolean = bytes.length != 0

  /**
   * Canonical lowercase zero-padded hex representation.
   * Example: Array(0x0a, 0x1b, 0xff) → "0a1bff"
   */
  def toHex: String = bytes.map(b => f"${b & 0xFF}%02x").mkString

  /**
   * Content-based equality using java.util.Arrays.equals.
   * Two MintedPimpedBytes are equal if their byte content is identical.
   */
  override def equals(other: Any): Boolean = other match {
    case that: MintedPimpedBytes => Arrays.equals(this.bytes, that.bytes)
    case _ => false
  }

  /**
   * Cached hashCode computed at construction time.
   * Safe because bytes array is never mutated or exposed.
   */
  override val hashCode: Int = Arrays.hashCode(bytes)

  /**
   * Debugger-friendly string representation.
   * Format: "MintedPimpedBytes(0a1bff)"
   */
  override def toString: String = s"MintedPimpedBytes(${toHex})"
}

object MintedPimpedBytes {

  /** Singleton for zero-length bytes */
  val empty: MintedPimpedBytes = new MintedPimpedBytes(Array.emptyByteArray)

  /**
   * Primary factory with defensive copy.
   * Jackson uses this for deserialization via @JsonCreator.
   *
   * @param bytes Source byte array (copied, not stored directly)
   * @return MintedPimpedBytes wrapping a copy of the input
   */
  @JsonCreator(mode = DELEGATING)
  def apply(bytes: Array[Byte]): MintedPimpedBytes = {
    require(bytes != null, "bytes must not be null")
    if (bytes.length == 0) empty
    else new MintedPimpedBytes(bytes.clone())
  }

  /**
   * Parse hex string to bytes.
   * Inverse of toHex.
   *
   * @param hex Hex string (e.g., "0a1bff")
   * @return MintedPimpedBytes containing the parsed bytes
   */
  def fromHex(hex: String): MintedPimpedBytes = {
    require(hex != null, "hex must not be null")
    require(hex.length % 2 == 0, "Hex string must have even length")
    val bytes = hex.grouped(2).map(Integer.parseInt(_, 16).toByte).toArray
    apply(bytes)
  }
}

/**
 * Pimp-my-library extension for Array[Byte].
 * Provides ergonomic conversion at ACL boundaries.
 */
extension (bytes: Array[Byte])
  def toMintedPimpedBytes: MintedPimpedBytes = MintedPimpedBytes(bytes)
