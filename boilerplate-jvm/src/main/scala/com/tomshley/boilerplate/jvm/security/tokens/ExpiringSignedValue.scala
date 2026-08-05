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

import java.nio.charset.StandardCharsets
import java.time.Instant
import scala.concurrent.duration.{Duration, FiniteDuration}

/**
 * An authenticated value with an expiry.
 *
 * A signed value is authenticated, NOT encrypted: the value rides in clear inside
 * the opaque encoding, protected against tampering and expiry rollback by a
 * domain-separated truncated MAC. Put facts whose integrity matters in it, never
 * secrets.
 *
 * Instances are only produced by [[ExpiringSignedValue.verify]] — holding one
 * means the encoding was genuine and unexpired at verification time. The
 * constructor is package-private so that stays true; read and pattern-match
 * freely.
 */
final case class ExpiringSignedValue private[tokens] (
    value: String,
    expiresAt: Instant,
    kid: Int
)

/**
 * Sign and verify as two pure functions over immutable data. No instances, no
 * state.
 *
 * Decoded layout (`1 + 8 + valueBytes + 12` bytes): version/kid byte, expiry as
 * big-endian epoch millis, UTF-8 value, truncated MAC tail. Canonical unpadded
 * url-safe base64 on the wire. The codec appends its own format label to the
 * profile's domain separator before MACing, so this family may share a profile
 * and keyring with [[CompactMacToken]] without a token of one family ever
 * verifying as the other. `profile.payloadLength` is ignored (variable length).
 *
 * [[verify]] is total and returns `Either[RejectionReason, ExpiringSignedValue]`.
 * Expiry is judged only on an authentic value, and only STRICTLY after
 * `expiresAt + grace`.
 */
object ExpiringSignedValue {

  /** Cross-family separation label; [[CompactMacToken]] deliberately has none. */
  private val FormatLabel = "\u0000signed-value\u0000"

  private val FixedHeaderLength = 1 + 8
  private val MinimumDecodedLength = FixedHeaderLength + Hmac.MacLength

  /**
   * Sign `value` until `expiresAt` with the keyring's active slot.
   *
   * Signing without key material is a programmer error, not a runtime outcome:
   * the caller holds the keyring and can ask it (`activeKid`, `kids`) before
   * committing to sign.
   */
  def sign(keyring: Keyring, profile: TokenProfile, value: String, expiresAt: Instant): String = {
    require(value != null, "value must not be null")
    val (kid, secret) = keyring.activeKid
      .flatMap(active => keyring.secretFor(active).map(active -> _))
      .getOrElse(throw new IllegalStateException("cannot sign: keyring has no slots"))

    val valueBytes = value.getBytes(StandardCharsets.UTF_8)
    val header = new Array[Byte](FixedHeaderLength + valueBytes.length)
    header(0) = (((profile.version & 0x7) << 5) | (kid & 0x1f)).toByte
    val expiresAtMillis = expiresAt.toEpochMilli
    (0 until 8).foreach(i => header(1 + i) = ((expiresAtMillis >>> (8 * (7 - i))) & 0xff).toByte)
    System.arraycopy(valueBytes, 0, header, FixedHeaderLength, valueBytes.length)
    Base64Url.encode(header ++ Hmac.mac(secret, domain(profile), header))
  }

  /** Sign `value` for now plus `timeToLive` with the keyring's active slot. */
  def sign(keyring: Keyring, profile: TokenProfile, value: String, timeToLive: FiniteDuration): String = {
    require(timeToLive >= Duration.Zero, s"timeToLive must be non-negative, got $timeToLive")
    sign(keyring, profile, value, Instant.now().plusMillis(timeToLive.toMillis))
  }

  /** Verify `encoded` as of `now`. Total: never throws on any input. */
  def verify(
      keyring: Keyring,
      profile: TokenProfile,
      encoded: String,
      now: Instant
  ): Either[RejectionReason, ExpiringSignedValue] =
    for {
      bytes <- Base64Url.decode(encoded).filter(_.length >= MinimumDecodedLength).toRight(RejectionReason.Malformed)
      _ <- Either.cond((bytes(0) & 0xff) >>> 5 == profile.version, (), RejectionReason.UnknownVersion)
      kid = bytes(0) & 0x1f
      secret <- keyring.secretFor(kid).toRight(RejectionReason.UnknownKey)
      headerEnd = bytes.length - Hmac.MacLength
      _ <- Either.cond(
        Hmac.verify(secret, domain(profile), bytes.take(headerEnd), bytes.drop(headerEnd)),
        (),
        RejectionReason.SignatureMismatch
      )
      expiresAtMillis = (1 to 8).foldLeft(0L)((acc, i) => (acc << 8) | (bytes(i) & 0xff))
      _ <- Either.cond(
        now.toEpochMilli <= expiresAtMillis + profile.grace.toMillis,
        (),
        RejectionReason.Expired
      )
    } yield ExpiringSignedValue(
      new String(bytes, FixedHeaderLength, headerEnd - FixedHeaderLength, StandardCharsets.UTF_8),
      Instant.ofEpochMilli(expiresAtMillis),
      kid
    )

  private def domain(profile: TokenProfile): String = profile.domainSeparator + FormatLabel
}
