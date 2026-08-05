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

import com.tomshley.boilerplate.jvm.basics.MintedPimpedBytes

import java.time.Instant

/**
 * A compact token that verified: authenticated payload plus its envelope facts.
 *
 * Construction is package-private on purpose: instances exist only as the
 * `Right` of [[CompactMacToken.verify]], so holding one is proof the token
 * verified. Read and pattern-match freely.
 */
final case class VerifiedToken private[tokens] (
    kid: Int,
    payload: MintedPimpedBytes,
    expiryDaysSinceEpoch: Int
)

/**
 * Fixed-length compact-MAC tokens as two pure functions over immutable data:
 * [[mint]] and [[verify]]. No instances, no state — a keyring and a profile in,
 * a token or a verdict out.
 *
 * Decoded layout (`1 + payloadLength + 2 + 12` bytes):
 * {{{
 * byte 0                  version (3 high bits) | kid (5 low bits)
 * bytes 1 .. N            payload (N = profile payload length)
 * bytes N+1 .. N+2        expiry, uint16 days-since-epoch, big-endian
 * last 12 bytes           truncated HMAC-SHA256 over everything before it
 * }}}
 * The uint16 expiry is a hard horizon: day 65535 is 2149-06-06. A family that
 * must outlive it needs a new `version`, not a wider field — the width is part of
 * the wire contract, and the version bits are how a decode ladder tells the
 * generations apart.
 *
 * Wire form is canonical unpadded url-safe base64 of the whole layout. The MAC
 * input is exactly `domainSeparator-bytes ++ header` with no codec-added framing,
 * so the wire bytes are a pure function of the caller's profile — families
 * sharing a keyring MUST use distinct domain separators.
 *
 * Verification is total ([[verify]] never throws) and returns
 * `Either[RejectionReason, VerifiedToken]`, so decode ladders compose with
 * `orElse`/`flatMap`. The MAC comparison is constant-time and sealed inside the
 * package. Checks are ordered so no rejection reveals more than the previous
 * one: shape, version, key id, authenticity — and expiry only on a token already
 * proven authentic, and only STRICTLY after `expiry + grace`.
 */
object CompactMacToken {

  private val MillisPerDay = 86_400_000L

  /**
   * Mint with the keyring's active slot.
   *
   * Minting without key material is a programmer error, not a runtime outcome:
   * the caller holds the keyring and can ask it (`activeKid`, `kids`) before
   * committing to mint.
   */
  def mint(
      keyring: Keyring,
      profile: TokenProfile,
      payload: MintedPimpedBytes,
      expiryDaysSinceEpoch: Int
  ): String = {
    val kid = keyring.activeKid.getOrElse(
      throw new IllegalStateException("cannot mint: keyring has no slots")
    )
    mint(keyring, profile, payload, expiryDaysSinceEpoch, kid)
  }

  /** Mint with an explicit slot. */
  def mint(
      keyring: Keyring,
      profile: TokenProfile,
      payload: MintedPimpedBytes,
      expiryDaysSinceEpoch: Int,
      kid: Int
  ): String = {
    require(
      payload.length == profile.payloadLength,
      s"payload must be exactly ${profile.payloadLength} bytes, got ${payload.length}"
    )
    require(
      expiryDaysSinceEpoch >= 0 && expiryDaysSinceEpoch <= 0xffff,
      s"expiryDaysSinceEpoch must be 0..65535 (uint16), got $expiryDaysSinceEpoch"
    )
    val secret = keyring
      .secretFor(kid)
      .getOrElse(throw new IllegalArgumentException(s"cannot mint: no keyring slot $kid"))

    val header = new Array[Byte](headerLength(profile))
    header(0) = (((profile.version & 0x7) << 5) | (kid & 0x1f)).toByte
    System.arraycopy(payload.underlying, 0, header, 1, profile.payloadLength)
    header(1 + profile.payloadLength) = ((expiryDaysSinceEpoch >>> 8) & 0xff).toByte
    header(2 + profile.payloadLength) = (expiryDaysSinceEpoch & 0xff).toByte
    Base64Url.encode(header ++ Hmac.mac(secret, profile.domainSeparator, header))
  }

  /** Verify `token` as of `now`. Total: never throws on any input. */
  def verify(
      keyring: Keyring,
      profile: TokenProfile,
      token: String,
      now: Instant
  ): Either[RejectionReason, VerifiedToken] =
    for {
      bytes <- Base64Url.decodeExact(token, decodedLength(profile)).toRight(RejectionReason.Malformed)
      _ <- Either.cond((bytes(0) & 0xff) >>> 5 == profile.version, (), RejectionReason.UnknownVersion)
      kid = bytes(0) & 0x1f
      secret <- keyring.secretFor(kid).toRight(RejectionReason.UnknownKey)
      header = bytes.take(headerLength(profile))
      presentedMac = bytes.drop(headerLength(profile))
      _ <- Either.cond(
        Hmac.verify(secret, profile.domainSeparator, header, presentedMac),
        (),
        RejectionReason.SignatureMismatch
      )
      expiryDays = ((bytes(1 + profile.payloadLength) & 0xff) << 8) | (bytes(2 + profile.payloadLength) & 0xff)
      _ <- Either.cond(!isExpired(profile, expiryDays, now), (), RejectionReason.Expired)
    } yield VerifiedToken(
      kid,
      MintedPimpedBytes(bytes.slice(1, 1 + profile.payloadLength)),
      expiryDays
    )

  /** Encoded (wire) length of this family's tokens. */
  def encodedLength(profile: TokenProfile): Int = Base64Url.encodedLength(decodedLength(profile))

  private def headerLength(profile: TokenProfile): Int = 1 + profile.payloadLength + 2

  private def decodedLength(profile: TokenProfile): Int = headerLength(profile) + Hmac.MacLength

  private def isExpired(profile: TokenProfile, expiryDaysSinceEpoch: Int, now: Instant): Boolean =
    now.toEpochMilli > expiryDaysSinceEpoch.toLong * MillisPerDay + profile.grace.toMillis
}
