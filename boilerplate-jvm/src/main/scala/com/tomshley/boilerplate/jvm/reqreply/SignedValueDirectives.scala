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
package com.tomshley.boilerplate.jvm.reqreply

import com.tomshley.boilerplate.jvm.reqreply.exceptions.{ExpiredExpiringValueRejection, ExpiringValueRejection}
import com.tomshley.boilerplate.jvm.security.tokens.{ExpiringSignedValue, Keyring, RejectionReason, TokenProfile}
import com.tomshley.boilerplate.jvm.utils.ConfigKeyUtil
import org.apache.pekko.http.scaladsl.server.Directive1
import org.apache.pekko.http.scaladsl.server.Directives.*

import java.time.Instant
import scala.concurrent.duration.FiniteDuration
import scala.jdk.CollectionConverters.*
import scala.jdk.DurationConverters.*
import scala.util.matching.Regex

private object SignedValueSettings extends ConfigKeyUtil {
  final val keysPath = "tomshley-boilerplate-reqreply-signing.keys"
  final val gracePath = "tomshley-boilerplate-reqreply-signing.grace"
  final val domainSeparator = "tomshley-boilerplate-reqreply-signed-value-v1\n"
  final val version = 1

  def configuredKeyring: Keyring =
    if (!config.hasPath(keysPath)) Keyring.empty
    else {
      val keys = config.getConfig(keysPath)
      Keyring.fromHex(
        keys
          .root()
          .keySet()
          .asScala
          .map { name =>
            val kid = name.toIntOption.getOrElse(
              throw new IllegalArgumentException(s"signing key slot '$name' is not an integer 0..31")
            )
            kid -> keys.getString(name)
          }
          .toMap
      )
    }

  def configuredProfile: TokenProfile =
    TokenProfile(domainSeparator, version, payloadLength = 0, grace = config.getDuration(gracePath).toScala)
}

/**
 * Directives over `security.tokens.ExpiringSignedValue`: authenticated, expiring
 * values with a domain-separated truncated MAC.
 *
 * Keys come from configuration and there is NO default key material — an
 * unconfigured deployment signs nothing and verifies nothing (every value
 * rejects). Configure hex keys by slot, e.g.:
 *
 * {{{
 * tomshley-boilerplate-reqreply-signing.keys.1 = "<64 hex chars, e.g. openssl rand -hex 32>"
 * tomshley-boilerplate-reqreply-signing.grace = 0 s
 * }}}
 */
trait SignedValueDirectives {

  protected def signedValueKeyring: Keyring = SignedValueDirectives.configuredKeyring

  protected def signedValueProfile: TokenProfile = SignedValueDirectives.configuredProfile

  final def signedPathMatcher: Regex = SignedValueDirectives.base64UrlSafeRegex

  /**
   * Sign a value for `timeToLive` from now, using the configured keyring.
   *
   * @throws IllegalStateException
   *   if no signing key is configured — a deployment error, surfaced loudly rather
   *   than by handing out values nobody can verify.
   */
  def signValue(value: String, timeToLive: FiniteDuration): String = {
    val keyring = signedValueKeyring
    if (keyring.isEmpty)
      throw new IllegalStateException(
        s"cannot sign: no signing key configured — set ${SignedValueSettings.keysPath}.<slot 0..31> to a hex key"
      )
    ExpiringSignedValue.sign(keyring, signedValueProfile, value, timeToLive)
  }

  /**
   * Provide the verified value, or reject it.
   *
   * Expiry is called out (the client can refresh); every other failure rejects
   * identically on purpose — telling a caller whether a key id exists, a MAC
   * mismatched, or the encoding was malformed turns the route into a verification
   * oracle. The distinct [[RejectionReason]]s stay server-side.
   */
  def signedValue(matchedPath: String): Directive1[ExpiringSignedValue] =
    ExpiringSignedValue.verify(signedValueKeyring, signedValueProfile, matchedPath, Instant.now()) match {
      case Right(verified) => provide(verified)
      case Left(RejectionReason.Expired) =>
        reject(ExpiredExpiringValueRejection("The value has expired"))
      case Left(_) =>
        reject(ExpiringValueRejection("Unable to validate the signed value"))
    }

  /**
   * Verify outside a route: `Some` only for a genuine, unexpired encoding.
   * The distinct [[RejectionReason]]s are deliberately not surfaced here for the
   * same anti-oracle reason as [[signedValue]].
   */
  final def verifiedValue(encoded: String): Option[ExpiringSignedValue] =
    ExpiringSignedValue.verify(signedValueKeyring, signedValueProfile, encoded, Instant.now()).toOption
}

object SignedValueDirectives extends SignedValueDirectives {
  private val base64UrlSafeRegex: Regex = """^[A-Za-z0-9\-_]+$""".r
  private lazy val configuredKeyring: Keyring = SignedValueSettings.configuredKeyring
  private lazy val configuredProfile: TokenProfile = SignedValueSettings.configuredProfile
}