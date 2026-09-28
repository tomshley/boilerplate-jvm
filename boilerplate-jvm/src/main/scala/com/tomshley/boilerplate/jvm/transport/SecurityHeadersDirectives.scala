/*
 * copyright 2026 tomshley llc
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

package com.tomshley.boilerplate.jvm.transport

import org.apache.pekko.http.scaladsl.model.HttpHeader
import org.apache.pekko.http.scaladsl.model.headers.{RawHeader, `Strict-Transport-Security`}
import org.apache.pekko.http.scaladsl.server.{Directive0, Directives}

/** Configurable browser response headers. */
trait SecurityHeadersDirectives {

  /** Applies the baseline set as defaults on every response the inner route
    * completes: an inner route that sets the same header keeps its own
    * value, so a route may override per response. Rejections and exceptions
    * that escape the wrapped route become responses outside it and do not
    * carry these headers — wrap a sealed route (`Route.seal`) to cover
    * error responses.
    *
    * @param contentSecurityPolicy rendered verbatim; None emits no CSP
    * @param transportSecurity HSTS header; None emits none. The default
    *        pins HTTPS for a year and includes every subdomain, and browsers
    *        cache it for that long — keep it only when this host and all of
    *        its subdomains are HTTPS-only, or pass a header without
    *        `includeSubDomains`.
    * @param frameOptions X-Frame-Options value; None emits none
    * @param referrerPolicy Referrer-Policy value; None emits none
    * @param permissionsPolicy Permissions-Policy value; None emits none
    */
  def respondWithSecurityHeaders(
      contentSecurityPolicy: Option[String] = None,
      transportSecurity: Option[`Strict-Transport-Security`] =
        Some(`Strict-Transport-Security`(31536000L, includeSubDomains = true)),
      frameOptions: Option[String] = Some("DENY"),
      referrerPolicy: Option[String] = Some("strict-origin-when-cross-origin"),
      permissionsPolicy: Option[String] = Some("camera=(), microphone=(), geolocation=()")
  ): Directive0 = {
    val headers: Seq[HttpHeader] = Seq(
      Some(RawHeader("X-Content-Type-Options", "nosniff")),
      frameOptions.map(RawHeader("X-Frame-Options", _)),
      referrerPolicy.map(RawHeader("Referrer-Policy", _)),
      permissionsPolicy.map(RawHeader("Permissions-Policy", _)),
      transportSecurity,
      contentSecurityPolicy.map(RawHeader("Content-Security-Policy", _))
    ).flatten
    Directives.respondWithDefaultHeaders(headers)
  }
}
