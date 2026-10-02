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
package com.tomshley.boilerplate.jvm.reqreply.proxy

import org.apache.pekko.http.scaladsl.model.{HttpMethod, HttpMethods, Uri}
import scala.concurrent.duration.{Duration, FiniteDuration}
import scala.util.Try

/** Validated wiring for exact same-origin routes to one upstream origin.
 *  [[SubresourceReverseProxy.create]] and [[SubresourceReverseProxy.Endpoint.create]]
 *  return configuration errors as values and retain parsed Pekko URIs and paths.
 *  The caller owns endpoint selection, the HTTP client, and the response-header
 *  deadline. Proxying preserves the trust required in the upstream content.
 */
final class SubresourceReverseProxy private (
    val name: String,
    val upstream: Uri,
    val mount: Uri.Path,
    val endpoints: Vector[SubresourceReverseProxy.Endpoint],
    val forwardHeaders: Set[String],
    val returnHeaders: Set[String],
    val timeout: FiniteDuration):

  /** The local request path for an endpoint — the mount plus its path. */
  def localPath(endpoint: SubresourceReverseProxy.Endpoint): Uri.Path =
    mount ++ endpoint.path

  /** The upstream URI for an endpoint — the declared origin plus its path. */
  def upstreamUri(endpoint: SubresourceReverseProxy.Endpoint): Uri =
    upstream.withPath(endpoint.path)

object SubresourceReverseProxy:

  /** Wiring-time rejection reasons — one case per broken invariant. */
  enum ConfigError:
    case EmptyName
    case InvalidOrigin(value: String)
    case InvalidPath(value: String)
    case EmptyEndpoints
    case DuplicatePaths(paths: Set[Uri.Path])
    case InvalidMethods(methods: Set[HttpMethod])
    case InvalidHeaderNames(names: Set[String])
    case ForbiddenForwardHeaders(names: Set[String])
    case ForbiddenReturnHeaders(names: Set[String])
    case InvalidTimeout(value: FiniteDuration)

  /** One validated endpoint: an exact path under the mount plus the
   *  nonempty subset of supported methods it answers. */
  final class Endpoint private (val path: Uri.Path, val methods: Set[HttpMethod])

  object Endpoint:
    def create(path: String, methods: Set[HttpMethod]): Either[ConfigError, Endpoint] =
      for
        parsed <- parsePath(path)
        _ <- Either.cond(methods.nonEmpty && methods.subsetOf(allowedMethods), (),
          ConfigError.InvalidMethods(methods))
      yield new Endpoint(parsed, methods)

  /** Default application headers. `referer` can contain the requesting page's
   *  path and query; callers choose which context may reach their upstream.
   */
  val DefaultForwardHeaders: Set[String] =
    Set("user-agent", "referer", "accept-language")

  /** Optional response headers. The directives also retain body encoding and
   *  cache semantics regardless of this allowlist.
   */
  val DefaultReturnHeaders: Set[String] = Set("etag", "last-modified")

  /** Supported methods; each endpoint selects a nonempty subset. */
  private val allowedMethods: Set[HttpMethod] =
    Set(HttpMethods.GET, HttpMethods.HEAD, HttpMethods.POST,
        HttpMethods.PUT, HttpMethods.PATCH, HttpMethods.DELETE)

  /** Hop-by-hop fields describe this connection, not the proxy's. */
  private val hopHeaders: Set[String] =
    Set("connection", "keep-alive", "proxy-authenticate", "proxy-authorization",
        "te", "trailer", "transfer-encoding", "upgrade", "timeout-access")

  /** Credentials, original authority, and hop fields never go upstream. */
  private val forbiddenForward: Set[String] =
    hopHeaders ++ Set("cookie", "cookie2", "authorization", "host",
        "forwarded", "x-real-ip", "content-type", "content-length")

  /** Vendor cookies and authentication challenges never reach the caller. */
  private val forbiddenReturn: Set[String] =
    hopHeaders ++ Set("set-cookie", "set-cookie2", "www-authenticate",
        "content-type", "content-length")

  /** RFC 9110 header-name tokens, ASCII lowercase only — matched
   *  against a character class, so the check is locale-independent. */
  private val headerToken = "[!#$%&'*+\\-.^_`|~0-9a-z]+".r

  /** Shape of a bare origin before native parsing: `http(s)://` plus an
   *  authority that excludes the userinfo marker `@` and every
   *  path/query/fragment delimiter, then at most one trailing slash.
   *  Anything carrying a real path — `/x`, `/./`, `/../`, `//` — fails
   *  to match before normalization could hide it. */
  private val originSyntax = "(?i)https?://([^/?#@]+)/?".r

  /** Endpoint paths and mounts land verbatim in the URIs the proxy
   *  builds and are matched against the request's decoded path, so
   *  they are plain literals: RFC 3986 raw path characters, no escapes. */
  private val pathChars: Set[Char] =
    (('A' to 'Z') ++ ('a' to 'z') ++ ('0' to '9') ++ "-._~!$&'()*+,;=:@/").toSet

  def create(
      name: String,
      upstream: String,
      mount: String,
      endpoints: Seq[Endpoint],
      timeout: FiniteDuration,
      forwardHeaders: Set[String] = DefaultForwardHeaders,
      returnHeaders: Set[String] = DefaultReturnHeaders): Either[ConfigError, SubresourceReverseProxy] =
    val duplicate = duplicatePaths(endpoints.map(_.path))
    val badNames = (forwardHeaders ++ returnHeaders).filterNot(headerToken.matches)
    val badForward = forwardHeaders.filter(h => forbiddenForward(h) || h.startsWith("x-forwarded-"))
    val badReturn = returnHeaders.intersect(forbiddenReturn)
    for
      _ <- Either.cond(name.trim.nonEmpty, (), ConfigError.EmptyName)
      origin <- parseOrigin(upstream)
      prefix <- parsePath(mount)
      _ <- Either.cond(endpoints.nonEmpty, (), ConfigError.EmptyEndpoints)
      _ <- Either.cond(duplicate.isEmpty, (), ConfigError.DuplicatePaths(duplicate))
      _ <- Either.cond(timeout > Duration.Zero, (), ConfigError.InvalidTimeout(timeout))
      _ <- Either.cond(badNames.isEmpty, (), ConfigError.InvalidHeaderNames(badNames))
      _ <- Either.cond(badForward.isEmpty, (), ConfigError.ForbiddenForwardHeaders(badForward))
      _ <- Either.cond(badReturn.isEmpty, (), ConfigError.ForbiddenReturnHeaders(badReturn))
    yield new SubresourceReverseProxy(
      name, origin, prefix, endpoints.toVector, forwardHeaders, returnHeaders, timeout)

  /** Exact-path duplicates — shared with the directives' cross-proxy check. */
  private[proxy] def duplicatePaths(paths: Seq[Uri.Path]): Set[Uri.Path] =
    paths.groupMapReduce(identity)(_ => 1)(_ + _)
      .collect { case (path, n) if n > 1 => path }.toSet

  /** The regex refuses userinfo, real paths, query, and fragment before
   *  normalization could hide them; native `Uri` parsing then rules on
   *  syntax. The port check runs on the raw authority so an IPv6 `:` inside
   *  brackets is not mistaken for a port, while an explicit port must be a
   *  number in range. */
  private def parseOrigin(value: String): Either[ConfigError, Uri] = value match
    case originSyntax(authority) =>
      val portAt = authority.lastIndexOf(':')
      val port = Option.when(portAt > authority.lastIndexOf(']'))(authority.drop(portAt + 1))
      for
        _ <- Either.cond(
          value.forall(_ <= 0x7f) &&
            port.forall(_.toIntOption.exists(p => p > 0 && p <= 65535)),
          (), ConfigError.InvalidOrigin(value))
        uri <- Try(Uri(value, Uri.ParsingMode.Strict))
          .toEither.left.map(_ => ConfigError.InvalidOrigin(value))
        _ <- Either.cond(
          uri.authority.host.address.nonEmpty &&
            uri.authority.userinfo.isEmpty &&
            (uri.path.isEmpty || uri.path == Uri.Path.SingleSlash) &&
            uri.rawQueryString.isEmpty && uri.fragment.isEmpty,
          (), ConfigError.InvalidOrigin(value))
      yield uri.withPath(Uri.Path.Empty)
    case _ => Left(ConfigError.InvalidOrigin(value))

  /** Absolute literal paths, excluding escapes, empty segments, and dot segments. */
  private def parsePath(value: String): Either[ConfigError, Uri.Path] =
    val valid = value.startsWith("/") && !value.endsWith("/") && value.forall(pathChars) &&
      !value.split('/').iterator.drop(1).exists(s => s.isEmpty || s == "." || s == "..")
    for
      _ <- Either.cond(valid, (), ConfigError.InvalidPath(value))
      path <- Try(Uri.Path(value)).toEither.left.map(_ => ConfigError.InvalidPath(value))
    yield path
