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

package com.tomshley.boilerplate.jvm.staticassets

import org.apache.pekko.http.scaladsl.model.Uri
import org.apache.pekko.http.scaladsl.model.headers.{CacheDirectives, `Cache-Control`}

import scala.annotation.tailrec
import scala.util.matching.Regex

/**
 * Named `Cache-Control` policies for `getStaticAssetRoute(system, cachePolicy)`.
 * A `CachePolicy` is a `PartialFunction` so policies combine with the standard
 * `orElse` — `immutableWhere(hashed) orElse revalidate` — and a caller may
 * write its own in place of any named policy. Where a policy is undefined, no
 * `Cache-Control` is sent.
 */
object StaticAssetCaching {

  type CachePolicy = PartialFunction[Uri.Path, `Cache-Control`]

  private val immutableHeader: `Cache-Control` = `Cache-Control`(
    CacheDirectives.`public`,
    CacheDirectives.`max-age`(31536000),
    CacheDirectives.immutableDirective)

  private val revalidateHeader: `Cache-Control` = `Cache-Control`(CacheDirectives.`no-cache`(Nil))

  /** `public, max-age=31536000, immutable` on every path. */
  val immutable: CachePolicy = { case _ => immutableHeader }

  /** `no-cache` on every path: the response may be stored but must revalidate. */
  val revalidate: CachePolicy = { case _ => revalidateHeader }

  /**
   * Immutable on any path where some decoded segment fully matches `segment`,
   * undefined otherwise — combine with `orElse` to give the rest a header.
   * Segment boundaries are preserved: an encoded slash (`%2F`) stays inside its
   * segment rather than splitting it. The library ships no default pattern:
   * the caller owns which of its paths carry content-derived names, and a
   * generic `name-<hex>` guess would also match names such as
   * `release-20261022`.
   */
  def immutableWhere(segment: Regex): CachePolicy = {
    case path if matchesSegment(path, segment) => immutableHeader
  }

  private def matchesSegment(path: Uri.Path, segment: Regex): Boolean = {
    @tailrec
    def loop(remaining: Uri.Path): Boolean = remaining match {
      case Uri.Path.Empty => false
      case Uri.Path.Slash(tail) => loop(tail)
      case Uri.Path.Segment(value, tail) =>
        if (segment.matches(value)) true else loop(tail)
    }
    loop(path)
  }
}
