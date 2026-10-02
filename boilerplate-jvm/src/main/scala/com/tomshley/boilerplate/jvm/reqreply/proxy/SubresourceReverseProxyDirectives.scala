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

import java.util.Locale
import java.util.concurrent.TimeoutException
import org.apache.pekko.actor.typed.ActorSystem
import org.apache.pekko.http.scaladsl.Http
import org.apache.pekko.http.scaladsl.model.{HttpEntity, HttpHeader, HttpRequest, HttpResponse, StatusCodes}
import org.apache.pekko.http.scaladsl.server.{Directive0, PathMatchers, Route}
import org.apache.pekko.http.scaladsl.server.Directives.*
import org.apache.pekko.pattern.timeout
import org.apache.pekko.stream.{Materializer, SystemMaterializer}
import scala.collection.immutable
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}
import SubresourceReverseProxy.ConfigError

/** Routes for a closed set of upstream subresources, with no query forwarding.
 *  Application headers follow the declared allowlists; body encoding and cache
 *  metadata are retained independently. Connection-nominated fields and chunk
 *  metadata are removed. Nominating required entity/cache headers is refused
 *  (`400` inbound, `502` upstream).
 *
 *  The deadline covers response headers only. Late replies are drained; pool,
 *  transport, and body timeouts remain the HTTP client's responsibility.
 */
object SubresourceReverseProxyDirectives:

  /** The outgoing call, supplied at wiring so a caller can bring its own
   *  client settings and a check can stand the wire up in-process. */
  type Send = HttpRequest => Future[HttpResponse]

  /** Protocol metadata carried in addition to caller-declared headers —
   *  the entity bytes pass through untouched, so their describing fields
   *  and cache semantics must survive. */
  private val requestMetadata = Set("content-encoding")
  private val responseMetadata =
    Set("content-encoding", "content-range", "cache-control", "vary", "expires", "age", "date")

  /** Names a `Connection` nomination must not touch; nominating one
   *  refuses the exchange rather than silently corrupting it. */
  private val requestProtected = requestMetadata ++ Set("content-type", "content-length")
  private val responseProtected = responseMetadata ++ Set("content-type", "content-length")

  /** Every endpoint of one proxy. */
  def proxyRoute(proxy: SubresourceReverseProxy)(using system: ActorSystem[?]): Route =
    val http = Http()
    proxyRoute(proxy, request => http.singleRequest(request))

  def proxyRoute(proxy: SubresourceReverseProxy, send: Send)(using system: ActorSystem[?]): Route =
    concat(proxy.endpoints.map(endpoint => endpointRoute(proxy, endpoint, send))*)

  /** A whole proxy set bound at once — fails wiring rather than routing
   *  when two proxies claim the same local path. */
  def proxyRoutes(proxies: Seq[SubresourceReverseProxy])(using system: ActorSystem[?]): Either[ConfigError, Route] =
    val http = Http()
    proxyRoutes(proxies, request => http.singleRequest(request))

  def proxyRoutes(proxies: Seq[SubresourceReverseProxy], send: Send)(using system: ActorSystem[?]): Either[ConfigError, Route] =
    val duplicate = SubresourceReverseProxy.duplicatePaths(proxies.flatMap(p => p.endpoints.map(p.localPath)))
    Either.cond(duplicate.isEmpty, (), ConfigError.DuplicatePaths(duplicate))
      .map(_ => concat(proxies.map(p => proxyRoute(p, send))*))

  private def endpointRoute(
      proxy: SubresourceReverseProxy,
      endpoint: SubresourceReverseProxy.Endpoint,
      send: Send)(using system: ActorSystem[?]): Route =
    given ExecutionContext = system.executionContext
    given Materializer = SystemMaterializer(system).materializer
    val target = proxy.upstreamUri(endpoint)
    val matcher = PathMatchers.separateOnSlashes(proxy.localPath(endpoint).toString.drop(1))
    val requestAllowed = proxy.forwardHeaders ++ requestMetadata
    val responseAllowed = proxy.returnHeaders ++ responseMetadata
    val allowedMethods = endpoint.methods.toVector.sortBy(_.value)
      .map(m => method(m): Directive0).reduceLeft(_ | _)
    path(matcher) {
      allowedMethods {
        extractRequest { request =>
          passedThrough(requestAllowed, requestProtected, request.headers) match
            case None =>
              request.discardEntityBytes()
              complete(StatusCodes.BadRequest)
            case Some(forwarded) =>
              val outgoing = HttpRequest(
                method = request.method,
                uri = target,
                headers = forwarded,
                entity = request.entity match
                  case chunked: HttpEntity.Chunked => cleanChunks(chunked)
                  case entity => entity)
              val upstream: Future[HttpResponse] =
                Try(send(outgoing)).fold(Future.failed, identity)
              onComplete(withDeadline(proxy, upstream)) {
                case Success(response) =>
                  passedThrough(responseAllowed, responseProtected, response.headers) match
                    case Some(filtered) =>
                      complete(HttpResponse(
                        status = response.status,
                        headers = filtered,
                        entity = response.entity match
                          case chunked: HttpEntity.Chunked => cleanChunks(chunked)
                          case entity => entity))
                    case None =>
                      response.discardEntityBytes()
                      system.log.warn("{} upstream response nominated protected headers", proxy.name)
                      complete(StatusCodes.BadGateway)
                case Failure(_: TimeoutException) =>
                  system.log.warn("{} upstream did not answer within {}", proxy.name, proxy.timeout)
                  complete(StatusCodes.GatewayTimeout)
                case Failure(failure) =>
                  system.log.warn("{} upstream request failed", proxy.name, failure)
                  complete(StatusCodes.BadGateway)
              }
        }
      }
    }

  /** Bounds the wait for upstream response headers at `proxy.timeout`;
   *  the timer cancels when the upstream answers first. A reply arriving
   *  after the deadline is drained rather than left unread — an
   *  undiscarded entity would hold its connection-pool slot until idle
   *  timeout. */
  private def withDeadline(
      proxy: SubresourceReverseProxy,
      upstream: Future[HttpResponse])(using system: ActorSystem[?],
      ec: ExecutionContext, mat: Materializer): Future[HttpResponse] =
    timeout(proxy.timeout, system.classicSystem.scheduler)(upstream).andThen {
      case Failure(_) => upstream.foreach(_.discardEntityBytes())
      case Success(_) => ()
    }

  /** Allowlist filtering plus `Connection` nomination handling; `None`
   *  means a protected name was nominated and the exchange must fail. */
  private def passedThrough(
      allowed: Set[String],
      protectedNames: Set[String],
      headers: immutable.Seq[HttpHeader]): Option[immutable.Seq[HttpHeader]] =
    val connectionNames = headers.iterator.filter(_.is("connection"))
      .flatMap(_.value().split(',').iterator.map(_.trim.toLowerCase(Locale.ROOT))).toSet
    Option.unless(connectionNames.exists(protectedNames)) {
      headers.filter { h =>
        val name = h.lowercaseName()
        allowed(name) && !connectionNames(name)
      }
    }

  /** Hop metadata can also ride on chunk extensions and trailers,
   *  outside `HttpMessage.headers` — drop it while preserving byte
   *  content, content type, streaming, and backpressure. */
  private def cleanChunks(entity: HttpEntity.Chunked): HttpEntity.Chunked =
    entity.copy(chunks = entity.chunks.map {
      case HttpEntity.Chunk(data, _) => HttpEntity.Chunk(data, "")
      case _: HttpEntity.LastChunk => HttpEntity.LastChunk
    })
