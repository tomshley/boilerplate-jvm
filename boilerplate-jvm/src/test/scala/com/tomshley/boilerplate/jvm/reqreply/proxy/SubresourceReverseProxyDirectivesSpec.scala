/*
 * copyright 2026 tomshley llc
 *
 * licensed under the apache license, version 2.0 (the "license");
 * you may not use this file except in compliance with the license.
 * you may obtain a copy of the license at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
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

import org.apache.pekko.{Done, NotUsed}
import org.apache.pekko.actor.testkit.typed.scaladsl.{LogCapturing, TestProbe}
import org.apache.pekko.actor.typed.ActorSystem
import org.apache.pekko.actor.typed.scaladsl.adapter.*
import org.apache.pekko.http.scaladsl.Http
import org.apache.pekko.http.scaladsl.model.{ContentTypes, HttpEntity, HttpMethod, HttpMethods, HttpRequest, HttpResponse, StatusCodes, UniversalEntity, Uri}
import org.apache.pekko.http.scaladsl.model.headers.{Connection, Cookie, HttpEncodings, RawHeader, `Cache-Control`, CacheDirectives, `Content-Encoding`, `User-Agent`}
import org.apache.pekko.http.scaladsl.server.Directives.{complete, extractRequest, onSuccess}
import org.apache.pekko.http.scaladsl.server.Route
import org.apache.pekko.http.scaladsl.testkit.ScalatestRouteTest
import org.apache.pekko.stream.scaladsl.{Keep, Sink, Source}
import org.apache.pekko.util.ByteString
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{Millis, Seconds, Span}
import org.scalatest.wordspec.AnyWordSpec
import scala.collection.immutable
import scala.concurrent.{Future, Promise}
import scala.concurrent.duration.{Duration, DurationInt, FiniteDuration}
import SubresourceReverseProxy.ConfigError
import SubresourceReverseProxyDirectives.{Send, proxyRoute, proxyRoutes}

final class SubresourceReverseProxyDirectivesSpec
    extends AnyWordSpec
    with Matchers
    with ScalatestRouteTest
    with ScalaFutures
    with LogCapturing {

  implicit override val patienceConfig: PatienceConfig =
    PatienceConfig(Span(5, Seconds), Span(50, Millis))

  private given ActorSystem[?] = system.toTyped

  private def endpoint(
      path: String,
      methods: Set[HttpMethod]): SubresourceReverseProxy.Endpoint =
    SubresourceReverseProxy.Endpoint.create(path, methods)
      .fold(error => fail(error.toString), identity)

  private def proxy(
      name: String = "analytics",
      upstream: String = "https://upstream.example",
      mount: String = "/vendor",
      endpoints: Seq[SubresourceReverseProxy.Endpoint] = Seq(
        endpoint("/script.js", Set(HttpMethods.GET)),
        endpoint("/api/send", Set(HttpMethods.POST))),
      timeout: FiniteDuration = 5.seconds,
      forwardHeaders: Set[String] = SubresourceReverseProxy.DefaultForwardHeaders,
      returnHeaders: Set[String] = SubresourceReverseProxy.DefaultReturnHeaders): SubresourceReverseProxy =
    SubresourceReverseProxy.create(name, upstream, mount, endpoints, timeout,
        forwardHeaders = forwardHeaders, returnHeaders = returnHeaders)
      .fold(error => fail(error.toString), identity)

  private def validRoutes(proxies: Seq[SubresourceReverseProxy], send: Send): Route =
    proxyRoutes(proxies, send).fold(error => fail(error.toString), identity)

  /** An entity whose stream announces its own termination — a drained
   *  entity is observable without inspecting internals. */
  private def watchedEntity(drained: Promise[Done], bytes: ByteString): UniversalEntity =
    HttpEntity(ContentTypes.`application/octet-stream`, bytes.length.toLong,
      Source.single(bytes).watchTermination()(Keep.right)
        .mapMaterializedValue(done => { drained.completeWith(done); Done }))

  /** A wire double: `send` answers by echoing the outgoing request's
   *  method, URI, entity, and header names back as the response body —
   *  checks assert on what the upstream would actually have seen. */
  private def echoBody(request: HttpRequest): String = {
    val entity = request.entity match {
      case strict: HttpEntity.Strict => strict.data.utf8String
      case _ => "<non-strict>"
    }
    Seq(
      s"method=${request.method.value}",
      s"uri=${request.uri}",
      s"entity=$entity",
      s"headers=${request.headers.map(_.lowercaseName()).toSeq.sorted.mkString(",")}"
    ).mkString("\n")
  }

  private def echoUpstream(request: HttpRequest): Future[HttpResponse] =
    Future.successful(HttpResponse(StatusCodes.OK,
      headers = immutable.Seq(
        `Cache-Control`(CacheDirectives.`max-age`(60)),
        RawHeader("X-Vendor-Internal", "hidden")),
      entity = HttpEntity(echoBody(request))))

  private val gzipBytes = ByteString(
    0x1f.toByte, 0x8b.toByte, 8.toByte, 0.toByte, 0.toByte, 0.toByte,
    0.toByte, 0.toByte, 0.toByte, 3.toByte, 3.toByte, 0.toByte,
    0.toByte, 0.toByte, 0.toByte, 0.toByte, 0.toByte, 0.toByte, 0.toByte, 0.toByte)

  "SubresourceReverseProxy" should {
    "map endpoints to local and upstream addresses" in {
      val p = proxy()
      p.localPath(p.endpoints.head).toString shouldBe "/vendor/script.js"
      p.upstreamUri(p.endpoints.head).toString shouldBe "https://upstream.example/script.js"
      // a trailing slash on the declared origin is normalized at wiring
      proxy(upstream = "https://upstream.example/")
        .upstreamUri(p.endpoints.head).toString shouldBe "https://upstream.example/script.js"
    }

    "expose the parsed origin and mount as typed values" in {
      val p = proxy()
      p.upstream.toString shouldBe "https://upstream.example"
      p.mount.toString shouldBe "/vendor"
      p.upstream.scheme shouldBe "https"
      p.upstream.authority.host.address shouldBe "upstream.example"
    }

    "refuse upstream values that are not bare origins" in {
      val invalid = Seq(
        "upstream.example",
        "ftp://upstream.example",
        "https://upstream.example/extra",
        "https://upstream.example/./",
        "https://upstream.example/../x",
        "https://upstream.example//",
        "https://user@upstream.example",
        "https://upstream.example?x=1",
        "https://upstream.example?",
        "https://upstream.example#frag",
        "https://upstream.example#",
        "https://upstream.example:0",
        "https://upstream.example:65536",
        "https://upstream.example:-1",
        "https://upstream.example:",
        "https://[::1]:0",
        "https://[::1]:65536",
        "https://upstréam.example")
      invalid.foreach(value =>
        SubresourceReverseProxy.create("x", value, "/m",
          Seq(endpoint("/a", Set(HttpMethods.GET))), 5.seconds)
         .swap.toOption shouldBe Some(ConfigError.InvalidOrigin(value)))
    }

    "accept origins with no port, a trailing slash, explicit ports, and IPv6" in {
      val accepted = Seq(
        "http://upstream.example",
        "https://upstream.example",
        "https://upstream.example/",
        "https://upstream.example:80",
        "https://upstream.example:443",
        "https://upstream.example:65535",
        "https://[::1]",
        "https://[::1]:8443")
      accepted.foreach { value =>
        SubresourceReverseProxy.create("x", value, "/m",
          Seq(endpoint("/a", Set(HttpMethods.GET))), 5.seconds)
          .map(_.upstream.scheme) should matchPattern { case Right("http") | Right("https") => }
      }
      SubresourceReverseProxy.create("x", "https://upstream.example/", "/m",
        Seq(endpoint("/a", Set(HttpMethods.GET))), 5.seconds)
        .map(_.upstream.toString).toOption shouldBe Some("https://upstream.example")
      val ipv6 = proxy(upstream = "https://[::1]:8443")
      ipv6.upstream.authority.host.address should include("::1")
      ipv6.upstreamUri(ipv6.endpoints.head).toString shouldBe "https://[::1]:8443/script.js"
    }

    "refuse a mount that is relative, trailing, doubled, or the root" in {
      Seq("vendor", "/vendor/", "//vendor", "/").foreach(mount =>
        SubresourceReverseProxy.create("x", "https://upstream.example", mount,
          Seq(endpoint("/a", Set(HttpMethods.GET))), 5.seconds)
         .swap.toOption shouldBe Some(ConfigError.InvalidPath(mount)))
    }

    "refuse malformed endpoint paths, empty sets, and undeclared methods" in {
      Seq("script.js", "/script%zz.js", "/a%20b.js", "/../x", "/a//b", "/x.js/", "/").foreach(path =>
        SubresourceReverseProxy.Endpoint.create(path, Set(HttpMethods.GET))
         .swap.toOption shouldBe Some(ConfigError.InvalidPath(path)))
      SubresourceReverseProxy.Endpoint.create("/x.js", Set.empty)
       .swap.toOption shouldBe Some(ConfigError.InvalidMethods(Set.empty))
      Seq(HttpMethods.TRACE, HttpMethods.CONNECT, HttpMethods.OPTIONS).foreach(m =>
        SubresourceReverseProxy.Endpoint.create("/x.js", Set(m))
         .swap.toOption shouldBe Some(ConfigError.InvalidMethods(Set(m))))
      SubresourceReverseProxy.Endpoint.create("/x.js", Set(HttpMethods.GET, HttpMethods.OPTIONS))
       .swap.toOption shouldBe Some(ConfigError.InvalidMethods(Set(HttpMethods.GET, HttpMethods.OPTIONS)))
      SubresourceReverseProxy.create("x", "https://upstream.example", "/m",
        Seq.empty, 5.seconds).swap.toOption shouldBe Some(ConfigError.EmptyEndpoints)
    }

    "refuse endpoints that shadow each other" in {
      SubresourceReverseProxy.create("x", "https://upstream.example", "/m",
        Seq(endpoint("/x.js", Set(HttpMethods.GET)), endpoint("/x.js", Set(HttpMethods.POST))),
        5.seconds) match {
        case Left(ConfigError.DuplicatePaths(paths)) =>
          paths shouldBe Set(Uri.Path("/x.js"))
        case other => fail(s"expected duplicate paths, got $other")
      }
    }

    "refuse a blank name and a non-positive timeout" in {
      SubresourceReverseProxy.create("   ", "https://upstream.example", "/m",
        Seq(endpoint("/a", Set(HttpMethods.GET))), 5.seconds)
       .swap.toOption shouldBe Some(ConfigError.EmptyName)
      Seq(Duration.Zero, (-1).seconds).foreach(d =>
        SubresourceReverseProxy.create("x", "https://upstream.example", "/m",
          Seq(endpoint("/a", Set(HttpMethods.GET))), d)
         .swap.toOption shouldBe Some(ConfigError.InvalidTimeout(d)))
    }

    "refuse credential and hop-by-hop request headers" in {
      Seq("cookie", "cookie2", "authorization", "proxy-authorization", "host", "forwarded",
          "x-forwarded-for", "x-forwarded-port", "x-real-ip", "connection", "content-length",
          "content-type", "transfer-encoding", "te", "trailer", "upgrade", "keep-alive",
          "timeout-access").foreach(name =>
        SubresourceReverseProxy.create("x", "https://upstream.example", "/m",
          Seq(endpoint("/a", Set(HttpMethods.GET))), 5.seconds, forwardHeaders = Set(name))
         .swap.toOption shouldBe Some(ConfigError.ForbiddenForwardHeaders(Set(name))))
    }

    "refuse credential and hop-by-hop response headers" in {
      Seq("set-cookie", "set-cookie2", "www-authenticate", "proxy-authenticate", "connection",
          "content-length", "content-type", "transfer-encoding", "te", "trailer", "upgrade",
          "keep-alive").foreach(name =>
        SubresourceReverseProxy.create("x", "https://upstream.example", "/m",
          Seq(endpoint("/a", Set(HttpMethods.GET))), 5.seconds, returnHeaders = Set(name))
         .swap.toOption shouldBe Some(ConfigError.ForbiddenReturnHeaders(Set(name))))
    }

    "refuse header names that are not lowercase HTTP tokens" in {
      Seq("User-Agent", "bad header", "").foreach(name =>
        SubresourceReverseProxy.create("x", "https://upstream.example", "/m",
          Seq(endpoint("/a", Set(HttpMethods.GET))), 5.seconds, forwardHeaders = Set(name))
         .swap.toOption shouldBe Some(ConfigError.InvalidHeaderNames(Set(name))))
      SubresourceReverseProxy.create("x", "https://upstream.example", "/m",
        Seq(endpoint("/a", Set(HttpMethods.GET))), 5.seconds, returnHeaders = Set("ETAG"))
       .swap.toOption shouldBe Some(ConfigError.InvalidHeaderNames(Set("ETAG")))
    }
  }

  "proxyRoute" should {
    "proxy a declared GET endpoint and return upstream status, entity, and declared headers" in {
      Get("/vendor/script.js") ~> proxyRoute(proxy(), echoUpstream) ~> check {
        status shouldBe StatusCodes.OK
        header[`Cache-Control`] shouldBe defined
        response.headers.exists(_.is("x-vendor-internal")) shouldBe false
        responseAs[String] should include("method=GET")
        responseAs[String] should include("uri=https://upstream.example/script.js")
      }
    }

    "proxy a declared POST endpoint with the request entity" in {
      Post("/vendor/api/send", HttpEntity("payload-body")) ~>
        proxyRoute(proxy(), echoUpstream) ~> check {
        status shouldBe StatusCodes.OK
        responseAs[String] should include("method=POST")
        responseAs[String] should include("uri=https://upstream.example/api/send")
        responseAs[String] should include("entity=payload-body")
      }
    }

    "answer a multi-method endpoint with each declared method" in {
      val p = proxy(endpoints = Seq(endpoint("/multi.js", Set(HttpMethods.GET, HttpMethods.HEAD))))
      val route = proxyRoute(p, echoUpstream)
      Get("/vendor/multi.js") ~> route ~> check {
        status shouldBe StatusCodes.OK
        responseAs[String] should include("method=GET")
      }
      Head("/vendor/multi.js") ~> route ~> check {
        status shouldBe StatusCodes.OK
      }
    }

    "never let inbound input reach the upstream URL" in {
      Get("/vendor/script.js?v=2&x=%2F") ~> proxyRoute(proxy(), echoUpstream) ~> check {
        responseAs[String] should include("uri=https://upstream.example/script.js")
        responseAs[String] should not include "v=2"
      }
    }

    "forward declared request headers and nothing else" in {
      Get("/vendor/script.js") ~>
        `User-Agent`("test-agent") ~>
        Cookie("session", "first-party-secret") ~>
        RawHeader("x-vendor-opt", "not-declared") ~>
        proxyRoute(proxy(), echoUpstream) ~> check {
        val echoed = responseAs[String].linesIterator
          .find(_.startsWith("headers=")).getOrElse("")
        echoed should include("user-agent")
        echoed should not include "cookie"
        echoed should not include "x-vendor-opt"
        echoed should not include "host"
      }
    }

    "answer 502 when the upstream call fails" in {
      Get("/vendor/script.js") ~>
        proxyRoute(proxy(), _ => Future.failed(new RuntimeException("connection refused"))) ~> check {
        status shouldBe StatusCodes.BadGateway
      }
    }

    "answer 502 when the send function throws synchronously" in {
      Get("/vendor/script.js") ~>
        proxyRoute(proxy(), _ => throw new RuntimeException("no wire")) ~> check {
        status shouldBe StatusCodes.BadGateway
      }
    }

    "answer 504 when upstream outlives the declared timeout" in {
      Get("/vendor/script.js") ~>
        proxyRoute(proxy(timeout = 150.milliseconds), _ => Promise[HttpResponse]().future) ~> check {
        status shouldBe StatusCodes.GatewayTimeout
      }
    }

    "drain a late upstream reply after the deadline" in {
      val upstream = Promise[HttpResponse]()
      val drained = Promise[Done]()
      Get("/vendor/script.js") ~>
        proxyRoute(proxy(timeout = 150.milliseconds), _ => upstream.future) ~> check {
        status shouldBe StatusCodes.GatewayTimeout
      }
      upstream.success(HttpResponse(entity = watchedEntity(drained, ByteString("late"))))
      drained.future.futureValue shouldBe Done
    }

    "pass a streamed success entity through once and unchanged" in {
      val materializations = TestProbe[Done]()
      val entity = HttpEntity(ContentTypes.`application/octet-stream`, 5L,
        Source(List(ByteString("he"), ByteString("llo")))
          .mapMaterializedValue(_ => { materializations.ref ! Done; NotUsed }))
      Get("/vendor/script.js") ~>
        proxyRoute(proxy(), _ => Future.successful(HttpResponse(entity = entity))) ~> check {
        status shouldBe StatusCodes.OK
        responseAs[String] shouldBe "hello"
      }
      materializations.receiveMessage() shouldBe Done
      materializations.expectNoMessage(100.milliseconds)
    }

    "leave undeclared paths and undeclared methods alone" in {
      val route = proxyRoute(proxy(), echoUpstream)
      Seq("/vendor/other.js", "/vendor/script.js/", "/vendor/script.js/extra",
          "/vendor//script.js", "/vendor").foreach(uri =>
        Get(uri) ~> route ~> check {
          handled shouldBe false
        })
      // a decoded slash inside a segment cannot widen the exact match
      Post("/vendor/api%2Fsend", HttpEntity("x")) ~> route ~> check {
        handled shouldBe false
      }
      Post("/vendor/script.js", HttpEntity("x")) ~> Route.seal(route) ~> check {
        status shouldBe StatusCodes.MethodNotAllowed
      }
    }

    "bind a whole proxy set at once" in {
      val other = proxy(name = "fonts", upstream = "https://fonts.example", mount = "/fonts",
        endpoints = Seq(endpoint("/list.css", Set(HttpMethods.GET))))
      Get("/fonts/list.css") ~> validRoutes(Seq(proxy(), other), echoUpstream) ~> check {
        status shouldBe StatusCodes.OK
        responseAs[String] should include("uri=https://fonts.example/list.css")
      }
    }

    "refuse proxies whose paths would shadow each other" in {
      val dupe = proxy(name = "dupe", upstream = "https://other.example")
      proxyRoutes(Seq(proxy(), dupe), echoUpstream) match {
        case Left(ConfigError.DuplicatePaths(paths)) =>
          paths.map(_.toString) shouldBe Set("/vendor/script.js", "/vendor/api/send")
        case other => fail(s"expected duplicate paths, got $other")
      }
    }

    "reject every request for an empty proxy set" in {
      val route = validRoutes(Seq.empty, echoUpstream)
      Get("/vendor/script.js") ~> route ~> check {
        handled shouldBe false
      }
    }

    "build routes through the documented wiring shape" in {
      val configuredTimeout: FiniteDuration = 5.seconds
      val wired =
        for
          script <- SubresourceReverseProxy.Endpoint.create("/script.js", Set(HttpMethods.GET))
          beacon <- SubresourceReverseProxy.Endpoint.create("/api/send", Set(HttpMethods.POST))
          p <- SubresourceReverseProxy.create(
            name = "analytics",
            upstream = "https://upstream.example",
            mount = "/vendor",
            endpoints = Seq(script, beacon),
            timeout = configuredTimeout)
          route <- SubresourceReverseProxyDirectives.proxyRoutes(Seq(p))
        yield route
      wired.isRight shouldBe true
    }

    "forward an encoded body through the native client without leaking credentials" in {
      val seen = Promise[(HttpRequest, ByteString)]()
      val upstreamRoute: Route = extractRequest { request =>
        onSuccess(request.entity.dataBytes.runFold(ByteString.empty)(_ ++ _)) { bytes =>
          seen.success(request -> bytes)
          complete(HttpResponse(
            headers = List(
              `Content-Encoding`(HttpEncodings.gzip),
              `Cache-Control`(CacheDirectives.`no-store`),
              RawHeader("Vary", "Accept-Language"),
              RawHeader("Set-Cookie", "vendor=secret")),
            entity = HttpEntity(ContentTypes.`application/octet-stream`, bytes)))
        }
      }
      val binding = Http().newServerAt("127.0.0.1", 0).bind(upstreamRoute).futureValue
      try {
        val p = proxy(upstream = s"http://127.0.0.1:${binding.localAddress.getPort}")
        Post("/vendor/api/send?ignored=1", HttpEntity(gzipBytes)).withHeaders(
          `Content-Encoding`(HttpEncodings.gzip),
          Cookie("session", "first-party"),
          RawHeader("Authorization", "Bearer first-party")) ~> proxyRoute(p) ~> check {
          status shouldBe StatusCodes.OK
          responseAs[ByteString] shouldBe gzipBytes
          response.headers.find(_.is("content-encoding")).map(_.value()) shouldBe Some("gzip")
          response.headers.find(_.is("cache-control")).map(_.value()) shouldBe Some("no-store")
          response.headers.find(_.is("vary")).map(_.value()) shouldBe Some("Accept-Language")
          response.headers.exists(_.is("set-cookie")) shouldBe false
        }
        val (request, bytes) = seen.future.futureValue
        request.uri.path.toString shouldBe "/api/send"
        request.uri.rawQueryString shouldBe None
        request.uri.authority.host.address shouldBe "127.0.0.1"
        request.uri.authority.port shouldBe binding.localAddress.getPort
        request.headers.exists(h => h.is("cookie") || h.is("authorization")) shouldBe false
        request.headers.find(_.is("content-encoding")).map(_.value()) shouldBe Some("gzip")
        bytes shouldBe gzipBytes
      } finally {
        binding.terminate(2.seconds).futureValue
      }
    }
  }

  "proxy protocol metadata" should {
    "retain Vary when retaining upstream cacheability" in {
      val route = proxyRoute(proxy(), _ => Future.successful(HttpResponse(
        headers = List(RawHeader("Cache-Control", "public, max-age=60"),
          RawHeader("Vary", "Accept-Language")),
        entity = "localized response")))
      Get("/vendor/script.js") ~> route ~> check {
        response.headers.find(_.is("cache-control")).map(_.value()) shouldBe Some("public, max-age=60")
        response.headers.find(_.is("vary")).map(_.value()) shouldBe Some("Accept-Language")
      }
    }

    "carry Content-Encoding when forwarding an encoded request body unchanged" in {
      val seen = Promise[HttpRequest]()
      val route = proxyRoute(proxy(), request => {
        seen.success(request)
        Future.successful(HttpResponse(entity = "ok"))
      })
      Post("/vendor/api/send", HttpEntity(gzipBytes))
        .withHeaders(RawHeader("Content-Encoding", "gzip")) ~> route ~> check {
        status shouldBe StatusCodes.OK
      }
      val out = seen.future.futureValue
      out.headers.find(_.is("content-encoding")).map(_.value()) shouldBe Some("gzip")
      out.entity match {
        case strict: HttpEntity.Strict => strict.data shouldBe gzipBytes
        case other => fail(s"request entity should stay strict, got $other")
      }
    }

    "pass encoded response bodies through byte-identically" in {
      val route = proxyRoute(proxy(), _ => Future.successful(HttpResponse(
        headers = List(RawHeader("Content-Encoding", "gzip")),
        entity = HttpEntity(gzipBytes))))
      Get("/vendor/script.js") ~> route ~> check {
        status shouldBe StatusCodes.OK
        response.headers.find(_.is("content-encoding")).map(_.value()) shouldBe Some("gzip")
        response.entity match {
          case strict: HttpEntity.Strict => strict.data shouldBe gzipBytes
          case other => fail(s"response entity should stay strict, got $other")
        }
      }
    }

    "retain response cache metadata even with an empty return allowlist" in {
      val route = proxyRoute(proxy(returnHeaders = Set.empty), _ => Future.successful(HttpResponse(
        headers = List(
          RawHeader("Cache-Control", "no-store"),
          RawHeader("Vary", "*"),
          RawHeader("Age", "7"),
          RawHeader("Date", "Wed, 14 Oct 2026 07:28:00 GMT"),
          RawHeader("Expires", "Wed, 21 Oct 2026 07:28:00 GMT"),
          RawHeader("X-Vendor-Internal", "hidden"),
          RawHeader("Set-Cookie", "session=vendor")),
        entity = "cached")))
      Get("/vendor/script.js") ~> route ~> check {
        response.headers.find(_.is("cache-control")).map(_.value()) shouldBe Some("no-store")
        response.headers.find(_.is("vary")).map(_.value()) shouldBe Some("*")
        response.headers.find(_.is("age")).map(_.value()) shouldBe Some("7")
        response.headers.find(_.is("date")).map(_.value()) shouldBe
          Some("Wed, 14 Oct 2026 07:28:00 GMT")
        response.headers.find(_.is("expires")).map(_.value()) shouldBe
          Some("Wed, 21 Oct 2026 07:28:00 GMT")
        response.headers.exists(_.is("x-vendor-internal")) shouldBe false
        response.headers.exists(_.is("set-cookie")) shouldBe false
      }
    }

    "remove request headers named by Connection even when allowlisted" in {
      val seen = Promise[HttpRequest]()
      val route = proxyRoute(proxy(), request => {
        seen.success(request)
        Future.successful(HttpResponse(entity = "ok"))
      })
      Get("/vendor/script.js")
        .withHeaders(Connection("user-agent"), `User-Agent`("review-client")) ~> route ~> check {
        status shouldBe StatusCodes.OK
      }
      seen.future.futureValue.headers.exists(_.is("user-agent")) shouldBe false
    }

    "remove request headers named by multiple Connection fields and mixed-case tokens" in {
      val seen = Promise[HttpRequest]()
      val route = proxyRoute(proxy(), request => {
        seen.success(request)
        Future.successful(HttpResponse(entity = "ok"))
      })
      Get("/vendor/script.js").withHeaders(
        `User-Agent`("mixed-case"),
        RawHeader("Referer", "https://site.example/page"),
        RawHeader("Connection", "User-AGENT"),
        RawHeader("Connection", " referer , x-unused")) ~> route ~> check {
        status shouldBe StatusCodes.OK
      }
      val forwarded = seen.future.futureValue.headers.map(_.lowercaseName())
      forwarded should not contain "user-agent"
      forwarded should not contain "referer"
    }

    "remove response headers named by Connection" in {
      val route = proxyRoute(proxy(returnHeaders = Set("x-vendor-note")), _ =>
        Future.successful(HttpResponse(
          headers = List(
            Connection("X-Vendor-Note"),
            RawHeader("x-vendor-note", "internal"),
            RawHeader("Cache-Control", "public"),
            RawHeader("Vary", "Accept")),
          entity = "ok")))
      Get("/vendor/script.js") ~> route ~> check {
        status shouldBe StatusCodes.OK
        response.headers.exists(_.is("x-vendor-note")) shouldBe false
        response.headers.find(_.is("cache-control")).map(_.value()) shouldBe Some("public")
        response.headers.find(_.is("vary")).map(_.value()) shouldBe Some("Accept")
      }
    }

    "refuse a request whose Connection nominates protected entity metadata" in {
      Seq("content-encoding", "content-type", "content-length").foreach { nominated =>
        Get("/vendor/script.js").withHeaders(Connection(nominated)) ~>
          proxyRoute(proxy(), _ => fail("upstream must not be called")) ~> check {
          status shouldBe StatusCodes.BadRequest
        }
      }
      val drained = Promise[Done]()
      Post("/vendor/api/send").withEntity(watchedEntity(drained, ByteString("payload")))
        .withHeaders(Connection("content-type")) ~>
        proxyRoute(proxy(), _ => fail("upstream must not be called")) ~> check {
        status shouldBe StatusCodes.BadRequest
      }
      drained.future.futureValue shouldBe Done
    }

    "refuse an upstream response whose Connection nominates protected cache metadata" in {
      val drained = Promise[Done]()
      val route = proxyRoute(proxy(), _ => Future.successful(HttpResponse(
        headers = List(Connection("cache-control"), RawHeader("Cache-Control", "no-cache")),
        entity = watchedEntity(drained, ByteString("untrusted")))))
      Get("/vendor/script.js") ~> route ~> check {
        status shouldBe StatusCodes.BadGateway
      }
      drained.future.futureValue shouldBe Done
    }

    "strip chunk extensions and trailers from a chunked request entity" in {
      val seen = Promise[HttpRequest]()
      val parts: immutable.Seq[HttpEntity.ChunkStreamPart] = immutable.Seq(
        HttpEntity.Chunk(ByteString("abc"), "ttl=60"),
        HttpEntity.Chunk(ByteString("de"), ""),
        HttpEntity.LastChunk("sig=x", immutable.Seq(RawHeader("Cookie", "secret"))))
      Post("/vendor/api/send")
        .withEntity(HttpEntity.Chunked(ContentTypes.`application/octet-stream`, Source(parts))) ~>
        proxyRoute(proxy(), request => {
          seen.success(request)
          Future.successful(HttpResponse(entity = "ok"))
        }) ~> check {
        status shouldBe StatusCodes.OK
      }
      val forwarded = seen.future.futureValue.entity match {
        case chunked: HttpEntity.Chunked => chunked.chunks.runWith(Sink.seq).futureValue
        case other => fail(s"request entity should remain chunked, got $other")
      }
      val chunks = forwarded.collect { case c: HttpEntity.Chunk => c }
      chunks.map(_.data).foldLeft(ByteString.empty)(_ ++ _) shouldBe ByteString("abcde")
      chunks.map(_.extension) shouldBe Seq("", "")
      val trailers = forwarded.collect { case l: HttpEntity.LastChunk => l }
      trailers should have size 1
      trailers.foreach { last =>
        last.extension shouldBe ""
        last.trailer shouldBe empty
      }
    }

    "strip chunk extensions and trailers from a chunked upstream response" in {
      val parts: immutable.Seq[HttpEntity.ChunkStreamPart] = immutable.Seq(
        HttpEntity.Chunk(ByteString("ven"), "q=0.9"),
        HttpEntity.LastChunk("hash=y", immutable.Seq(RawHeader("Set-Cookie", "id=vendor"))))
      val route = proxyRoute(proxy(), _ => Future.successful(HttpResponse(
        entity = HttpEntity.Chunked(ContentTypes.`application/octet-stream`, Source(parts)))))
      Get("/vendor/script.js") ~> route ~> check {
        status shouldBe StatusCodes.OK
        val received = response.entity match {
          case chunked: HttpEntity.Chunked => chunked.chunks.runWith(Sink.seq).futureValue
          case other => fail(s"response entity should remain chunked, got $other")
        }
        val chunks = received.collect { case c: HttpEntity.Chunk => c }
        chunks.map(_.data).foldLeft(ByteString.empty)(_ ++ _) shouldBe ByteString("ven")
        chunks.map(_.extension) shouldBe Seq("")
        val trailers = received.collect { case l: HttpEntity.LastChunk => l }
        trailers should have size 1
        trailers.foreach { last =>
          last.extension shouldBe ""
          last.trailer shouldBe empty
        }
      }
    }
  }
}
