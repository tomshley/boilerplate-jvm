package com.tomshley.boilerplate.jvm.staticassets

import com.tomshley.boilerplate.jvm.staticassets.StaticAssetCaching.{CachePolicy, immutableWhere, revalidate}
import com.tomshley.boilerplate.jvm.staticassets.StaticAssetDirectives.{cacheControlled, staticAssetEntity, staticAssets, staticAssetsFrom}
import com.tomshley.boilerplate.jvm.staticassets.exceptions.StaticAssetRoutingRejection
import org.apache.pekko.http.scaladsl.model.{ContentTypes, HttpEntity, StatusCodes, Uri}
import org.apache.pekko.http.scaladsl.model.headers.{CacheDirectives, ETag, EntityTag, EntityTagRange, RawHeader, `Cache-Control`, `If-Match`, `If-None-Match`}
import org.apache.pekko.http.scaladsl.server.Directives.{complete, conditional, mapUnmatchedPath, respondWithHeader, withMaterializer}
import org.apache.pekko.http.scaladsl.testkit.ScalatestRouteTest
import org.apache.pekko.stream.Materializer
import org.apache.pekko.stream.scaladsl.Source
import org.apache.pekko.util.ByteString
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import java.io.{ByteArrayInputStream, InputStream}
import scala.concurrent.Promise

final class StaticAssetRoutingSpec extends AnyWordSpec with Matchers with ScalatestRouteTest with ScalaFutures {

  import org.apache.pekko.actor.typed.scaladsl.adapter.*

  private object UnderTest extends StaticAssetRouting

  private val routes = UnderTest.getStaticAssetRoute(system.toTyped)

  // A caller-chosen policy: directories named for a content hash are
  // immutable and everything else revalidates. Fixtures under
  // src/test/resources are synthetic and stable.
  private val cachePolicy: CachePolicy = immutableWhere("[0-9a-f]{8}".r) orElse revalidate

  private val cachedRoutes = UnderTest.getStaticAssetRoute(system.toTyped, cachePolicy)

  // Built once: the directive value owns the index actor.
  private val composed = staticAssets(system.toTyped) { (path, asset) =>
    (cacheControlled(cachePolicy)(path) & conditional(asset.etag) & respondWithHeader(RawHeader("x-added-by-caller", "1"))) {
      complete(staticAssetEntity(asset))
    }
  }

  private def assetEtag(path: String): EntityTag =
    Get(path) ~> cachedRoutes ~> check {
      response.header[ETag].getOrElse(fail("response must carry an etag")).etag
    }

  "StaticAssetRouting" should {
    "serve a JS asset with a javascript content type" in {
      Get("/static/static-asset-routing-fixture.js") ~> routes ~> check {
        status shouldBe StatusCodes.OK
        contentType.mediaType.value shouldBe "text/javascript"
        responseAs[String].trim should include("console.log")
      }
    }

    "serve a CSS asset with a css content type" in {
      Get("/static/static-asset-routing-fixture.css") ~> routes ~> check {
        status shouldBe StatusCodes.OK
        contentType.mediaType.value shouldBe "text/css"
        responseAs[String].trim should include("body")
      }
    }

    "send a charset parameter only for text media types" in {
      Get("/static/static-asset-routing/fixture.json") ~> routes ~> check {
        status shouldBe StatusCodes.OK
        contentType.value shouldBe "application/json"
      }
      Get("/static/static-asset-routing-fixture.css") ~> routes ~> check {
        status shouldBe StatusCodes.OK
        contentType.value shouldBe "text/css; charset=UTF-8"
      }
    }

    "send no etag or cache-control on the default route" in {
      Get("/static/static-asset-routing-fixture.css") ~> routes ~> check {
        status shouldBe StatusCodes.OK
        response.header[ETag] shouldBe None
        response.header[`Cache-Control`] shouldBe None
      }
    }

    "reject unsupported extensions" in {
      Get("/static/static-asset-routing-fixture.xyz") ~> routes ~> check {
        rejections.length should be > 0
        rejections.exists(_.isInstanceOf[StaticAssetRoutingRejection]) shouldBe true
      }
    }

    "reject when the resource does not exist" in {
      Get("/static/does-not-exist.js") ~> routes ~> check {
        rejections.exists(_.isInstanceOf[StaticAssetRoutingRejection]) shouldBe true
      }
    }
  }

  "StaticAssetRouting on an empty file" should {
    "answer 200 with an empty body on both routes" in {
      Get("/static/static-asset-routing/empty.css") ~> routes ~> check {
        status shouldBe StatusCodes.OK
        responseAs[String] shouldBe ""
        contentType.mediaType.value shouldBe "text/css"
      }
      Get("/static/static-asset-routing/empty.css") ~> cachedRoutes ~> check {
        status shouldBe StatusCodes.OK
        responseAs[String] shouldBe ""
        contentType.mediaType.value shouldBe "text/css"
        response.header[ETag].map(_.etag.tag) should contain("e3b0c44298fc1c14")
      }
    }
  }

  "StaticAssetRouting with a cache policy" should {
    "serve a 200 with a strong content etag and the policy cache-control" in {
      Get("/static/static-asset-routing/0a1b2c3d/fixture.css") ~> cachedRoutes ~> check {
        status shouldBe StatusCodes.OK
        contentType.mediaType.value shouldBe "text/css"
        responseAs[String].trim should include("body")
        val tag = response.header[ETag].getOrElse(fail("response must carry an etag")).etag
        tag.tag should fullyMatch regex "[0-9a-f]{16}"
        tag.weak shouldBe false
        response.header[`Cache-Control`].map(_.value) should contain("public, max-age=31536000, immutable")
        val fixtureLength =
          getClass.getClassLoader.getResourceAsStream("static-asset-routing/0a1b2c3d/fixture.css").readAllBytes().length.toLong
        response.entity.contentLengthOption shouldBe Some(fixtureLength)
      }
    }

    "produce the same etag for the same bytes on repeat requests" in {
      val first = assetEtag("/static/static-asset-routing/0a1b2c3d/fixture.css")
      assetEtag("/static/static-asset-routing/0a1b2c3d/fixture.css") shouldBe first
    }

    "apply the policy per path and omit the header where the policy is undefined" in {
      Get("/static/static-asset-routing-fixture.css") ~> cachedRoutes ~> check {
        status shouldBe StatusCodes.OK
        response.header[ETag].isDefined shouldBe true
        response.header[`Cache-Control`].map(_.value) should contain("no-cache")
      }
      val immutableOnly = UnderTest.getStaticAssetRoute(system.toTyped, immutableWhere("[0-9a-f]{8}".r))
      Get("/static/static-asset-routing-fixture.js") ~> immutableOnly ~> check {
        status shouldBe StatusCodes.OK
        response.header[ETag].isDefined shouldBe true
        response.header[`Cache-Control`] shouldBe None
      }
    }

    "answer every path through the named policies" in {
      val immutableRoutes = UnderTest.getStaticAssetRoute(system.toTyped, StaticAssetCaching.immutable)
      Get("/static/static-asset-routing-fixture.js") ~> immutableRoutes ~> check {
        status shouldBe StatusCodes.OK
        response.header[`Cache-Control`].map(_.value) should contain("public, max-age=31536000, immutable")
      }
      val revalidateRoutes = UnderTest.getStaticAssetRoute(system.toTyped, StaticAssetCaching.revalidate)
      Get("/static/static-asset-routing-fixture.js") ~> revalidateRoutes ~> check {
        status shouldBe StatusCodes.OK
        response.header[`Cache-Control`].map(_.value) should contain("no-cache")
      }
    }

    "match immutableWhere on a whole path segment only" in {
      val hashedOnly = UnderTest.getStaticAssetRoute(system.toTyped, immutableWhere("[0-9a-f]{8}".r))
      Get("/static/static-asset-routing/0a1b2c3d/fixture.css") ~> hashedOnly ~> check {
        response.header[`Cache-Control`].map(_.value) should contain("public, max-age=31536000, immutable")
      }
      Get("/static/static-asset-routing/x0a1b2c3d/fixture.css") ~> hashedOnly ~> check {
        status shouldBe StatusCodes.OK
        response.header[`Cache-Control`] shouldBe None
      }
    }

    "compose the public directives into an equivalent route" in {
      val tag = Get("/static/static-asset-routing/0a1b2c3d/fixture.css") ~> composed ~> check {
        status shouldBe StatusCodes.OK
        response.headers.exists(_.is("x-added-by-caller")) shouldBe true
        response.header[`Cache-Control`].map(_.value) should contain("public, max-age=31536000, immutable")
        response.header[ETag].getOrElse(fail("response must carry an etag")).etag
      }
      Get("/static/static-asset-routing/0a1b2c3d/fixture.css") ~> `If-None-Match`(EntityTagRange(tag)) ~> composed ~> check {
        status shouldBe StatusCodes.NotModified
      }
    }

    "answer 304 with validators and no body on a matching If-None-Match" in {
      val path = "/static/static-asset-routing/0a1b2c3d/fixture.css"
      val tag = assetEtag(path)
      Get(path) ~> `If-None-Match`(EntityTagRange(tag)) ~> cachedRoutes ~> check {
        status shouldBe StatusCodes.NotModified
        responseAs[String] shouldBe ""
        response.header[ETag].map(_.etag) should contain(tag)
        response.header[`Cache-Control`].map(_.value) should contain("public, max-age=31536000, immutable")
      }
    }

    "answer the full asset on a non-matching If-None-Match" in {
      Get("/static/static-asset-routing/0a1b2c3d/fixture.css") ~>
        `If-None-Match`(EntityTagRange(EntityTag("0000000000000000"))) ~> cachedRoutes ~> check {
        status shouldBe StatusCodes.OK
        responseAs[String].trim should include("body")
      }
    }

    "match a weak etag and the star range" in {
      val path = "/static/static-asset-routing/0a1b2c3d/fixture.css"
      val tag = assetEtag(path)
      Get(path) ~> `If-None-Match`(EntityTagRange(EntityTag(tag.tag, weak = true))) ~> cachedRoutes ~> check {
        status shouldBe StatusCodes.NotModified
      }
      Get(path) ~> `If-None-Match`(EntityTagRange.*) ~> cachedRoutes ~> check {
        status shouldBe StatusCodes.NotModified
      }
    }

    "keep rejections unchanged and never consult the policy for them" in {
      val unconsulted: CachePolicy = { case _ => fail("cache policy consulted for an asset that was not served") }
      val guardedRoutes = UnderTest.getStaticAssetRoute(system.toTyped, unconsulted)
      Get("/static/static-asset-routing-fixture.xyz") ~> guardedRoutes ~> check {
        rejections.exists(_.isInstanceOf[StaticAssetRoutingRejection]) shouldBe true
      }
      Get("/static/does-not-exist.js") ~> guardedRoutes ~> check {
        rejections.exists(_.isInstanceOf[StaticAssetRoutingRejection]) shouldBe true
      }
    }
  }

  "StaticAssetRouting conditional requests" should {
    "answer 200 on a matching If-Match and on the star range" in {
      val path = "/static/static-asset-routing/0a1b2c3d/fixture.css"
      val tag = assetEtag(path)
      Get(path) ~> `If-Match`(EntityTagRange(tag)) ~> cachedRoutes ~> check {
        status shouldBe StatusCodes.OK
        responseAs[String].trim should include("body")
      }
      Get(path) ~> `If-Match`(EntityTagRange.*) ~> cachedRoutes ~> check {
        status shouldBe StatusCodes.OK
      }
    }

    "answer 412 without a cache header when If-Match fails" in {
      val path = "/static/static-asset-routing/0a1b2c3d/fixture.css"
      val tag = assetEtag(path)
      Get(path) ~> `If-Match`(EntityTagRange(EntityTag("0000000000000000"))) ~> cachedRoutes ~> check {
        status shouldBe StatusCodes.PreconditionFailed
        response.header[`Cache-Control`] shouldBe None
      }
      Get(path) ~> `If-Match`(EntityTagRange(EntityTag(tag.tag, weak = true))) ~> cachedRoutes ~> check {
        status shouldBe StatusCodes.PreconditionFailed
        response.header[`Cache-Control`] shouldBe None
      }
    }

    "let a failing If-Match win over a matching If-None-Match" in {
      val path = "/static/static-asset-routing/0a1b2c3d/fixture.css"
      val tag = assetEtag(path)
      Get(path) ~> `If-Match`(EntityTagRange(EntityTag("0000000000000000"))) ~>
        `If-None-Match`(EntityTagRange(tag)) ~> cachedRoutes ~> check {
        status shouldBe StatusCodes.PreconditionFailed
      }
    }

    "ignore conditional headers on the route without validators" in {
      Get("/static/static-asset-routing-fixture.css") ~> `If-None-Match`(EntityTagRange.*) ~> routes ~> check {
        status shouldBe StatusCodes.OK
        response.header[ETag] shouldBe None
        response.header[`Cache-Control`] shouldBe None
      }
      Get("/static/static-asset-routing-fixture.css") ~>
        `If-Match`(EntityTagRange(EntityTag("0000000000000000"))) ~> routes ~> check {
        status shouldBe StatusCodes.OK
      }
    }
  }

  "StaticAssetIndex.digest" should {
    def digestOf(source: Source[ByteString, ?]): StaticAssetIndex.AssetInfo =
      StaticAssetIndex.digest(source).futureValue

    "tag empty content with the empty SHA-256" in {
      val info = digestOf(Source.empty[ByteString])
      info.etag.tag shouldBe "e3b0c44298fc1c14"
      info.length shouldBe 0L
    }

    "give distinct contents distinct tags" in {
      digestOf(Source.single(ByteString("fixture-a"))).etag should not be
        digestOf(Source.single(ByteString("fixture-b"))).etag
    }

    "give the same bytes the same tag and length however the source is chunked" in {
      val info = digestOf(Source(List(ByteString("fixture-"), ByteString("a"), ByteString("1"))))
      info shouldBe digestOf(Source.single(ByteString("fixture-a1")))
      info.length shouldBe 10L
    }
  }

  "StaticAssetDirectives.cacheControlled" should {
    "leave server errors untouched" in {
      val errorRoute = cacheControlled(StaticAssetCaching.immutable)(Uri.Path("/fixture.css")) {
        complete(StatusCodes.InternalServerError)
      }
      Get("/") ~> errorRoute ~> check {
        status shouldBe StatusCodes.InternalServerError
        response.header[`Cache-Control`] shouldBe None
      }
    }

    "not duplicate a Cache-Control the route sets itself" in {
      val tag = EntityTag("fixturetag")
      val noStore = `Cache-Control`(CacheDirectives.`no-store`)
      val route = cacheControlled(StaticAssetCaching.immutable)(Uri.Path("fixture.css")) {
        respondWithHeader(noStore) {
          conditional(tag) {
            complete(HttpEntity(ContentTypes.`text/plain(UTF-8)`, ByteString("cache fixture")))
          }
        }
      }
      Get("/") ~> route ~> check {
        status shouldBe StatusCodes.OK
        response.headers.filter(_.is("cache-control")) shouldBe List(noStore)
      }
      Get("/") ~> `If-None-Match`(EntityTagRange(tag)) ~> route ~> check {
        status shouldBe StatusCodes.NotModified
        response.headers.filter(_.is("cache-control")) shouldBe List(noStore)
      }
    }
  }

  "StaticAssetCaching.immutableWhere" should {
    "match decoded unicode and space segments" in {
      immutableWhere("café".r).isDefinedAt(Uri.Path("caf%C3%A9/fixture.css")) shouldBe true
      immutableWhere("a b".r).isDefinedAt(Uri.Path("a%20b/fixture.css")) shouldBe true
    }

    "keep an encoded slash inside its segment" in {
      immutableWhere("a/b".r).isDefinedAt(Uri.Path("a%2Fb/fixture.css")) shouldBe true
      immutableWhere("a".r).isDefinedAt(Uri.Path("a%2Fb/fixture.css")) shouldBe false
    }

    "never match an empty segment" in {
      immutableWhere("".r).isDefinedAt(Uri.Path.Empty) shouldBe false
      immutableWhere("".r).isDefinedAt(Uri.Path("//")) shouldBe false
    }

    "keep orElse precedence" in {
      val policy = immutableWhere("[0-9a-f]{8}".r) orElse revalidate
      policy.lift(Uri.Path("/x/0a1b2c3d/f.css")).map(_.value) should contain("public, max-age=31536000, immutable")
      policy.lift(Uri.Path("/x/y/f.css")).map(_.value) should contain("no-cache")
    }
  }

  "StaticAssetDirectives.staticAssetsFrom" should {
    "reject dot segments before consulting the class loader" in {
      val loader = new ClassLoader(null) {
        override def getResourceAsStream(name: String): InputStream = null
      }
      val route = staticAssetsFrom(system.toTyped, loader) { (_, asset) =>
        complete(staticAssetEntity(asset))
      }
      // URI parsing collapses dot segments before routing, so the guard is
      // defense in depth for programmatically built paths — exercised here
      // through `mapUnmatchedPath`, which swaps the unmatched path after
      // parsing.
      for (rawPath <- List(
          Uri.Path("/static/../synthetic.css"),
          Uri.Path("/static/./synthetic.css"),
          Uri.Path("/static/%2e%2e/synthetic.css"))) {
        Get("/") ~> mapUnmatchedPath(_ => rawPath)(route) ~> check {
          rejections should contain(
            StaticAssetRoutingRejection("An unsupported asset path specified"))
        }
      }
    }

    "serve a resource only the supplied class loader can see" in {
      val payload = Array[Byte](0x0a, 0x1b, 0x2c, 0x3d, 0xc3.toByte, 0xa9.toByte)
      val onlyHere = new ClassLoader(null) {
        override def getResourceAsStream(name: String): InputStream =
          if (name == "synthetic.css") new ByteArrayInputStream(payload) else null
      }
      val customRoute = staticAssetsFrom(system.toTyped, onlyHere) { (_, asset) =>
        complete(staticAssetEntity(asset))
      }
      Get("/static/synthetic.css") ~> customRoute ~> check {
        status shouldBe StatusCodes.OK
        contentType.mediaType.value shouldBe "text/css"
        response.entity.dataBytes.runFold(ByteString.empty)(_ ++ _).futureValue shouldBe ByteString(payload)
      }
      val defaultRoute = staticAssets(system.toTyped) { (_, asset) =>
        complete(staticAssetEntity(asset))
      }
      Get("/static/synthetic.css") ~> defaultRoute ~> check {
        rejections.exists(_.isInstanceOf[StaticAssetRoutingRejection]) shouldBe true
      }
    }

    "reject when the lookup or the read fails, closing the stream and keeping the cause" in {
      val unconsulted: CachePolicy = { case _ => fail("cache policy consulted after a failed read") }
      val lookupError = new IllegalStateException("synthetic lookup failure")
      val throwingLookup = new ClassLoader(null) {
        override def getResourceAsStream(name: String): InputStream = throw lookupError
      }
      val lookupRoute = staticAssetsFrom(system.toTyped, throwingLookup) { (path, asset) =>
        (cacheControlled(unconsulted)(path) & conditional(asset.etag)) {
          complete(staticAssetEntity(asset))
        }
      }
      Get("/static/synthetic.css") ~> lookupRoute ~> check {
        rejections.collectFirst { case r: StaticAssetRoutingRejection => r.cause } shouldBe Some(Some(lookupError))
      }

      val readClosed = Promise[Unit]()
      val readError = new IllegalStateException("synthetic read failure")
      val readFailLoader = new ClassLoader(null) {
        override def getResourceAsStream(name: String): InputStream =
          new InputStream {
            override def read(): Int = throw readError
            override def close(): Unit = { readClosed.trySuccess(()); () }
          }
      }
      val readRoute = staticAssetsFrom(system.toTyped, readFailLoader) { (path, asset) =>
        (cacheControlled(unconsulted)(path) & conditional(asset.etag)) {
          complete(staticAssetEntity(asset))
        }
      }
      Get("/static/synthetic.css") ~> readRoute ~> check {
        rejections.collectFirst { case r: StaticAssetRoutingRejection => r.cause } shouldBe Some(Some(readError))
      }
      readClosed.future.futureValue
    }

    "close the stream after a successful read" in {
      val closed = Promise[Unit]()
      val payload = Array[Byte](1, 2, 3)
      val loader = new ClassLoader(null) {
        override def getResourceAsStream(name: String): InputStream =
          new ByteArrayInputStream(payload) {
            override def close(): Unit = { closed.trySuccess(()); () }
          }
      }
      val route = staticAssetsFrom(system.toTyped, loader) { (_, asset) =>
        complete(staticAssetEntity(asset))
      }
      Get("/static/synthetic.css") ~> route ~> check {
        status shouldBe StatusCodes.OK
        response.entity.dataBytes.runFold(ByteString.empty)(_ ++ _).futureValue shouldBe ByteString(payload)
      }
      closed.future.futureValue
    }

    "read and digest a path once, then serve from the index" in {
      val payload = Array[Byte](7, 8, 9)
      // Requests are sequential, so an Iterator is a deterministic open seam:
      // the first request opens twice (digest, then body), the second once,
      // and a matching If-None-Match never opens.
      val opens = Iterator.fill(3)(new ByteArrayInputStream(payload): InputStream)
      val loader = new ClassLoader(null) {
        override def getResourceAsStream(name: String): InputStream =
          if (opens.hasNext) opens.next()
          else throw new IllegalStateException("opened past the index")
      }
      val route = staticAssetsFrom(system.toTyped, loader) { (path, asset) =>
        (cacheControlled(StaticAssetCaching.immutable)(path) & conditional(asset.etag)) {
          complete(staticAssetEntity(asset))
        }
      }
      val tag = Get("/static/synthetic.css") ~> route ~> check {
        status shouldBe StatusCodes.OK
        response.entity.dataBytes.runFold(ByteString.empty)(_ ++ _).futureValue shouldBe ByteString(payload)
        response.header[ETag].getOrElse(fail("response must carry an etag")).etag
      }
      Get("/static/synthetic.css") ~> route ~> check {
        status shouldBe StatusCodes.OK
        response.entity.dataBytes.runFold(ByteString.empty)(_ ++ _).futureValue shouldBe ByteString(payload)
      }
      Get("/static/synthetic.css") ~> `If-None-Match`(EntityTagRange(tag)) ~> route ~> check {
        status shouldBe StatusCodes.NotModified
      }
    }

    "not remember a failed load" in {
      val error = new IllegalStateException("first load fails")
      val payload = Array[Byte](4, 5, 6)
      // Sequential requests again: the first open throws, the retry loads and
      // serves normally.
      val attempts = Iterator[() => InputStream](
        () => throw error,
        () => new ByteArrayInputStream(payload),
        () => new ByteArrayInputStream(payload))
      val loader = new ClassLoader(null) {
        override def getResourceAsStream(name: String): InputStream =
          if (attempts.hasNext) attempts.next()()
          else throw new IllegalStateException("opened past the fixture")
      }
      val route = staticAssetsFrom(system.toTyped, loader) { (_, asset) =>
        complete(staticAssetEntity(asset))
      }
      Get("/static/synthetic.css") ~> route ~> check {
        rejections.collectFirst { case r: StaticAssetRoutingRejection => r.cause } shouldBe Some(Some(error))
      }
      Get("/static/synthetic.css") ~> route ~> check {
        status shouldBe StatusCodes.OK
        response.entity.dataBytes.runFold(ByteString.empty)(_ ++ _).futureValue shouldBe ByteString(payload)
      }
    }
  }

  "getStaticAssetRoute" should {
    "serve through the supplied system, not the materializer in the request context" in {
      val stopped = Materializer(system)
      stopped.shutdown()
      val route = withMaterializer(stopped)(UnderTest.getStaticAssetRoute(system.toTyped))
      Get("/static/static-asset-routing-fixture.css") ~> route ~> check {
        status shouldBe StatusCodes.OK
        responseAs[String].trim should include("body")
      }
      val cachedRoute = withMaterializer(stopped)(UnderTest.getStaticAssetRoute(system.toTyped, cachePolicy))
      Get("/static/static-asset-routing/0a1b2c3d/fixture.css") ~> cachedRoute ~> check {
        status shouldBe StatusCodes.OK
        response.header[ETag].isDefined shouldBe true
      }
    }
  }
}
