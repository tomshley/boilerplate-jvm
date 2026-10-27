package com.tomshley.boilerplate.jvm.staticassets

import com.tomshley.boilerplate.jvm.staticassets.StaticAssetCaching.CachePolicy
import com.tomshley.boilerplate.jvm.staticassets.exceptions.StaticAssetRoutingRejection
import org.apache.pekko.actor.typed.{ActorSystem, Scheduler}
import org.apache.pekko.actor.typed.scaladsl.AskPattern.*
import org.apache.pekko.http.scaladsl.model.{ContentType, HttpEntity, StatusCodes, UniversalEntity, Uri}
import org.apache.pekko.http.scaladsl.model.headers.EntityTag
import org.apache.pekko.http.scaladsl.server.{Directive, Directive0, Directives}
import org.apache.pekko.stream.{IOResult, Materializer, SystemMaterializer}
import org.apache.pekko.stream.scaladsl.{Source, StreamConverters}
import org.apache.pekko.util.{ByteString, Timeout}

import java.io.FileNotFoundException
import java.util.UUID
import scala.annotation.tailrec
import scala.concurrent.Future
import scala.util.{Failure, Success}

/**
 * The pieces `getStaticAssetRoute` composes, exposed so a caller with an
 * unusual need can build its own static-asset route — adding a response
 * header, for example — without forking the library.
 */
object StaticAssetDirectives {
  import Directives.*

  /**
   * One classpath asset as served: its content type, strong validator, byte
   * length, and a re-materializable body.
   */
  final case class StaticAsset(
      contentType: ContentType,
      etag: EntityTag,
      length: Long,
      data: Source[ByteString, Future[IOResult]])

  private val contentTypeByExtension: Map[String, ContentType] =
    StaticAssetType.values.iterator
      .map(assetType => s".${assetType.toExtension}" -> assetType.toContentType)
      .toMap

  private def hasDotSegment(path: Uri.Path): Boolean = {
    @tailrec
    def loop(remaining: Uri.Path): Boolean = remaining match {
      case Uri.Path.Empty => false
      case Uri.Path.Slash(tail) => loop(tail)
      case Uri.Path.Segment(value, tail) =>
        if (value == "." || value == "..") true else loop(tail)
    }
    loop(path)
  }

  private def resource(classLoader: ClassLoader, resourceName: String): Source[ByteString, Future[IOResult]] =
    StreamConverters
      .fromInputStream(() => Option(classLoader.getResourceAsStream(resourceName))
        .getOrElse(throw new FileNotFoundException(resourceName)))

  /**
   * `GET /static/<path>` resolved against the class loader that loaded
   * `StaticAssetDirectives`: the request path below `/static/` and the
   * `StaticAsset` for it — or a `StaticAssetRoutingRejection` when the path
   * contains a `.` or `..` segment, the extension is unsupported, or the
   * resource is missing or unreadable; lookup and read failures keep their
   * cause on the rejection for the caller's diagnostics. Owns one index actor
   * on `system`, so build the directive once at wiring and reuse the value.
   */
  def staticAssets(system: ActorSystem[?]): Directive[(Uri.Path, StaticAsset)] =
    staticAssetsFrom(system, getClass.getClassLoader)

  private[staticassets] def staticAssetsFrom(
      system: ActorSystem[?],
      classLoader: ClassLoader): Directive[(Uri.Path, StaticAsset)] = {
    given Materializer = SystemMaterializer(system).materializer
    given Timeout = Timeout.create(
      system.settings.config.getDuration("tomshley-boilerplate-staticassets.ask-timeout"))
    given Scheduler = system.scheduler

    val index = system.systemActorOf(
      StaticAssetIndex(path => StaticAssetIndex.digest(resource(classLoader, path.toString))),
      s"static-asset-index-${UUID.randomUUID()}")
    (get & pathPrefix("static") & path(RemainingPath)).flatMap { assetPath =>
      val resourceName = assetPath.toString
      val extension =
        if (resourceName.indexOf('.') > 0) resourceName.substring(resourceName.lastIndexOf('.'))
        else ""
      if (hasDotSegment(assetPath)) {
        reject(StaticAssetRoutingRejection("An unsupported asset path specified"))
      } else {
        contentTypeByExtension.get(extension) match {
          case None =>
            reject(StaticAssetRoutingRejection("An unsupported asset type specified"))
          case Some(contentType) =>
            onComplete(index.askWithStatus[StaticAssetIndex.AssetInfo](StaticAssetIndex.Lookup(assetPath, _))).flatMap {
              case Failure(error) =>
                reject(StaticAssetRoutingRejection("An error occurred reading the file", Some(error)))
              case Success(info) =>
                tprovide((assetPath, StaticAsset(contentType, info.etag, info.length, resource(classLoader, resourceName))))
            }
        }
      }
    }
  }

  /**
   * The entity for a resolved asset: an empty `Strict` entity with the
   * asset's content type for a zero-length asset — `HttpEntity.Default`
   * requires a positive length — otherwise an `HttpEntity.Default` so
   * `Content-Length` is known and the body streams.
   */
  def staticAssetEntity(asset: StaticAsset): UniversalEntity =
    if (asset.length == 0) HttpEntity.empty(asset.contentType)
    else HttpEntity.Default(asset.contentType, asset.length, asset.data)

  /**
   * The `Cache-Control` header `policy` selects for `path`, added to `200` and
   * `304` responses only — any other status is left untouched, as are
   * rejections. A `Cache-Control` the inner route already set is authoritative
   * and wins over the policy; the selected header fills in only where none is
   * present, and nothing at all where the policy is undefined.
   */
  def cacheControlled(policy: CachePolicy)(path: Uri.Path): Directive0 =
    policy.lift(path).fold(pass) { header =>
      mapResponse { response =>
        response.status match {
          case StatusCodes.OK | StatusCodes.NotModified => response.withDefaultHeaders(header)
          case _ => response
        }
      }
    }
}
