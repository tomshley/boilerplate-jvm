package com.tomshley.boilerplate.jvm.staticassets

import com.tomshley.boilerplate.jvm.staticassets.StaticAssetCaching.CachePolicy
import com.tomshley.boilerplate.jvm.staticassets.StaticAssetDirectives.{cacheControlled, staticAssetEntity, staticAssetsFrom}
import com.tomshley.boilerplate.jvm.utils.FilesUtil
import org.apache.pekko.actor.typed.ActorSystem
import org.apache.pekko.http.scaladsl.server.Directives.{complete, conditional}
import org.apache.pekko.http.scaladsl.server.Route

/** Static-asset routes; the inherited `FilesUtil` methods remain for API compatibility. */
trait StaticAssetRouting extends FilesUtil {

  /** Classpath serving of requests below `/static/`; the overload taking a
    * `CachePolicy` adds content validators and cache headers. Each route
    * value owns an index actor that computes an asset's validator and byte
    * length on the first request for a path; the response body streams the
    * resource.
    */
  def getStaticAssetRoute(system: ActorSystem[?]): Route =
    staticAssetsFrom(system, getClass.getClassLoader) { (_, asset) =>
      complete(staticAssetEntity(asset))
    }

  /**
   * The same static-asset route, with opt-in cache validators: every served
   * resource carries a strong `ETag` computed from its bytes, conditional
   * requests are answered through Pekko's `conditional` directive, and the
   * caller's `cachePolicy` supplies the `Cache-Control` header on `200` and
   * `304` responses — other statuses are left untouched, and where the policy
   * is undefined no header is sent. The policy is consulted only for a
   * resource that exists and has a supported extension. The validator and
   * length are computed once per path on first request and kept for the life
   * of the system: a `304` costs no I/O, a `200` streams the resource, and
   * nothing is stored for a path that fails to load.
   */
  def getStaticAssetRoute(
      system: ActorSystem[?],
      cachePolicy: CachePolicy): Route =
    staticAssetsFrom(system, getClass.getClassLoader) { (path, asset) =>
      (cacheControlled(cachePolicy)(path) & conditional(asset.etag)) {
        complete(staticAssetEntity(asset))
      }
    }
}
