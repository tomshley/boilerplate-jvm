package com.tomshley.boilerplate.jvm.staticassets

import org.apache.pekko.http.scaladsl.model.ContentType
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

final class StaticAssetTypeSpec extends AnyWordSpec with Matchers {

  "StaticAssetType" should {
    "return lowercase extensions" in {
      StaticAssetType.JS.toExtension shouldBe "js"
      StaticAssetType.CSS.toExtension shouldBe "css"
      StaticAssetType.JSON.toExtension shouldBe "json"
      StaticAssetType.WEBP.toExtension shouldBe "webp"
    }

    "return the correct mime type" in {
      StaticAssetType.JS.toMime shouldBe "text/javascript"
      StaticAssetType.CSS.toMime shouldBe "text/css"
      StaticAssetType.JSON.toMime shouldBe "application/json"
      StaticAssetType.WEBP.toMime shouldBe "image/webp"
    }

    "carry a charset only where the media type has one" in {
      StaticAssetType.values.foreach { assetType =>
        assetType match {
          case StaticAssetType.JS =>
            assetType.toContentType.value shouldBe "text/javascript; charset=UTF-8"
          case StaticAssetType.CSS =>
            assetType.toContentType.value shouldBe "text/css; charset=UTF-8"
          case StaticAssetType.JSON =>
            assetType.toContentType.value shouldBe "application/json"
          case StaticAssetType.WEBMANIFEST =>
            assetType.toContentType.value shouldBe "application/manifest+json"
          case StaticAssetType.PNG | StaticAssetType.JPEG | StaticAssetType.JPG | StaticAssetType.WOFF2 |
               StaticAssetType.SVG | StaticAssetType.WEBP | StaticAssetType.ICO =>
            assetType.toContentType.value shouldBe assetType.toMime
            assetType.toContentType shouldBe a[ContentType.Binary]
        }
      }
    }

    "derive the media type from the mime type for every asset type" in {
      StaticAssetType.values.foreach { assetType =>
        val mediaType = assetType.toMediaType
        s"${mediaType.mainType}/${mediaType.subType}" shouldBe assetType.toMime
        mediaType.fileExtensions should contain(assetType.toExtension)
      }
    }
  }
}
