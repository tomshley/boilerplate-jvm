package com.tomshley.boilerplate.jvm.staticassets

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

    "produce a ContentType with UTF-8 charset" in {
      val ct = StaticAssetType.JS.toContentType
      ct.charset shouldBe org.apache.pekko.http.scaladsl.model.HttpCharsets.`UTF-8`
    }

    "derive the media type from the mime type for every asset type" in {
      StaticAssetType.values.foreach { assetType =>
        val mediaType = assetType.toMediaType
        s"${mediaType.mainType}/${mediaType.subType}" shouldBe assetType.toMime
        mediaType.fileExtensions shouldBe List(assetType.toExtension)
      }
    }
  }
}
