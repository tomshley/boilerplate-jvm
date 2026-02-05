package com.tomshley.boilerplate.jvm.staticassets

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

final class StaticAssetTypeSpec extends AnyWordSpec with Matchers {

  "StaticAssetType" should {
    "return lowercase extensions" in {
      StaticAssetType.JS.toExtension shouldBe "js"
      StaticAssetType.CSS.toExtension shouldBe "css"
    }

    "return the correct mime type" in {
      StaticAssetType.JS.toMime shouldBe "text/javascript"
      StaticAssetType.CSS.toMime shouldBe "text/css"
    }

    "produce a ContentType with UTF-8 charset" in {
      val ct = StaticAssetType.JS.toContentType
      ct.charset shouldBe org.apache.pekko.http.scaladsl.model.HttpCharsets.`UTF-8`
    }
  }
}
