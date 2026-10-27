package com.tomshley.boilerplate.jvm.staticassets

import com.tomshley.boilerplate.jvm.staticassets.StaticAssetIndex.AssetInfo
import org.apache.pekko.actor.testkit.typed.scaladsl.{LogCapturing, ScalaTestWithActorTestKit}
import org.apache.pekko.http.scaladsl.model.Uri
import org.apache.pekko.http.scaladsl.model.headers.EntityTag
import org.apache.pekko.pattern.StatusReply
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

import scala.concurrent.{Future, Promise}

final class StaticAssetIndexSpec
    extends ScalaTestWithActorTestKit
    with AnyWordSpecLike
    with Matchers
    with LogCapturing {

  private def info(tag: String): AssetInfo = AssetInfo(EntityTag(tag), tag.length.toLong)

  "StaticAssetIndex" should {
    "coalesce concurrent lookups of one path into one load" in {
      val loaded = Promise[AssetInfo]()
      val once = Promise[Unit]()
      val load: Uri.Path => Future[AssetInfo] =
        _ => if (once.trySuccess(())) loaded.future else Future.failed(new AssertionError("loaded twice"))
      val index = spawn(StaticAssetIndex(load))
      val first = createTestProbe[StatusReply[AssetInfo]]()
      val second = createTestProbe[StatusReply[AssetInfo]]()
      val expected = info("0123456789abcdef")

      index ! StaticAssetIndex.Lookup(Uri.Path("/a.css"), first.ref)
      index ! StaticAssetIndex.Lookup(Uri.Path("/a.css"), second.ref)
      loaded.success(expected)

      first.expectMessage(StatusReply.Success(expected))
      second.expectMessage(StatusReply.Success(expected))
    }

    "answer a remembered path without loading" in {
      val expected = info("0123456789abcdef")
      val loads = Iterator(Future.successful(expected))
      val load: Uri.Path => Future[AssetInfo] =
        _ => if (loads.hasNext) loads.next() else Future.failed(new AssertionError("loaded twice"))
      val index = spawn(StaticAssetIndex(load))
      val probe = createTestProbe[StatusReply[AssetInfo]]()

      index ! StaticAssetIndex.Lookup(Uri.Path("/a.css"), probe.ref)
      probe.expectMessage(StatusReply.Success(expected))
      index ! StaticAssetIndex.Lookup(Uri.Path("/a.css"), probe.ref)
      probe.expectMessage(StatusReply.Success(expected))
    }

    "forget a failed load and retry on the next lookup" in {
      val error = new IllegalStateException("load failed")
      val expected = info("0123456789abcdef")
      val loads = Iterator(Future.failed[AssetInfo](error), Future.successful(expected))
      val load: Uri.Path => Future[AssetInfo] =
        _ => if (loads.hasNext) loads.next() else Future.failed(new AssertionError("loaded past fixture"))
      val index = spawn(StaticAssetIndex(load))
      val probe = createTestProbe[StatusReply[AssetInfo]]()

      index ! StaticAssetIndex.Lookup(Uri.Path("/a.css"), probe.ref)
      probe.receiveMessage() match {
        case StatusReply.Error(e) => e shouldBe error
        case other => fail(s"expected StatusReply.Error, got $other")
      }

      index ! StaticAssetIndex.Lookup(Uri.Path("/a.css"), probe.ref)
      probe.expectMessage(StatusReply.Success(expected))
    }

    "keep paths independent" in {
      val infoA = info("aaaaaaaaaaaaaaaa")
      val infoB = info("bbbbbbbbbbbbbbbb")
      val index = spawn(StaticAssetIndex { path =>
        Future.successful(if (path == Uri.Path("/a.css")) infoA else infoB)
      })
      val probe = createTestProbe[StatusReply[AssetInfo]]()

      index ! StaticAssetIndex.Lookup(Uri.Path("/a.css"), probe.ref)
      index ! StaticAssetIndex.Lookup(Uri.Path("/b.css"), probe.ref)
      probe.expectMessage(StatusReply.Success(infoA))
      probe.expectMessage(StatusReply.Success(infoB))
    }
  }
}
