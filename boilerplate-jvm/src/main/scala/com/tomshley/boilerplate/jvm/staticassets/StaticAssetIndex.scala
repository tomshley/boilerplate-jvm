package com.tomshley.boilerplate.jvm.staticassets

import org.apache.pekko.actor.typed.{ActorRef, Behavior}
import org.apache.pekko.actor.typed.scaladsl.Behaviors
import org.apache.pekko.http.scaladsl.model.Uri
import org.apache.pekko.http.scaladsl.model.headers.EntityTag
import org.apache.pekko.pattern.StatusReply
import org.apache.pekko.stream.Materializer
import org.apache.pekko.stream.scaladsl.Source
import org.apache.pekko.util.ByteString

import java.security.MessageDigest
import java.util.HexFormat
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}
import scala.util.control.NonFatal

/**
 * A single-writer memo of derived, write-once data: the first `Lookup` of a
 * path starts `load`, concurrent lookups of the same path coalesce onto the
 * in-flight load, and every later `Lookup` is answered from the remembered
 * result. A failed load is forgotten, not stored, so the next lookup retries.
 * The actor does no I/O itself; `load` must not block.
 */
private[staticassets] object StaticAssetIndex {

  sealed trait Command
  final case class Lookup(path: Uri.Path, replyTo: ActorRef[StatusReply[AssetInfo]]) extends Command
  private final case class Indexed(path: Uri.Path, result: Try[AssetInfo]) extends Command

  final case class AssetInfo(etag: EntityTag, length: Long)

  private val hexFormat: HexFormat = HexFormat.of()

  /**
   * The validator and length of a byte source: a strong ETag of the first 8
   * bytes of its SHA-256 as 16 hex digits. One `MessageDigest` per
   * materialization, confined to the fold.
   */
  def digest(data: Source[ByteString, ?])(using Materializer): Future[AssetInfo] = {
    val md = MessageDigest.getInstance("SHA-256")
    data
      .runFold(0L) { (n, chunk) =>
        chunk.asByteBuffers.foreach(buffer => md.update(buffer))
        n + chunk.length
      }
      .map(n => AssetInfo(EntityTag(hexFormat.formatHex(md.digest(), 0, 8)), n))(ExecutionContext.parasitic)
  }

  def apply(load: Uri.Path => Future[AssetInfo]): Behavior[Command] =
    running(load, Map.empty, Map.empty)

  private def running(
      load: Uri.Path => Future[AssetInfo],
      index: Map[Uri.Path, AssetInfo],
      pending: Map[Uri.Path, List[ActorRef[StatusReply[AssetInfo]]]]): Behavior[Command] =
    Behaviors.receive { (context, message) =>
      message match {
        case Lookup(path, replyTo) =>
          index.get(path) match {
            case Some(info) =>
              replyTo ! StatusReply.success(info)
              Behaviors.same
            case None =>
              pending.get(path) match {
                case Some(waiters) =>
                  running(load, index, pending.updated(path, replyTo :: waiters))
                case None =>
                  val loading = try load(path) catch { case NonFatal(e) => Future.failed(e) }
                  context.pipeToSelf(loading)(Indexed(path, _))
                  running(load, index, pending.updated(path, List(replyTo)))
              }
          }
        case Indexed(path, Success(info)) =>
          pending.getOrElse(path, Nil).foreach(_ ! StatusReply.success(info))
          running(load, index.updated(path, info), pending - path)
        case Indexed(path, Failure(error)) =>
          pending.getOrElse(path, Nil).foreach(_ ! StatusReply.error(error))
          running(load, index, pending - path)
      }
    }
}
