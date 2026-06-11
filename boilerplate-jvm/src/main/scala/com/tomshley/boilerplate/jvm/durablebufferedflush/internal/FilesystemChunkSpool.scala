/*
 * Copyright 2026 Tomshley LLC.
 * All Rights Reserved.
 */

package com.tomshley.boilerplate.jvm.durablebufferedflush.internal

import com.tomshley.boilerplate.jvm.durablebufferedflush.{ChunkSpool, SpoolMeta, SpoolSizeReporter}
import com.tomshley.boilerplate.jvm.utils.RestorableDigestUtil
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.pekko.actor.typed.{ActorRef, ActorSystem, Behavior, DispatcherSelector}
import org.apache.pekko.actor.typed.scaladsl.{Behaviors, StashBuffer}

import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.file.StandardOpenOption.{CREATE, CREATE_NEW, READ, TRUNCATE_EXISTING, WRITE}
import java.nio.file.StandardCopyOption.{ATOMIC_MOVE, REPLACE_EXISTING}
import java.nio.file.attribute.BasicFileAttributes
import java.nio.file.{FileVisitResult, Files, Path, SimpleFileVisitor}
import java.util.UUID
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.jdk.CollectionConverters.IteratorHasAsScala
import scala.util.{Failure, Success, Try, Using}

/** Local filesystem spool for chunk data.
  *
  * Contract:
  *   - write() fsyncs the chunk file contents before returning
  *   - ACK is gated on write() completion — if write() succeeds, the chunk file contents are durable on disk
  *   - readChunk() returns the raw proto bytes for a given sequence
  *   - readMeta() returns the current spool metadata (or None if spool doesn't exist)
  *   - updateMeta() atomically replaces `meta.json` (write temp → fsync temp file → rename)
  *
  * Crash-consistency note:
  *   - This implementation fsyncs both file contents and parent directory entries.
  *   - After chunk file creation and meta.json rename, the parent directory is fsynced
  *     to ensure directory entry visibility survives sudden host crash.
  *   - On platforms where directory fsync is not supported, the fsync call is a no-op
  *     and the durability guarantee degrades to process-crash-only.
  *
  * File layout:
  * {{{
  *   {spoolRoot}/{entityId}/
  *     chunks/
  *       000000000.bin     ← serialized chunk bytes
  *       000000001.bin
  *       ...
  *     meta.json           ← SpoolMeta JSON
  * }}}
  *
  * Chunk filenames are zero-padded 9-digit sequence numbers (supports up to 999,999,999 chunks).
  */
class FilesystemChunkSpool(
    system: ActorSystem[?],
    rootDir: Path
) extends ChunkSpool with SpoolSizeReporter {

  private given ExecutionContext =
    system.executionContext

  private val blockingEc = system.dispatchers.lookup(DispatcherSelector.blocking())

  private val mapper = new ObjectMapper()
  mapper.registerModule(DefaultScalaModule)
  mapper.registerModule(new JavaTimeModule())

  // Actor-owned byte counter. The actor's mailbox serializes increments
  // (post-chunk-fsync), decrements (post-cleanup), full replacements
  // (post-recount), and queries. There is no shared mutable state and no
  // atomic primitive — the count lives entirely inside the actor's behavior
  // recursion, which is the same architectural slot as `AdmissionGate` and
  // `SpoolPressureMonitor`. Per-mutation cost is one mailbox enqueue; per-
  // read cost is one mailbox enqueue plus one Promise allocation. Both are
  // O(1) and both are dwarfed by the chunk + meta fsyncs already present
  // on the write hot path.
  //
  // The actor starts at zero. Pre-existing on-disk state from a previous
  // process is reconciled by the first `recountFromFilesystem()` call,
  // which the monitor schedules on its slow tick. A fresh process therefore
  // reads zero until that tick lands — the conservative default (no
  // false-Critical signal at startup).
  private val sizeAccount: ActorRef[SpoolSizeAccountingActor.Command] =
    system.systemActorOf(
      SpoolSizeAccountingActor(),
      s"spool-size-${UUID.randomUUID()}"
    )

  private object EntityActor {
    sealed trait Command

    final case class Write(seq: Long, bytes: Array[Byte], reply: Promise[Long]) extends Command

    final case class ReadChunk(seq: Long, reply: Promise[Array[Byte]]) extends Command

    final case class ReadMeta(reply: Promise[Option[SpoolMeta]]) extends Command

    final case class UpdateMeta(meta: SpoolMeta, reply: Promise[Unit]) extends Command

    final case class UpdateFlushedSeq(seq: Long, reply: Promise[Unit]) extends Command

    final case class Initialize(meta: SpoolMeta, reply: Promise[SpoolMeta]) extends Command

    final case class Cleanup(reply: Promise[Unit]) extends Command

    private sealed trait Completion extends Command {
      def complete(): Unit
      def stopIfIdle: Boolean
      def succeeded: Boolean
    }

    private final case class WriteCompleted(reply: Promise[Long], result: Try[Long]) extends Completion {
      override val stopIfIdle: Boolean = false
      override val succeeded: Boolean = result.isSuccess
      override def complete(): Unit = completePromise(reply, result)
    }

    private final case class ReadChunkCompleted(reply: Promise[Array[Byte]], result: Try[Array[Byte]]) extends Completion {
      override val stopIfIdle: Boolean = false
      override val succeeded: Boolean = result.isSuccess
      override def complete(): Unit = completePromise(reply, result)
    }

    private final case class ReadMetaCompleted(reply: Promise[Option[SpoolMeta]], result: Try[Option[SpoolMeta]]) extends Completion {
      override val stopIfIdle: Boolean = false
      override val succeeded: Boolean = result.isSuccess
      override def complete(): Unit = completePromise(reply, result)
    }

    private final case class UpdateMetaCompleted(reply: Promise[Unit], result: Try[Unit]) extends Completion {
      override val stopIfIdle: Boolean = false
      override val succeeded: Boolean = result.isSuccess
      override def complete(): Unit = completePromise(reply, result)
    }

    private final case class UpdateFlushedSeqCompleted(reply: Promise[Unit], result: Try[Unit]) extends Completion {
      override val stopIfIdle: Boolean = false
      override val succeeded: Boolean = result.isSuccess
      override def complete(): Unit = completePromise(reply, result)
    }

    private final case class InitializeCompleted(reply: Promise[SpoolMeta], result: Try[SpoolMeta]) extends Completion {
      override val stopIfIdle: Boolean = false
      override val succeeded: Boolean = result.isSuccess
      override def complete(): Unit = completePromise(reply, result)
    }

    private final case class CleanupCompleted(reply: Promise[Unit], result: Try[Unit]) extends Completion {
      override val stopIfIdle: Boolean = true
      override val succeeded: Boolean = result.isSuccess
      override def complete(): Unit = ()
    }

    def apply(entityId: String): Behavior[Command] =
      // One actor serializes all filesystem ops per entity. The stash only needs
      // to absorb short bursts while a single blocking I/O op is in flight.
      Behaviors.withStash(capacity = 1024) { stash =>
        idle(entityId, stash)
      }

    private def idle(entityId: String, stash: StashBuffer[Command]): Behavior[Command] =
      Behaviors.receive { (context, message) =>
        message match {
          case Write(seq, bytes, reply) =>
            context.pipeToSelf(Future(FilesystemChunkSpool.this.writeBlocking(entityId, seq, bytes))(FilesystemChunkSpool.this.blockingEc))(
              result => WriteCompleted(reply, result)
            )
            busy(entityId, stash)
          case ReadChunk(seq, reply) =>
            context.pipeToSelf(Future(FilesystemChunkSpool.this.readChunkBlocking(entityId, seq))(FilesystemChunkSpool.this.blockingEc))(
              result => ReadChunkCompleted(reply, result)
            )
            busy(entityId, stash)
          case ReadMeta(reply) =>
            context.pipeToSelf(Future(FilesystemChunkSpool.this.readMetaBlocking(entityId))(FilesystemChunkSpool.this.blockingEc))(
              result => ReadMetaCompleted(reply, result)
            )
            busy(entityId, stash)
          case UpdateMeta(meta, reply) =>
            context.pipeToSelf(Future(FilesystemChunkSpool.this.updateMetaBlocking(entityId, meta.copy(entityId = entityId)))(FilesystemChunkSpool.this.blockingEc))(
              result => UpdateMetaCompleted(reply, result)
            )
            busy(entityId, stash)
          case UpdateFlushedSeq(seq, reply) =>
            context.pipeToSelf(Future(FilesystemChunkSpool.this.updateFlushedSeqBlocking(entityId, seq))(FilesystemChunkSpool.this.blockingEc))(
              result => UpdateFlushedSeqCompleted(reply, result)
            )
            busy(entityId, stash)
          case Initialize(meta, reply) =>
            context.pipeToSelf(Future(FilesystemChunkSpool.this.initializeBlocking(entityId, meta))(FilesystemChunkSpool.this.blockingEc))(
              result => InitializeCompleted(reply, result)
            )
            busy(entityId, stash)
          case Cleanup(reply) =>
            context.pipeToSelf(Future(FilesystemChunkSpool.this.cleanupBlocking(entityId))(FilesystemChunkSpool.this.blockingEc))(
              result => CleanupCompleted(reply, result)
            )
            busy(entityId, stash)
          case completion: Completion =>
            completion.complete()
            Behaviors.same
        }
      }

    private def busy(entityId: String, stash: StashBuffer[Command]): Behavior[Command] =
      Behaviors.receive { (context, message) =>
        message match {
          case completion: Completion =>
            completion match {
              case CleanupCompleted(reply, result) if result.isSuccess && stash.isEmpty =>
                entityRegistry ! EntityRegistry.CleanupFinished(entityId, context.self, reply, result)
                Behaviors.stopped
              case CleanupCompleted(reply, result) =>
                completePromise(reply, result)
                stash.unstashAll(idle(entityId, stash))
              case _ =>
                completion.complete()
                if (completion.stopIfIdle && completion.succeeded && stash.isEmpty) {
                  entityRegistry ! EntityRegistry.Release(entityId, context.self)
                  Behaviors.stopped
                } else {
                  stash.unstashAll(idle(entityId, stash))
                }
            }
          case other =>
            stash.stash(other)
            Behaviors.same
        }
      }

    private def completePromise[A](reply: Promise[A], result: Try[A]): Unit =
      result match {
        case Success(value) =>
          reply.trySuccess(value)
        case Failure(ex) =>
          reply.tryFailure(ex)
      }
  }

  private object EntityRegistry {
    sealed trait Command

    final case class Get(entityId: String, reply: Promise[ActorRef[EntityActor.Command]]) extends Command

    final case class CleanupFinished(
        entityId: String,
        ref: ActorRef[EntityActor.Command],
        reply: Promise[Unit],
        result: Try[Unit]
    ) extends Command

    final case class Release(entityId: String, ref: ActorRef[EntityActor.Command]) extends Command

    def apply(): Behavior[Command] =
      active(Map.empty)

    private def completeRegistryPromise[A](reply: Promise[A], result: Try[A]): Unit =
      result match {
        case Success(value) =>
          reply.trySuccess(value)
        case Failure(ex) =>
          reply.tryFailure(ex)
      }

    private def active(refs: Map[String, ActorRef[EntityActor.Command]]): Behavior[Command] =
      Behaviors.receive { (context, message) =>
        message match {
          case Get(entityId, reply) =>
            refs.get(entityId) match {
              case Some(existing) =>
                reply.trySuccess(existing)
                Behaviors.same
              case None =>
                val created = context.spawn(EntityActor(entityId), s"chunk-spool-entity-${UUID.randomUUID()}")
                reply.trySuccess(created)
                active(refs.updated(entityId, created))
            }
          case CleanupFinished(entityId, ref, reply, result) =>
            refs.get(entityId) match {
              case Some(existing) if existing == ref =>
                completeRegistryPromise(reply, result)
                active(refs - entityId)
              case _ =>
                completeRegistryPromise(reply, result)
                Behaviors.same
            }
          case Release(entityId, ref) =>
            refs.get(entityId) match {
              case Some(existing) if existing == ref =>
                active(refs - entityId)
              case _ =>
                Behaviors.same
            }
        }
      }
  }

  private val entityRegistry = system.systemActorOf(
    EntityRegistry(),
    s"chunk-spool-entity-registry-${UUID.randomUUID()}"
  )
 
  protected def beforeChunkWrite(entityId: String, seq: Long, path: Path, bytes: Array[Byte]): Unit = ()

  protected def afterChunkFileFsync(entityId: String, seq: Long, path: Path, bytesWritten: Long): Unit = ()

  protected def beforeMetaRead(entityId: String, path: Path): Unit = ()

  protected def beforeMetaTempWrite(entityId: String, meta: SpoolMeta, tmpPath: Path): Unit = ()

  protected def afterMetaTempFsync(entityId: String, meta: SpoolMeta, tmpPath: Path): Unit = ()

  protected def beforeMetaRename(entityId: String, meta: SpoolMeta, tmpPath: Path, finalPath: Path): Unit = ()

  override def write(entityId: String, seq: Long, bytes: Array[Byte]): Future[Long] =
    withEntityActor(entityId) { (actor, reply) =>
      actor ! EntityActor.Write(seq, bytes, reply)
    }

  override def readChunk(entityId: String, seq: Long): Future[Array[Byte]] =
    withEntityActor(entityId) { (actor, reply) =>
      actor ! EntityActor.ReadChunk(seq, reply)
    }

  override def readMeta(entityId: String): Future[Option[SpoolMeta]] =
    withEntityActor(entityId) { (actor, reply) =>
      actor ! EntityActor.ReadMeta(reply)
    }

  override def updateMeta(entityId: String, meta: SpoolMeta): Future[Unit] =
    withEntityActor(entityId) { (actor, reply) =>
      actor ! EntityActor.UpdateMeta(meta, reply)
    }

  override def updateFlushedSeq(entityId: String, seq: Long): Future[Unit] =
    withEntityActor(entityId) { (actor, reply) =>
      actor ! EntityActor.UpdateFlushedSeq(seq, reply)
    }

  override def initialize(entityId: String, meta: SpoolMeta): Future[SpoolMeta] =
    withEntityActor(entityId) { (actor, reply) =>
      actor ! EntityActor.Initialize(meta, reply)
    }

  override def cleanup(entityId: String): Future[Unit] =
    withEntityActor(entityId) { (actor, reply) =>
      actor ! EntityActor.Cleanup(reply)
    }

  override def listEntities(): Future[Seq[String]] =
    Future {
      if (!Files.exists(rootDir)) Seq.empty
      else {
        import scala.jdk.CollectionConverters.*
        val stream = Files.list(rootDir)
        try {
          stream.iterator().asScala
            .filter(path => Files.isDirectory(path))
            .map(_.getFileName.toString)
            .toSeq
            .sorted
        } finally {
          stream.close()
        }
      }
    }(blockingEc)

  private def validatedEntityFuture[A](entityId: String)(run: String => Future[A]): Future[A] =
    Try(validateEntityId(entityId)) match {
      case Success(safeEntityId) =>
        run(safeEntityId)
      case Failure(ex) =>
        Future.failed(ex)
    }

  private def withEntityActor[A](
      entityId: String
  )(
      send: (ActorRef[EntityActor.Command], Promise[A]) => Unit
  ): Future[A] =
    validatedEntityFuture(entityId) { safeEntityId =>
      entityActorFor(safeEntityId).flatMap { actor =>
        val reply = Promise[A]()
        send(actor, reply)
        reply.future
      }
    }

  private def entityActorFor(entityId: String): Future[ActorRef[EntityActor.Command]] = {
    val reply = Promise[ActorRef[EntityActor.Command]]()
    entityRegistry ! EntityRegistry.Get(entityId, reply)
    reply.future
  }

  private val normalizedRootDir = rootDir.toAbsolutePath.normalize()

  private def writeBlocking(entityId: String, seq: Long, bytes: Array[Byte]): Long = {
    require(seq >= 0, s"seq must be >= 0: $seq")
    val meta = readMetaBlocking(entityId).getOrElse(
      throw new IllegalStateException(s"Spool not initialized for entity $entityId")
    )
    val expectedSeq = meta.lastSpooledSeq + 1L
    if (seq != expectedSeq) {
      throw new IllegalArgumentException(
        s"Non-contiguous spool sequence for entity $entityId: expected $expectedSeq, got $seq"
      )
    }
    val path = chunkPath(entityId, seq)
    Files.createDirectories(path.getParent)
    beforeChunkWrite(entityId, seq, path, bytes)
    Using.resource(FileChannel.open(path, CREATE_NEW, WRITE)) { channel =>
      val buffer = ByteBuffer.wrap(bytes)
      while (buffer.hasRemaining) {
        channel.write(buffer)
      }
      channel.force(true)
    }
    fsyncParentDir(path.getParent)
    afterChunkFileFsync(entityId, seq, path, bytes.length.toLong)
    // Digest midstate advances under the SAME atomic rename as
    // lastSpooledSeq — coverage and watermark cannot diverge, even across
    // crash/restart. The fold runs inside the per-entity actor's serialized
    // write path (single-writer confinement); the BC digest never escapes
    // RestorableDigestUtil's call frames. A spool whose existing chunks lack
    // coverage (meta written by a pre-digest version) stays uncovered —
    // None propagates rather than fabricating a wrong hash.
    val nextDigestState: Option[String] =
      if (meta.lastSpooledSeq < 0L) Some(RestorableDigestUtil.sha256FoldHex(None, bytes))
      else meta.digestStateHex.map(state => RestorableDigestUtil.sha256FoldHex(Some(state), bytes))
    updateMetaBlocking(
      entityId,
      meta.withSpooled(seq, bytes.length.toLong).withDigestState(nextDigestState)
    )
    // Inform the size-accounting actor after the chunk + meta are durable.
    // The observed value via `currentSizeBytes()` therefore lags the on-disk
    // truth by at most one in-flight chunk per entity actor (which
    // serializes its own writes) plus one mailbox traversal on the
    // size-accounting actor.
    sizeAccount ! SpoolSizeAccountingActor.Increment(bytes.length.toLong)
    bytes.length.toLong
  }

  private def updateFlushedSeqBlocking(entityId: String, seq: Long): Unit = {
    require(seq >= -1L, s"flushed seq must be >= -1: $seq")
    val current = readMetaBlocking(entityId).getOrElse(
      throw new IllegalStateException(s"Spool not initialized for entity $entityId")
    )
    if (seq > current.lastSpooledSeq) {
      throw new IllegalArgumentException(
        s"Cannot advance flushedSeq beyond lastSpooledSeq for entity $entityId: flushed=$seq, spooled=${current.lastSpooledSeq}"
      )
    }
    val nextSeq = math.max(current.flushedSeq, seq)
    if (nextSeq > current.flushedSeq) {
      updateMetaBlocking(entityId, current.withFlushed(nextSeq))
    }
  }

  private def initializeBlocking(entityId: String, meta: SpoolMeta): SpoolMeta = {
    Files.createDirectories(chunksDir(entityId))
    readMetaBlocking(entityId) match {
      case Some(existing) => existing
      case None =>
        val initialMeta = meta.copy(entityId = entityId)
        updateMetaBlocking(entityId, initialMeta)
        initialMeta
    }
  }

  private def cleanupBlocking(entityId: String): Unit = {
    val path = entityDir(entityId)
    if (Files.exists(path)) {
      // Source of truth for the byte delta is the meta's `totalSpooledBytes`,
      // which is maintained on every successful chunk write. Reading meta
      // BEFORE we delete the directory avoids a second filesystem walk and
      // a local fold accumulator. If meta has been corrupted or was never
      // written (entity directory exists with no meta), we fall back to 0L
      // and let the next `recountFromFilesystem()` tick reconcile the drift.
      val deletedChunkBytes =
        Try(readMetaBlocking(entityId)).toOption.flatten.map(_.totalSpooledBytes).getOrElse(0L)
      deleteEntityTree(path)
      if (deletedChunkBytes > 0L) {
        sizeAccount ! SpoolSizeAccountingActor.Decrement(deletedChunkBytes)
      }
    }
  }

  /** Recursive directory delete. The visitor only deletes — no accumulator,
    * no folding. The byte attribution for the size-accounting actor is
    * driven by the meta's `totalSpooledBytes` (see `cleanupBlocking`). */
  private def deleteEntityTree(path: Path): Unit =
    Files.walkFileTree(path, new SimpleFileVisitor[Path]() {
      override def visitFile(file: Path, attrs: BasicFileAttributes): FileVisitResult = {
        Files.deleteIfExists(file)
        FileVisitResult.CONTINUE
      }

      override def postVisitDirectory(dir: Path, exc: java.io.IOException | Null): FileVisitResult = {
        Files.deleteIfExists(dir)
        FileVisitResult.CONTINUE
      }

      override def visitFileFailed(file: Path, exc: java.io.IOException): FileVisitResult =
        exc match {
          case _: java.nio.file.NoSuchFileException => FileVisitResult.CONTINUE
          case other                                => throw other
        }
    })

  /** Predicate: does the supplied path live directly under a `chunks/`
    * subdirectory of an entity? Used by `walkAndSumChunkBytes` to filter
    * the recount fold so that meta / sidecar files are not counted as
    * chunk bytes.
    *
    * `getParent` / `getFileName` on a JDK [[Path]] return `null` for
    * root-relative cases (e.g. a top-level path with no parent). We lift
    * both into [[Option]] and short-circuit through them — trust the
    * option chain, not null checks. */
  private def isChunkFile(file: Path): Boolean =
    Option(file.getParent)
      .flatMap(parent => Option(parent.getFileName))
      .exists(_.toString == FilesystemChunkSpool.ChunksSubdir)

  private def validateEntityId(entityId: String): String = {
    require(entityId != null && entityId.nonEmpty, "entityId must be non-empty")
    require(entityId.forall(ch => ch.isLetterOrDigit || ch == '-' || ch == '_' || ch == '.'),
      s"entityId contains unsafe path characters: $entityId")
    val resolved = normalizedRootDir.resolve(entityId).normalize()
    require(resolved.startsWith(normalizedRootDir), s"entityId escapes spool root: $entityId")
    entityId
  }

  private def entityDir(entityId: String): Path =
    normalizedRootDir.resolve(entityId).normalize()

  private def chunksDir(entityId: String): Path =
    entityDir(entityId).resolve(FilesystemChunkSpool.ChunksSubdir)

  private def metaPath(entityId: String): Path =
    entityDir(entityId).resolve("meta.json")

  private def tempMetaPath(entityId: String): Path =
    entityDir(entityId).resolve("meta.json.tmp")

  private def chunkPath(entityId: String, seq: Long): Path =
    chunksDir(entityId).resolve(f"$seq%09d.bin")

  private def readChunkBlocking(entityId: String, seq: Long): Array[Byte] =
    Files.readAllBytes(chunkPath(entityId, seq))

  private def fsyncParentDir(dir: Path): Unit =
    Using.resource(FileChannel.open(dir, READ)) { channel =>
      channel.force(true)
    }

  private def readMetaBlocking(entityId: String): Option[SpoolMeta] = {
    val path = metaPath(entityId)
    if (!Files.exists(path)) None
    else {
      beforeMetaRead(entityId, path)
      Some(mapper.readValue(path.toFile, classOf[SpoolMeta]))
    }
  }

  private def updateMetaBlocking(entityId: String, meta: SpoolMeta): Unit = {
    Files.createDirectories(entityDir(entityId))
    val tmpPath = tempMetaPath(entityId)
    val finalPath = metaPath(entityId)
    beforeMetaTempWrite(entityId, meta, tmpPath)
    val bytes = mapper.writeValueAsBytes(meta)
    Using.resource(FileChannel.open(tmpPath, CREATE, WRITE, TRUNCATE_EXISTING)) { channel =>
      val buffer = ByteBuffer.wrap(bytes)
      while (buffer.hasRemaining) {
        channel.write(buffer)
      }
      channel.force(true)
    }
    afterMetaTempFsync(entityId, meta, tmpPath)
    beforeMetaRename(entityId, meta, tmpPath, finalPath)
    Files.move(tmpPath, finalPath, ATOMIC_MOVE, REPLACE_EXISTING)
    fsyncParentDir(finalPath.getParent)
  }

  // -- SpoolSizeReporter ------------------------------------------------------

  /** Async read of the actor-owned byte counter. O(1) — one mailbox
    * enqueue plus one Promise allocation. The resolved value may briefly
    * drift from the filesystem truth when a write or cleanup is in
    * flight; the [[SpoolPressureMonitor]] reconciles drift via the
    * periodic `recountFromFilesystem` call. */
  override def currentSizeBytes(): Future[Long] = {
    val reply = Promise[Long]()
    sizeAccount ! SpoolSizeAccountingActor.Query(reply)
    reply.future
  }

  /** Authoritative recount: walks every entity's `chunks/` subdirectory
    * and sums chunk-file sizes via an immutable `Files.walk` fold. The
    * walk result is then sent to the size-accounting actor as a
    * [[SpoolSizeAccountingActor.ReplaceWith]] message so that the actor's
    * view is corrected for any drift that accumulated against external
    * mutations or in-flight writes.
    *
    * The Future resolves with the walk total. Because the actor processes
    * the `ReplaceWith` asynchronously, a [[currentSizeBytes()]] reader
    * that arrives immediately after this Future resolves may briefly see
    * the pre-recount value — that drift closes within one mailbox
    * traversal. The same property holds for any concurrent write whose
    * `Increment` message races the `ReplaceWith`: the increment may be
    * overwritten if it arrives at the actor BEFORE the recount, or
    * preserved on top of the recount if AFTER. Either ordering is
    * eventually-consistent with the filesystem.
    *
    * Runs on the spool's blocking dispatcher so that the walk does not
    * starve the consumer's general-purpose pool. */
  override def recountFromFilesystem(): Future[Long] =
    Future {
      val total = walkAndSumChunkBytes(normalizedRootDir)
      sizeAccount ! SpoolSizeAccountingActor.ReplaceWith(total)
      total
    }(blockingEc)

  /** Immutable fold over the spool root, summing the sizes of chunk
    * files. Uses `Files.walk` (stream-shaped) so the accumulation can be
    * expressed as a pure `.sum` — no var, no visitor accumulator. Closed
    * resource-safely via `Using.resource`.
    *
    * Race-tolerant: if a chunk file disappears between the walk and the
    * `Files.size` call (e.g. concurrent cleanup), the per-file `Try`
    * absorbs the [[java.nio.file.NoSuchFileException]] as `0L` and the
    * fold continues. */
  private def walkAndSumChunkBytes(root: Path): Long =
    if (!Files.exists(root)) 0L
    else
      Using.resource(Files.walk(root)) { stream =>
        stream.iterator().asScala
          .filter(p => Files.isRegularFile(p) && isChunkFile(p))
          .map(p => Try(Files.size(p)).getOrElse(0L))
          .sum
      }

  // -- internal: size-accounting actor ----------------------------------------

  /** Typed actor that owns the byte counter for this spool. The Long
    * lives entirely in `active(total)` via behavior recursion — there
    * is no field, no atomic, no shared mutable state. Mutations and
    * reads are serialized through the mailbox.
    *
    * The actor never stops on its own; it lives for the lifetime of the
    * supplied [[ActorSystem]] and is reaped by system shutdown. This
    * matches the per-entity actors that also live for the lifetime of
    * the spool. */
  private object SpoolSizeAccountingActor {
    sealed trait Command
    final case class Increment(delta: Long) extends Command
    final case class Decrement(delta: Long) extends Command
    final case class ReplaceWith(value: Long) extends Command
    final case class Query(reply: Promise[Long]) extends Command

    def apply(): Behavior[Command] = active(0L)

    private def active(total: Long): Behavior[Command] = Behaviors.receiveMessage {
      case Increment(d)   => active(math.max(0L, total + d))
      case Decrement(d)   => active(math.max(0L, total - d))
      case ReplaceWith(v) => active(math.max(0L, v))
      case Query(reply)   =>
        reply.success(total)
        Behaviors.same
    }
  }
}

private[durablebufferedflush] object FilesystemChunkSpool {

  /** Subdirectory name under each entity directory that holds chunk files.
    * Centralised so the on-disk layout is documented in exactly one place
    * and the chunk-file predicate has a single source of truth. */
  val ChunksSubdir: String = "chunks"
}
