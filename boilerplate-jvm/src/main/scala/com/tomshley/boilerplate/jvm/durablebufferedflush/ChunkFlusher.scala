/*
 * Copyright 2026 Tomshley LLC.
 * All Rights Reserved.
 */

package com.tomshley.boilerplate.jvm.durablebufferedflush

import scala.concurrent.Future
import scala.concurrent.ExecutionContext
import com.tomshley.boilerplate.jvm.durablebufferedflush.internal.DefaultChunkFlusherFactory
import com.tomshley.boilerplate.jvm.objectstorage.SingleShotBlobWriter
import com.typesafe.config.Config
import org.apache.pekko.actor.typed.ActorSystem

/** Asynchronous blob uploader that reads chunks from the spool and uploads them.
  *
  * The flusher runs in the background during transfer, uploading chunks as they
  * become available. It tracks a contiguous watermark (flushedSeq) — only
  * advances when all sequences up to that point are confirmed uploaded.
  *
  * Key contract:
  *   - start() begins background uploading from flushedSeq + 1
  *   - drain() blocks until all spooled chunks are uploaded, returns finalFlushedSeq
  *   - The handler MUST assert `drain() == lastSpooledSeq` before sending Close
  */
trait ChunkFlusher {

  /** The entity this flusher is uploading for. */
  def entityId: String

  /** Start the background flusher. Begins uploading from the current
    * flushedSeq + 1 in the spool meta. Idempotent — calling start()
    * on an already-running flusher is a no-op. */
  def start(): Unit

  /** Stop the background flusher. Sets a permanent failure cause so that
    * any in-flight upload callbacks become no-ops. Does NOT cancel futures
    * already submitted to the executor — it poisons further state advances.
    * Does NOT drain — use drain() for graceful shutdown. */
  def stop(): Unit

  /** Drain: wait for all currently-spooled chunks to be uploaded to blob storage.
    * Returns the final contiguous flushed sequence number.
    *
    * '''Precondition:''' The caller MUST ensure all spool writes have
    * completed before calling drain(). drain() reads `lastSpooledSeq`
    * from meta at call time to set its target — any writes that land
    * after that read will not be included and the Close barrier will
    * fail with a sequence mismatch.
    *
    * The handler MUST assert:
    * {{{
    *   val finalFlushedSeq = flusher.drain().futureValue
    *   require(finalFlushedSeq == lastSpooledSeq,
    *     s"Flusher drain incomplete: flushed=$finalFlushedSeq, expected=$lastSpooledSeq")
    * }}}
    *
    * before sending ask(Close).
    */
  def drain(): Future[Long]

  /** The current contiguous flushed watermark.
    * Only sequences 0..flushedSeq are guaranteed to be in blob storage. */
  def flushedSeq: Future[Long]

  /** Whether the flusher is currently running. */
  def isRunning: Future[Boolean]
}

 /** Factory for creating ChunkFlusher instances per entity.
   * Used by the handler and recovery manager. */
trait ChunkFlusherFactory {

  /** Create a new flusher for the given entity.
    *
    * @param entityId  the session entity identifier
    * @param spool     the chunk spool to read from
    * @param startSeq  the sequence to start flushing from (flushedSeq + 1)
    * @return a new ChunkFlusher instance (not yet started)
    */
  def create(entityId: String, spool: ChunkSpool, startSeq: Long): ChunkFlusher
}

object ChunkFlusherFactory {
  def background(
      config: Config,
      system: ActorSystem[?],
      blobWriter: SingleShotBlobWriter,
      bucket: String,
      metaAdapter: FlusherMetaAdapter[SpoolMeta],
      blobKeyResolver: BlobKeyResolver = PrefixedSequentialBlobKeyResolver
  )(using ec: ExecutionContext): ChunkFlusherFactory =
    DefaultChunkFlusherFactory.fromConfig(
      config = config,
      system = system,
      blobWriter = blobWriter,
      bucket = bucket,
      metaAdapter = metaAdapter,
      blobKeyResolver = blobKeyResolver
    )
}
