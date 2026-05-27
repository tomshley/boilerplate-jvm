/*
 * Copyright 2026 Tomshley LLC.
 * All Rights Reserved.
 */

package com.tomshley.boilerplate.jvm.durablebufferedflush

import com.tomshley.boilerplate.jvm.durablebufferedflush.internal.FilesystemChunkSpool
import org.apache.pekko.actor.typed.ActorSystem

import java.nio.file.Path
import scala.concurrent.Future

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
  *   - The default implementation fsyncs both file contents and parent directory entries.
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
trait ChunkSpool {

  /** Write chunk bytes to spool and fsync. Returns the number of bytes written.
    *
    * This is the hot-path synchronous durability boundary.
    * The returned Future completes only after fsync — never before.
    *
    * @param entityId  the session entity identifier
    * @param seq       0-indexed chunk sequence number
    * @param bytes     serialized chunk bytes
    * @return bytes written (should equal bytes.length)
    */
  def write(entityId: String, seq: Long, bytes: Array[Byte]): Future[Long]

  /** Read chunk bytes from spool by sequence number.
    *
    * Used by:
    *   - chunk flushing to read chunks for object storage upload
    *   - close retry to resend missing claims from spool
    *   - recovery to resend claims after crash
    *
    * @param entityId  the session entity identifier
    * @param seq       0-indexed chunk sequence number
    * @return raw proto bytes
    */
  def readChunk(entityId: String, seq: Long): Future[Array[Byte]]

  /** Read the current spool metadata for an entity.
    * Returns None if no spool directory or meta.json exists. */
  def readMeta(entityId: String): Future[Option[SpoolMeta]]

  /** Atomically update spool metadata (write temp → fsync → rename).
    *
    * Called after each write() to advance lastSpooledSeq,
    * and by the flusher to advance flushedSeq. */
  def updateMeta(entityId: String, meta: SpoolMeta): Future[Unit]

  /** Advance only the flushed watermark in meta.json.
    *
    * This method must preserve any newer lastSpooledSeq / totalSpooledBytes
    * values written concurrently by the hot path.
    *
    * '''Thread safety:''' In `FilesystemChunkSpool`, correctness depends on
    * per-entity actor serialization between this method and `write()`. Both
    * perform read-modify-write on `meta.json`; without serialization the later
    * writer would silently clobber the earlier writer's field update. */
  def updateFlushedSeq(entityId: String, seq: Long): Future[Unit]

  /** Initialize a new spool directory for an entity.
    * Creates {spoolRoot}/{entityId}/chunks/ and writes initial meta.json.
    * Idempotent — returns existing meta if spool already exists. */
  def initialize(entityId: String, meta: SpoolMeta): Future[SpoolMeta]

  /** Delete the entire spool directory for an entity.
    * Called after Close succeeds and flusher confirms all chunks uploaded.
    * Idempotent — no error if directory doesn't exist. */
  def cleanup(entityId: String): Future[Unit]

  /** List all entity IDs that have spool directories.
    * Used by recovery at startup. */
  def listEntities(): Future[Seq[String]]
}

object ChunkSpool {

  /** Default filesystem-backed [[ChunkSpool]] that also reports its
    * on-disk size, expressed as the intersection type
    * `ChunkSpool & SpoolSizeReporter` so a single concrete instance
    * satisfies both capabilities.
    *
    * Existing consumers that widen the result to `ChunkSpool` continue
    * to compile unchanged — the intersection type is a subtype of
    * `ChunkSpool`. New consumers (notably [[SpoolPressureMonitor]]) may
    * narrow to `SpoolSizeReporter` without performing a runtime cast. */
  def filesystem(system: ActorSystem[?], rootDir: Path): ChunkSpool & SpoolSizeReporter =
    new FilesystemChunkSpool(system, rootDir)
}
