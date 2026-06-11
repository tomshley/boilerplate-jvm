/*
 * Copyright 2026 Tomshley LLC.
 * All Rights Reserved.
 */

package com.tomshley.boilerplate.jvm.durablebufferedflush

import com.fasterxml.jackson.annotation.JsonIgnore
import java.time.Instant

/** Default metadata persisted as `meta.json` in each entity's spool directory.
  *
  * Updated atomically: write temp file → fsync → rename.
  *
  * This is local spool bookkeeping, not the authoritative domain state.
  * Recovery reads it to reconcile the filesystem spool against the caller's
  * own durable session state.
  *
  * This model is intentionally the default sequential chunk-session contract
  * used by `FilesystemChunkSpool` and the default blob flusher path. The
  * flusher seam itself remains adapter-based (`FlusherMetaAdapter`), but this
  * concrete metadata shape is not pretending to be transport-agnostic.
  *
  * Layout:
  * {{{
  *   {spoolRoot}/{entityId}/
  *     chunks/
  *       000000000.bin        ← serialized chunk bytes
  *       000000001.bin
  *       ...
  *     meta.json              ← this model
  * }}}
  */
final case class SpoolMeta(
    entityId: String,
    deviceId: String,
    objectHashHex: String,
    lastSpooledSeq: Long,
    totalSpooledBytes: Long,
    flushedSeq: Long,
    declaredPayloadSize: Long,
    totalExpectedChunks: Long,
    createdAt: Instant,
    digestStateHex: Option[String] = None
) {

  /** Whether all expected chunks have been spooled */
  @JsonIgnore
  def isComplete: Boolean = lastSpooledSeq + 1 >= totalExpectedChunks

  /** Whether flusher has caught up to the spool */
  @JsonIgnore
  def isFlushed: Boolean = flushedSeq >= lastSpooledSeq

  /** Whether the digest midstate covers every spooled chunk.
    *
    * The midstate is advanced in the SAME atomic meta rename as
    * `lastSpooledSeq`, so within one version the two cannot diverge. The
    * only uncovered shape is a spool written by a pre-digest version:
    * chunks exist (`lastSpooledSeq >= 0`) with no recorded state. Folding
    * later chunks into a fresh digest would produce a silently wrong hash,
    * so coverage is surfaced as a fact and verification reports
    * [[FinalizeOutcome.UnverifiedReason.NoDigestCoverage]] instead. */
  @JsonIgnore
  def hasDigestCoverage: Boolean = digestStateHex.isDefined || lastSpooledSeq < 0L

  /** Number of chunks not yet flushed to blob storage */
  @JsonIgnore
  def flushLag: Long = lastSpooledSeq - flushedSeq

  /** Advance the spooled sequence and accumulate bytes */
  def withSpooled(seq: Long, chunkBytes: Long): SpoolMeta =
    copy(
      lastSpooledSeq = seq,
      totalSpooledBytes = totalSpooledBytes + chunkBytes
    )

  /** Advance the flushed watermark */
  def withFlushed(seq: Long): SpoolMeta =
    copy(flushedSeq = seq)

  /** Replace the digest midstate checkpoint. `None` records (and
    * propagates) the absence of coverage — see [[hasDigestCoverage]]. */
  def withDigestState(stateHex: Option[String]): SpoolMeta =
    copy(digestStateHex = stateHex)
}

object SpoolMeta {

  /** Initial meta for a new spool session.
    * lastSpooledSeq and flushedSeq start at -1 (nothing spooled/flushed yet).
    * Sequences are 0-indexed to match entity convention. */
  def initial(
      entityId: String,
      deviceId: String,
      objectHashHex: String,
      declaredPayloadSize: Long,
      totalExpectedChunks: Long
  ): SpoolMeta = SpoolMeta(
    entityId = entityId,
    deviceId = deviceId,
    objectHashHex = objectHashHex,
    lastSpooledSeq = -1L,
    totalSpooledBytes = 0L,
    flushedSeq = -1L,
    declaredPayloadSize = declaredPayloadSize,
    totalExpectedChunks = totalExpectedChunks,
    createdAt = Instant.now()
  )
}
