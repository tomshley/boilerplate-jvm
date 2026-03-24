/*
 * Copyright 2026 Tomshley LLC.
 * All Rights Reserved.
 */

package com.tomshley.boilerplate.jvm.durablebufferedflush

final case class FlusherMetaView(
    lastSpooledSeq: Long,
    blobKeyPrefix: BlobKeyPrefix
)

trait FlusherMetaAdapter[M] {
  def toFlusherMetaView(meta: M): FlusherMetaView
}

final class SpoolMetaFlusherAdapter(
    prefixBuilder: SpoolMeta => BlobKeyPrefix
) extends FlusherMetaAdapter[SpoolMeta] {
  override def toFlusherMetaView(meta: SpoolMeta): FlusherMetaView =
    FlusherMetaView(
      lastSpooledSeq = meta.lastSpooledSeq,
      blobKeyPrefix = prefixBuilder(meta)
    )
}

object SpoolMetaFlusherAdapter {
  def apply(prefixBuilder: SpoolMeta => BlobKeyPrefix): SpoolMetaFlusherAdapter =
    new SpoolMetaFlusherAdapter(prefixBuilder)
}
