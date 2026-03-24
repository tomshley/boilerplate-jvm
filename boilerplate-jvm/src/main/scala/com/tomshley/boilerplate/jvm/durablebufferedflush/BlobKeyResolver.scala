/*
 * Copyright 2026 Tomshley LLC.
 * All Rights Reserved.
 */

package com.tomshley.boilerplate.jvm.durablebufferedflush

final case class BlobKeyPrefix(value: String) extends AnyVal

trait BlobKeyResolver {
  def chunkKey(blobKeyPrefix: BlobKeyPrefix, sequence: Long): String
}

object PrefixedSequentialBlobKeyResolver extends BlobKeyResolver {
  override def chunkKey(blobKeyPrefix: BlobKeyPrefix, sequence: Long): String =
    f"${blobKeyPrefix.value}/$sequence%09d"
}
