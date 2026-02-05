/*
 * Copyright 2026 Tomshley LLC.
 * All Rights Reserved.
 */

package com.tomshley.boilerplate.jvm.objectstorage.models

/**
 * Reference to a stored blob - the "claim ticket" for object storage.
 * 
 * Contains all information needed to retrieve or reference the stored object.
 */
case class BlobReference(
    bucket: String,
    key: String,
    etag: String,
    checksum: String,
    sizeBytes: Long
) {
  /** Full S3-style URI */
  def uri: String = s"s3://$bucket/$key"
}
