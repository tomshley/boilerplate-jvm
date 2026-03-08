/*
 * Copyright 2026 Tomshley LLC.
 * All Rights Reserved.
 */

package com.tomshley.boilerplate.jvm.objectstorage

/**
 * Configuration for S3 BlobStore implementation.
 * 
 * @param region AWS region (e.g., "us-east-1")
 * @param accessKeyId AWS access key ID
 * @param secretAccessKey AWS secret access key
 * @param endpoint Optional custom endpoint (for MinIO, local dev)
 * @param pathStyleAccess Use path-style access (for MinIO, local dev)
 */
case class S3BlobStoreConfig(
    region: String,
    accessKeyId: Option[String] = None,
    secretAccessKey: Option[String] = None,
    endpoint: Option[String] = None,
    pathStyleAccess: Boolean = false
) {
  override def toString: String = {
    val maskedKeyId = accessKeyId match {
      case Some(keyId) if keyId.length > 4 => s"****${keyId.takeRight(4)}"
      case Some(_) => "****"
      case None => "<none>"
    }
    s"S3BlobStoreConfig(region=$region, accessKeyId=$maskedKeyId, secretAccessKey=<redacted>, endpoint=$endpoint, pathStyleAccess=$pathStyleAccess)"
  }
}
