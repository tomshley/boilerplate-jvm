/*
 * Copyright 2026 Tomshley LLC.
 * All Rights Reserved.
 */

package com.tomshley.boilerplate.jvm.objectstorage.models

/**
 * ETag for an uploaded part.
 */
case class PartETag(
    partNo: Int,
    etag: String,
    sizeBytes: Long
)
