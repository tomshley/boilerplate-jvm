/*
 * Copyright 2026 Tomshley LLC.
 * All Rights Reserved.
 */

package com.tomshley.boilerplate.jvm.objectstorage.models

/**
 * Metadata about a stored object.
 */
case class ObjectMetadata(
    contentType: Option[String],
    contentLength: Long,
    metadata: Map[String, String] = Map.empty
)
