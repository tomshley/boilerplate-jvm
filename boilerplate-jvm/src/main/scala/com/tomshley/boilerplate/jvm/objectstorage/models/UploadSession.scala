/*
 * Copyright 2026 Tomshley LLC.
 * All Rights Reserved.
 */

package com.tomshley.boilerplate.jvm.objectstorage.models

/**
 * Active upload session for multipart uploads.
 */
case class UploadSession(
    bucket: String,
    key: String,
    uploadId: String
)
