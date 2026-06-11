/*
 * Copyright 2026 Tomshley LLC.
 * All Rights Reserved.
 */

package com.tomshley.boilerplate.jvm.utils

import org.bouncycastle.crypto.digests.SHA256Digest

import java.util.HexFormat

object RestorableDigestUtil extends RestorableDigestUtil

/** Incremental SHA-256 with restorable midstate, expressed as pure
  * hex-string transitions — the checkpointable complement to
  * [[ChecksumUtil]]'s one-shot digests.
  *
  * Use when a digest must survive a process restart: fold bytes as they
  * arrive, persist the returned midstate hex alongside the data it covers
  * (atomically, so state and coverage can never disagree), and restore by
  * passing the persisted hex back in. The checkpoint is O(1) per fold
  * (~100 bytes of encoded state) and the resume is O(1) regardless of how
  * many bytes were previously folded.
  *
  * Every function constructs a fresh BouncyCastle [[SHA256Digest]], applies
  * one transition, encodes the result, and discards the digest object — the
  * digest's internal mutability never escapes a single call frame.
  *
  * BouncyCastle (lightweight API, no JCE provider registration) is required
  * because the JDK [[java.security.MessageDigest]] cannot export or restore
  * midstate; BC's `getEncodedState()` / state-constructor pair exists for
  * exactly this purpose. The dependency group is centralized in magicroot's
  * `cryptoLibraries` (LibProjectCryptoPlugin).
  *
  * The encoded-state hex is BouncyCastle's serialization format — treat it
  * as opaque and version-coupled to bcprov; it is a resumable checkpoint,
  * not an interchange format.
  */
trait RestorableDigestUtil {

  private val hex = HexFormat.of()

  private def restore(stateHex: Option[String]): SHA256Digest =
    stateHex.fold(new SHA256Digest())(state => new SHA256Digest(hex.parseHex(state)))

  /** Fold bytes into the digest state.
    *
    * @param previousStateHex `None` = fresh digest (no bytes folded yet)
    * @return the encoded midstate AFTER folding `bytes`
    * @throws IllegalArgumentException if `previousStateHex` is not valid hex
    */
  def sha256FoldHex(previousStateHex: Option[String], bytes: Array[Byte]): String = {
    val digest = restore(previousStateHex)
    digest.update(bytes, 0, bytes.length)
    hex.formatHex(digest.getEncodedState)
  }

  /** Finish the digest from a midstate and return the content hash hex.
    *
    * `None` yields the SHA-256 of the empty byte sequence — the correct
    * verdict for content that declared zero bytes.
    *
    * @throws IllegalArgumentException if `stateHex` is not valid hex
    */
  def sha256DigestHex(stateHex: Option[String]): String = {
    val digest = restore(stateHex)
    val out = new Array[Byte](digest.getDigestSize)
    digest.doFinal(out, 0)
    hex.formatHex(out)
  }
}
