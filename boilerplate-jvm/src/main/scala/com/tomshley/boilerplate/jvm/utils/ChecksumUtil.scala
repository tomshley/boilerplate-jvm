package com.tomshley.boilerplate.jvm.utils

import java.security.MessageDigest

object ChecksumUtil extends ChecksumUtil

trait ChecksumUtil {
  /** MD5 hex digest — suitable for integrity checks and fingerprinting, not for security hashing. */
  def toMD5(contents: String): String = {
    MessageDigest.getInstance("MD5").digest(contents.getBytes("UTF-8")).map(0xFF & _).map {
      "%02x".format(_)
    }.foldLeft("") {
      _ + _
    }
  }

  /** SHA-256 hex digest — use when a cryptographically strong hash is required. */
  def toSHA256(contents: String): String = {
    MessageDigest.getInstance("SHA-256").digest(contents.getBytes("UTF-8")).map(0xFF & _).map {
      "%02x".format(_)
    }.foldLeft("") {
      _ + _
    }
  }

  def computeCrc32(data: Array[Byte]): Long = {
    val crc = new java.util.zip.CRC32()
    crc.update(data)
    crc.getValue
  }
}
