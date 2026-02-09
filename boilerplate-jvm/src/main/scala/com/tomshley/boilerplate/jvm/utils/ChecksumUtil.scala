package com.tomshley.boilerplate.jvm.utils

import java.security.MessageDigest

object ChecksumUtil extends ChecksumUtil

trait ChecksumUtil {
  def toMD5(contents: String): String = {
    MessageDigest.getInstance("MD5").digest(contents.getBytes("UTF-8")).map(0xFF & _).map {
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
