package com.tomshley.boilerplate.jvm.kafka.exceptions

final class KafkaTombstoneException(
  val key: String,
  val partition: Int,
  val offset: Long
) extends RuntimeException(
  s"Tombstone record cannot be deserialized (key=$key, partition=$partition, offset=$offset)"
)
