/*
 * Copyright 2026 Tomshley LLC.
 * All Rights Reserved.
 */

package com.tomshley.boilerplate.jvm.durablebufferedflush

sealed abstract class CloseValidationFailure(
    val code: String,
    detail: String
) extends RuntimeException(s"$code: $detail")

object CloseValidationFailure {

  private object Code {
    val SessionNotOpen = "close_validation.session_not_open"
    val MissingValidationFields = "close_validation.missing_validation_fields"
    val ClaimsCountMismatch = "close_validation.claims_count_mismatch"
    val BytesMismatch = "close_validation.bytes_mismatch"
    val SequenceMismatch = "close_validation.sequence_mismatch"
    val ChunkAddressMissing = "close_validation.chunk_address_missing"
    val RequiredSessionFieldsMissing = "close_validation.required_session_fields_missing"
  }

  sealed trait Classification

  object Classification {
    case object RecoverableResend extends Classification
    case object Fatal extends Classification
    case object Unknown extends Classification
  }

  case object SessionNotOpen extends CloseValidationFailure(
    Code.SessionNotOpen,
    "Session not open"
  )

  case object MissingValidationFields extends CloseValidationFailure(
    Code.MissingValidationFields,
    "Close validation requires expectedClaimsCount, expectedTotalBytes, and expectedLastSequence together"
  )

  final case class ClaimsCountMismatch(entity: Long, expected: Long)
      extends CloseValidationFailure(
        Code.ClaimsCountMismatch,
        s"Claims count mismatch: entity=$entity, expected=$expected"
      )

  final case class BytesMismatch(entity: Long, expected: Long)
      extends CloseValidationFailure(
        Code.BytesMismatch,
        s"Bytes mismatch: entity=$entity, expected=$expected"
      )

  final case class SequenceMismatch(entity: Long, expected: Long)
      extends CloseValidationFailure(
        Code.SequenceMismatch,
        s"Sequence mismatch: entity=$entity, expected=$expected"
      )

  case object ChunkAddressMissing extends CloseValidationFailure(
    Code.ChunkAddressMissing,
    "ChunkAddress missing at Close time — session was opened without device identity"
  )

  case object RequiredSessionFieldsMissing extends CloseValidationFailure(
    Code.RequiredSessionFieldsMissing,
    "Required session fields missing at Close time — session state is inconsistent"
  )

  private def classifyCode(code: String): Classification =
    code match {
      case Code.ClaimsCountMismatch | Code.SequenceMismatch => Classification.RecoverableResend
      case Code.BytesMismatch |
          Code.SessionNotOpen |
          Code.MissingValidationFields |
          Code.ChunkAddressMissing |
          Code.RequiredSessionFieldsMissing => Classification.Fatal
      case _ => Classification.Unknown
    }

  private def classifyMessage(message: String): Classification = {
    val codeClassification =
      message.indexOf(": ") match {
        case idx if idx > 0 => classifyCode(message.substring(0, idx))
        case _ => Classification.Unknown
      }

    if (codeClassification != Classification.Unknown) {
      codeClassification
    } else if (
      message.startsWith("Claims count mismatch:") ||
      message.startsWith("Sequence mismatch:")
    ) {
      Classification.RecoverableResend
    } else if (
      message.startsWith("Bytes mismatch:") ||
      message == "Session not open" ||
      message == "Close validation requires expectedClaimsCount, expectedTotalBytes, and expectedLastSequence together" ||
      message == "ChunkAddress missing at Close time — session was opened without device identity" ||
      message == "Required session fields missing at Close time — session state is inconsistent"
    ) {
      Classification.Fatal
    } else {
      Classification.Unknown
    }
  }

  def classify(ex: Throwable): Classification = ex match {
    case _: ClaimsCountMismatch => Classification.RecoverableResend
    case _: SequenceMismatch => Classification.RecoverableResend
    case _: BytesMismatch => Classification.Fatal
    case SessionNotOpen => Classification.Fatal
    case MissingValidationFields => Classification.Fatal
    case ChunkAddressMissing => Classification.Fatal
    case RequiredSessionFieldsMissing => Classification.Fatal
    case _: IllegalStateException => Classification.Fatal
    case _: IllegalArgumentException => Classification.Fatal
    case other =>
      Option(other.getMessage).map(classifyMessage).getOrElse(Classification.Unknown)
  }

  def isFatal(ex: Throwable): Boolean =
    classify(ex) match {
      case Classification.RecoverableResend => false
      case Classification.Fatal => true
      case Classification.Unknown => false
    }
}
