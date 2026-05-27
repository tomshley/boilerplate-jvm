/*
 * Copyright 2026 Tomshley LLC.
 * All Rights Reserved.
 */

package com.tomshley.boilerplate.jvm.durablebufferedflush

import scala.concurrent.duration.FiniteDuration

/** Raised by [[Workflow.prepareTransfer]] when the [[AdmissionController]]
  * refuses to admit a new session because spool pressure is at the
  * `Critical` level.
  *
  * Carries an operator-suggested `retryAfter` hint so that callers may
  * surface a typed backpressure signal to upstream producers. The
  * underlying transport (TCP error frame, HTTP `503 Retry-After`, gRPC
  * status, etc.) is the consumer's choice — boilerplate-jvm does not
  * prescribe a wire format.
  *
  * This is a runtime exception so that it can flow through `Future.failed`
  * cleanly; consumers MUST handle it explicitly at the workflow boundary
  * and translate it to whatever backpressure idiom their protocol uses.
  *
  * @param retryAfter operator-suggested delay before retrying the transfer */
final class SpoolPressureCriticalException(val retryAfter: FiniteDuration)
    extends RuntimeException(
      s"Spool pressure critical — admission closed for new sessions. Retry after $retryAfter."
    )
    with scala.util.control.NoStackTrace

object SpoolPressureCriticalException {
  def apply(retryAfter: FiniteDuration): SpoolPressureCriticalException =
    new SpoolPressureCriticalException(retryAfter)

  def unapply(ex: SpoolPressureCriticalException): Option[FiniteDuration] =
    Some(ex.retryAfter)
}
