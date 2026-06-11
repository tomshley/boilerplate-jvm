/*
 * Copyright 2026 Tomshley LLC.
 * All Rights Reserved.
 */

package com.tomshley.boilerplate.jvm.durablebufferedflush

import org.apache.pekko.util.Timeout

import scala.concurrent.Future

trait SessionPort[Device, Summary] {
  /** @param fileName producer-declared file name when the transport
    *                 carries one — see
    *                 [[FlushTransferDescriptor.fileName]]. Implementations
    *                 that have no use for it may ignore it. */
  def register(
      entityId: String,
      device: Device,
      deviceCorrelationId: String,
      objectHashHex: String,
      declaredPayloadSize: Long,
      fileName: Option[String] = None
  )(using Timeout): Future[Summary]

  def inspect(entityId: String)(using Timeout): Future[Summary]

  def abort(entityId: String, reason: String)(using Timeout): Future[Summary]

  def closeWithValidation(
      entityId: String,
      expectedClaimsCount: Long,
      expectedTotalBytes: Long,
      expectedLastSequence: Long
  )(using Timeout): Future[Summary]

  def toSessionView(summary: Summary): SessionView[Device]
}
