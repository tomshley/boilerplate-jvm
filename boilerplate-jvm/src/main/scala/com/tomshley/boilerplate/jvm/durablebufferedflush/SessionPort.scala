/*
 * Copyright 2026 Tomshley LLC.
 * All Rights Reserved.
 */

package com.tomshley.boilerplate.jvm.durablebufferedflush

import org.apache.pekko.util.Timeout

import scala.concurrent.Future

trait SessionPort[Device, Summary] {
  def register(
      entityId: String,
      device: Device,
      deviceCorrelationId: String,
      objectHashHex: String,
      declaredPayloadSize: Long
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
