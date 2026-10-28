/*
 * copyright 2026 tomshley llc
 *
 * licensed under the apache license, version 2.0 (the "license");
 * you may not use this file except in compliance with the license.
 * you may obtain a copy of the license at
 *
 * http://www.apache.org/licenses/license-2.0
 *
 * unless required by applicable law or agreed to in writing, software
 * distributed under the license is distributed on an "as is" basis,
 * without warranties or conditions of any kind, either express or implied.
 * see the license for the specific language governing permissions and
 * limitations under the license.
 *
 * @author thomas schena @sgoggles <https://github.com/sgoggles> | <https://gitlab.com/sgoggles>
 *
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
