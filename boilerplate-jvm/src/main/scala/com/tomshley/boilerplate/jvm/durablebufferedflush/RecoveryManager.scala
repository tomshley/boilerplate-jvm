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

import com.tomshley.boilerplate.jvm.durablebufferedflush.internal.RecoveryManagerImpl
import org.apache.pekko.actor.typed.ActorSystem
import scala.concurrent.Future

trait RecoveryManager {
  def recover(): Future[RecoveryReport]
}

object RecoveryManager {

  /** Build a recovery manager backed by the default implementation.
    *
    * The returned value satisfies both [[RecoveryManager]] (one-shot startup
    * recovery) and [[OrphanReconciler]] (steady-state reconciliation used by
    * [[OrphanSpoolSweeper]]). Callers that only need recovery may widen to
    * `RecoveryManager`; callers that need the sweeper may widen to
    * `OrphanReconciler`; nothing forces a caller to know about both. */
  def fromConfig[Device, Summary, Envelope, ReplyBinding](
      spool: ChunkSpool,
      flusherFactory: ChunkFlusherFactory,
      sessionPort: SessionPort[Device, Summary],
      claimPort: ClaimPort[Envelope, ReplyBinding],
      config: FlushConfig,
      system: ActorSystem[?]
  ): RecoveryManager & OrphanReconciler =
    new RecoveryManagerImpl(
      spool = spool,
      flusherFactory = flusherFactory,
      sessionPort = sessionPort,
      claimPort = claimPort,
      config = config,
      system = system
    )
}
