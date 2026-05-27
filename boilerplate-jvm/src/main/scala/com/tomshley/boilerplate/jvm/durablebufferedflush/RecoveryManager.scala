/*
 * Copyright 2026 Tomshley LLC.
 * All Rights Reserved.
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
