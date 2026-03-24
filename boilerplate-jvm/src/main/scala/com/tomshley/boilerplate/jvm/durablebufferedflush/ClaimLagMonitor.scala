/*
 * Copyright 2026 Tomshley LLC.
 * All Rights Reserved.
 */

package com.tomshley.boilerplate.jvm.durablebufferedflush

import org.apache.pekko.actor.typed.{ActorSystem, Behavior}
import org.apache.pekko.actor.typed.scaladsl.Behaviors

import java.util.UUID
import scala.concurrent.{ExecutionContext, Future, Promise}

final case class ClaimLagSnapshot(
    spooledSeq: Long,
    lastClaimAttemptedSeq: Long,
    lastClaimConfirmedSeq: Long,
    claimErrorCount: Long,
    claimLagSoft: Long,
    claimLagHard: Long,
    isPaused: Boolean
) {
  def claimLag: Long =
    math.max(0L, spooledSeq - lastClaimConfirmedSeq)

  def inflightClaims: Long =
    math.max(0L, lastClaimAttemptedSeq - lastClaimConfirmedSeq)

  def shouldPause: Boolean =
    claimLag > claimLagHard

  def shouldResume: Boolean =
    claimLag < claimLagSoft
}

private object ClaimLagMonitorActor {
  sealed trait Command

  final case class OnSpooled(seq: Long, reply: Promise[Unit]) extends Command

  final case class OnClaimAttempted(seq: Long, reply: Promise[Unit]) extends Command

  final case class OnClaimConfirmed(claimsCount: Long, reply: Promise[Unit]) extends Command

  final case class OnClaimError(reply: Promise[Unit]) extends Command

  final case class PauseIfNeeded(reply: Promise[Option[Future[Unit]]]) extends Command

  final case class CheckResume(reply: Promise[Unit]) extends Command

  final case class CancelPause(cause: Throwable, reply: Promise[Unit]) extends Command

  final case class Reset(reply: Promise[Unit]) extends Command

  final case class Snapshot(reply: Promise[ClaimLagSnapshot]) extends Command

  case object Stop extends Command

  final case class State(
      spooledSeq: Long = -1L,
      lastClaimAttemptedSeq: Long = -1L,
      lastClaimConfirmedSeq: Long = -1L,
      claimErrorCount: Long = 0L,
      pausePromise: Option[Promise[Unit]] = None
  ) {
    def snapshot(claimLagSoft: Long, claimLagHard: Long): ClaimLagSnapshot =
      ClaimLagSnapshot(
        spooledSeq = spooledSeq,
        lastClaimAttemptedSeq = lastClaimAttemptedSeq,
        lastClaimConfirmedSeq = lastClaimConfirmedSeq,
        claimErrorCount = claimErrorCount,
        claimLagSoft = claimLagSoft,
        claimLagHard = claimLagHard,
        isPaused = pausePromise.isDefined
      )
  }

  def apply(claimLagSoft: Long, claimLagHard: Long): Behavior[Command] =
    active(claimLagSoft, claimLagHard, State())

  private def validateSequence(name: String, seq: Long): Unit =
    require(seq >= 0L, s"$name must be >= 0: $seq")

  private def validateClaimsCount(claimsCount: Long): Unit =
    require(claimsCount >= 0L, s"claimsCount must be >= 0: $claimsCount")

  private def currentLag(state: State): Long =
    math.max(0L, state.spooledSeq - state.lastClaimConfirmedSeq)

  private def maybeResume(claimLagSoft: Long, state: State): State =
    state.pausePromise match {
      case Some(pause) if currentLag(state) < claimLagSoft =>
        pause.trySuccess(())
        state.copy(pausePromise = None)
      case _ =>
        state
    }

  private def activePauseIfNeeded(claimLagHard: Long, state: State): (State, Option[Future[Unit]]) =
    state.pausePromise match {
      case Some(existing) =>
        state -> Some(existing.future)
      case None if currentLag(state) <= claimLagHard =>
        state -> None
      case None =>
        val pause = Promise[Unit]()
        state.copy(pausePromise = Some(pause)) -> Some(pause.future)
    }

  private def active(
      claimLagSoft: Long,
      claimLagHard: Long,
      state: State
  ): Behavior[Command] =
    Behaviors.receiveMessage {
      case OnSpooled(seq, reply) =>
        try {
          validateSequence("spooled seq", seq)
          reply.trySuccess(())
          active(claimLagSoft, claimLagHard, state.copy(spooledSeq = math.max(state.spooledSeq, seq)))
        } catch {
          case ex: Throwable =>
            reply.tryFailure(ex)
            Behaviors.same
        }

      case OnClaimAttempted(seq, reply) =>
        try {
          validateSequence("claim attempt seq", seq)
          reply.trySuccess(())
          active(
            claimLagSoft,
            claimLagHard,
            state.copy(lastClaimAttemptedSeq = math.max(state.lastClaimAttemptedSeq, seq))
          )
        } catch {
          case ex: Throwable =>
            reply.tryFailure(ex)
            Behaviors.same
        }

      case OnClaimConfirmed(claimsCount, reply) =>
        try {
          validateClaimsCount(claimsCount)
          val confirmedSeq = claimsCount - 1L
          val resumed = maybeResume(
            claimLagSoft,
            state.copy(lastClaimConfirmedSeq = math.max(state.lastClaimConfirmedSeq, confirmedSeq))
          )
          reply.trySuccess(())
          active(claimLagSoft, claimLagHard, resumed)
        } catch {
          case ex: Throwable =>
            reply.tryFailure(ex)
            Behaviors.same
        }

      case OnClaimError(reply) =>
        reply.trySuccess(())
        active(claimLagSoft, claimLagHard, state.copy(claimErrorCount = state.claimErrorCount + 1L))

      case PauseIfNeeded(reply) =>
        val (nextState, pauseFuture) = activePauseIfNeeded(claimLagHard, state)
        reply.trySuccess(pauseFuture)
        active(claimLagSoft, claimLagHard, nextState)

      case CheckResume(reply) =>
        reply.trySuccess(())
        active(claimLagSoft, claimLagHard, maybeResume(claimLagSoft, state))

      case CancelPause(cause, reply) =>
        state.pausePromise.foreach(_.tryFailure(cause))
        reply.trySuccess(())
        active(claimLagSoft, claimLagHard, state.copy(pausePromise = None))

      case Reset(reply) =>
        state.pausePromise.foreach(_.trySuccess(()))
        reply.trySuccess(())
        active(claimLagSoft, claimLagHard, State())

      case Snapshot(reply) =>
        reply.trySuccess(state.snapshot(claimLagSoft, claimLagHard))
        Behaviors.same

      case Stop =>
        state.pausePromise.foreach(_.tryFailure(new IllegalStateException("ClaimLagMonitor stopped")))
        Behaviors.stopped
    }
}

final class ClaimLagMonitor(
    val claimLagSoft: Long,
    val claimLagHard: Long
) (using system: ActorSystem[?]) {

  require(claimLagSoft > 0, s"claimLagSoft must be > 0: $claimLagSoft")
  require(
    claimLagHard > claimLagSoft,
    s"claimLagHard ($claimLagHard) must be > claimLagSoft ($claimLagSoft)"
  )

  private given ExecutionContext = system.executionContext

  private val monitor = system.systemActorOf(
    ClaimLagMonitorActor(claimLagSoft, claimLagHard),
    s"claim-lag-monitor-${UUID.randomUUID()}"
  )

  def onSpooled(seq: Long): Future[Unit] = {
    val promise = Promise[Unit]()
    monitor ! ClaimLagMonitorActor.OnSpooled(seq, promise)
    promise.future
  }

  def onClaimAttempted(seq: Long): Future[Unit] = {
    val promise = Promise[Unit]()
    monitor ! ClaimLagMonitorActor.OnClaimAttempted(seq, promise)
    promise.future
  }

  def onClaimConfirmed(claimsCount: Long): Future[Unit] = {
    val promise = Promise[Unit]()
    monitor ! ClaimLagMonitorActor.OnClaimConfirmed(claimsCount, promise)
    promise.future
  }

  def onClaimError(): Future[Unit] = {
    val promise = Promise[Unit]()
    monitor ! ClaimLagMonitorActor.OnClaimError(promise)
    promise.future
  }

  def snapshot(): Future[ClaimLagSnapshot] = {
    val promise = Promise[ClaimLagSnapshot]()
    monitor ! ClaimLagMonitorActor.Snapshot(promise)
    promise.future
  }

  def spooledSeq: Future[Long] =
    snapshot().map(_.spooledSeq)

  def lastClaimAttemptedSeq: Future[Long] =
    snapshot().map(_.lastClaimAttemptedSeq)

  def lastClaimConfirmedSeq: Future[Long] =
    snapshot().map(_.lastClaimConfirmedSeq)

  def claimErrorCount: Future[Long] =
    snapshot().map(_.claimErrorCount)

  def claimLag: Future[Long] =
    snapshot().map(_.claimLag)

  def inflightClaims: Future[Long] =
    snapshot().map(_.inflightClaims)

  def shouldPause: Future[Boolean] =
    snapshot().map(_.shouldPause)

  def shouldResume: Future[Boolean] =
    snapshot().map(_.shouldResume)

  def isPaused: Future[Boolean] =
    snapshot().map(_.isPaused)

  def pauseIfNeeded(): Future[Option[Future[Unit]]] = {
    val promise = Promise[Option[Future[Unit]]]()
    monitor ! ClaimLagMonitorActor.PauseIfNeeded(promise)
    promise.future
  }

  def enterPause(): Future[Option[Future[Unit]]] =
    pauseIfNeeded()

  def checkResume(): Future[Unit] = {
    val promise = Promise[Unit]()
    monitor ! ClaimLagMonitorActor.CheckResume(promise)
    promise.future
  }

  def cancelPause(cause: Throwable): Future[Unit] = {
    val promise = Promise[Unit]()
    monitor ! ClaimLagMonitorActor.CancelPause(cause, promise)
    promise.future
  }

  def reset(): Future[Unit] = {
    val promise = Promise[Unit]()
    monitor ! ClaimLagMonitorActor.Reset(promise)
    promise.future
  }

  def stop(): Unit =
    monitor ! ClaimLagMonitorActor.Stop
}

object ClaimLagMonitor {
  def apply(claimLagSoft: Long, claimLagHard: Long)(using ActorSystem[?]): ClaimLagMonitor = {
    new ClaimLagMonitor(claimLagSoft, claimLagHard)
  }
}
