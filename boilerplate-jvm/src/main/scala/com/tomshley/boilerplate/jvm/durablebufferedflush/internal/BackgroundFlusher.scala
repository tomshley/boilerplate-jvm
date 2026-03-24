/*
 * Copyright 2026 Tomshley LLC.
 * All Rights Reserved.
 */

package com.tomshley.boilerplate.jvm.durablebufferedflush.internal

import com.tomshley.boilerplate.jvm.durablebufferedflush.{BlobKeyPrefix, BlobKeyResolver, ChunkFlusher, ChunkFlusherFactory, ChunkSpool, FlusherMetaAdapter, PrefixedSequentialBlobKeyResolver, SpoolMeta}
import com.tomshley.boilerplate.jvm.objectstorage.SingleShotBlobWriter
import com.typesafe.config.Config
import org.apache.pekko.actor.typed.{ActorSystem, Behavior}
import org.apache.pekko.actor.typed.scaladsl.{Behaviors, TimerScheduler}
import org.apache.pekko.pattern.after

import java.util.UUID
import scala.collection.immutable.TreeSet
import scala.concurrent.{ExecutionContext, Future, Promise, TimeoutException}
import scala.concurrent.duration.*
import scala.util.{Failure, Success, Try}
import scala.util.control.NonFatal

private object DefaultChunkFlusherActor {
  sealed trait Command

  case object Start extends Command

  case object Stop extends Command

  final case class Drain(reply: Promise[Long]) extends Command

  case object PollTick extends Command

  final case class MetaReadCompleted(result: Try[Option[SpoolMeta]], forDrain: Boolean, generation: Option[Long]) extends Command

  final case class UploadCompleted(seq: Long, result: Try[Long]) extends Command

  final case class WatermarkPersisted(result: Try[Unit]) extends Command

  final case class DrainTimedOut(generation: Long) extends Command

  final case class GetFlushedSeq(reply: Promise[Long]) extends Command

  final case class GetIsRunning(reply: Promise[Boolean]) extends Command

  private case object PollTimerKey

  private case object DrainTimerKey

  final case class Runtime(
      entityId: String,
      startSeq: Long,
      parallelism: Int,
      tickInterval: FiniteDuration,
      drainTimeout: FiniteDuration,
      readMeta: () => Future[Option[SpoolMeta]],
      updateFlushedSeq: Long => Future[Unit],
      uploadWithRetry: (Long, BlobKeyPrefix) => Future[Long],
      metaAdapter: FlusherMetaAdapter[SpoolMeta]
  )

  final case class State(
      flushedSeq: Long,
      nextSeqToUpload: Long,
      highestObservedSpooledSeq: Long,
      isRunning: Boolean = false,
      pollInProgress: Boolean = false,
      inFlightSeqs: Set[Long] = Set.empty,
      completedSeqs: TreeSet[Long] = TreeSet.empty,
      pendingMetaWrites: Int = 0,
      drainWaiters: Vector[Promise[Long]] = Vector.empty,
      drainTargetSeq: Option[Long] = None,
      nextDrainGeneration: Long = 0L,
      activeDrainGeneration: Option[Long] = None,
      failureCause: Option[Throwable] = None,
      blobKeyPrefix: Option[BlobKeyPrefix] = None
  )

  def apply(runtime: Runtime): Behavior[Command] =
    Behaviors.withTimers { timers =>
      active(
        runtime,
        timers,
        State(
          flushedSeq = runtime.startSeq - 1L,
          nextSeqToUpload = runtime.startSeq,
          highestObservedSpooledSeq = runtime.startSeq - 1L
        )
      )
    }

  private def active(
      runtime: Runtime,
      timers: TimerScheduler[Command],
      state: State
  ): Behavior[Command] =
    Behaviors.receive { (context, message) =>
      message match {
        case Start =>
          if (state.failureCause.isDefined || state.isRunning) {
            Behaviors.same
          } else {
            val nextState = state.copy(isRunning = true)
            startPollingIfNeeded(runtime, timers, context, nextState)
            active(runtime, timers, nextState)
          }

        case Stop =>
          active(runtime, timers, failFlusher(runtime, timers, state, new IllegalStateException(s"Chunk flusher stopped for entity ${runtime.entityId}")))

        case Drain(reply) =>
          state.failureCause match {
            case Some(ex) =>
              reply.tryFailure(ex)
              Behaviors.same
            case None if state.drainWaiters.nonEmpty =>
              active(runtime, timers, state.copy(drainWaiters = state.drainWaiters :+ reply))
            case None =>
              val generation = state.nextDrainGeneration + 1L
              timers.cancel(PollTimerKey)
              timers.startSingleTimer(DrainTimerKey, DrainTimedOut(generation), runtime.drainTimeout)
              context.pipeToSelf(runtime.readMeta())(result => MetaReadCompleted(result, forDrain = true, generation = Some(generation)))
              active(
                runtime,
                timers,
                state.copy(
                  isRunning = true,
                  drainWaiters = Vector(reply),
                  nextDrainGeneration = generation,
                  activeDrainGeneration = Some(generation),
                  drainTargetSeq = None
                )
              )
          }

        case PollTick =>
          if (!state.isRunning || state.failureCause.isDefined || state.pollInProgress || state.drainWaiters.nonEmpty) {
            Behaviors.same
          } else {
            context.pipeToSelf(runtime.readMeta())(result => MetaReadCompleted(result, forDrain = false, generation = None))
            active(runtime, timers, state.copy(pollInProgress = true))
          }

        case MetaReadCompleted(result, forDrain, generation) =>
          if (state.failureCause.isDefined || (forDrain && state.activeDrainGeneration != generation)) {
            Behaviors.same
          } else {
            val readState = if (forDrain) state else state.copy(pollInProgress = false)
            result match {
              case Success(Some(meta)) =>
                val flusherMeta = runtime.metaAdapter.toFlusherMetaView(meta)
                val updatedState = readState.copy(
                  blobKeyPrefix = readState.blobKeyPrefix.orElse(Some(flusherMeta.blobKeyPrefix)),
                  highestObservedSpooledSeq = math.max(readState.highestObservedSpooledSeq, flusherMeta.lastSpooledSeq),
                  drainTargetSeq = if (forDrain) Some(flusherMeta.lastSpooledSeq) else readState.drainTargetSeq
                )
                val launchedState = launchAvailableUploads(runtime, context, updatedState)
                active(runtime, timers, settleDrain(timers, launchedState))
              case Success(None) =>
                active(runtime, timers, failFlusher(runtime, timers, readState, new IllegalStateException(s"Spool metadata missing for entity ${runtime.entityId}")))
              case Failure(ex) =>
                active(runtime, timers, failFlusher(runtime, timers, readState, ex))
            }
          }

        case UploadCompleted(seq, result) =>
          if (state.failureCause.isDefined) {
            active(runtime, timers, state.copy(inFlightSeqs = state.inFlightSeqs - seq))
          } else {
            result match {
              case Success(_) =>
                val baseState = state.copy(
                  inFlightSeqs = state.inFlightSeqs - seq,
                  completedSeqs = state.completedSeqs + seq
                )
                val (advancedSeq, remainingCompleted) = advanceWatermark(baseState.flushedSeq, baseState.completedSeqs)
                val withWatermark =
                  if (advancedSeq > baseState.flushedSeq) {
                    context.pipeToSelf(runtime.updateFlushedSeq(advancedSeq))(WatermarkPersisted.apply)
                    baseState.copy(
                      flushedSeq = advancedSeq,
                      completedSeqs = remainingCompleted,
                      pendingMetaWrites = baseState.pendingMetaWrites + 1
                    )
                  } else {
                    baseState.copy(completedSeqs = remainingCompleted)
                  }
                val launchedState = launchAvailableUploads(runtime, context, withWatermark)
                active(runtime, timers, settleDrain(timers, launchedState))
              case Failure(ex) =>
                active(runtime, timers, failFlusher(runtime, timers, state.copy(inFlightSeqs = state.inFlightSeqs - seq), ex))
            }
          }

        case WatermarkPersisted(result) =>
          if (state.failureCause.isDefined) {
            Behaviors.same
          } else {
            result match {
              case Success(_) =>
                active(runtime, timers, settleDrain(timers, state.copy(pendingMetaWrites = math.max(0, state.pendingMetaWrites - 1))))
              case Failure(ex) =>
                active(runtime, timers, failFlusher(runtime, timers, state, ex))
            }
          }

        case DrainTimedOut(generation) =>
          if (state.failureCause.isDefined || !state.activeDrainGeneration.contains(generation)) {
            Behaviors.same
          } else {
            active(runtime, timers, failFlusher(runtime, timers, state, new TimeoutException(s"Chunk flusher drain timeout (${runtime.drainTimeout}) for entity ${runtime.entityId}")))
          }

        case GetFlushedSeq(reply) =>
          reply.trySuccess(state.flushedSeq)
          Behaviors.same

        case GetIsRunning(reply) =>
          reply.trySuccess(state.isRunning)
          Behaviors.same
      }
    }

  private def startPollingIfNeeded(
      runtime: Runtime,
      timers: TimerScheduler[Command],
      context: org.apache.pekko.actor.typed.scaladsl.ActorContext[Command],
      state: State
  ): Unit =
    if (state.failureCause.isEmpty && state.drainWaiters.isEmpty) {
      timers.startTimerWithFixedDelay(PollTimerKey, PollTick, runtime.tickInterval)
      context.self ! PollTick
    }

  private def launchAvailableUploads(
      runtime: Runtime,
      context: org.apache.pekko.actor.typed.scaladsl.ActorContext[Command],
      state: State
  ): State = {
    def loop(current: State): State = {
      val upperBound = current.drainTargetSeq.getOrElse(current.highestObservedSpooledSeq)
      if (
        current.failureCause.isDefined ||
        current.blobKeyPrefix.isEmpty ||
        current.inFlightSeqs.size >= runtime.parallelism ||
        current.nextSeqToUpload > upperBound
      ) {
        current
      } else {
        val seq = current.nextSeqToUpload
        val prefix = current.blobKeyPrefix.getOrElse(
          throw new IllegalStateException(s"Chunk address missing for entity ${runtime.entityId}")
        )
        context.pipeToSelf(runtime.uploadWithRetry(seq, prefix))(result => UploadCompleted(seq, result))
        loop(current.copy(
          nextSeqToUpload = seq + 1L,
          inFlightSeqs = current.inFlightSeqs + seq
        ))
      }
    }

    loop(state)
  }

  @scala.annotation.tailrec
  private def advanceWatermark(
      flushedSeq: Long,
      completedSeqs: TreeSet[Long]
  ): (Long, TreeSet[Long]) =
    if (completedSeqs.contains(flushedSeq + 1L))
      advanceWatermark(flushedSeq + 1L, completedSeqs - (flushedSeq + 1L))
    else
      flushedSeq -> completedSeqs

  private def settleDrain(
      timers: TimerScheduler[Command],
      state: State
  ): State =
    state.failureCause match {
      case Some(ex) if state.drainWaiters.nonEmpty =>
        timers.cancel(DrainTimerKey)
        state.drainWaiters.foreach(_.tryFailure(ex))
        state.copy(
          isRunning = false,
          drainWaiters = Vector.empty,
          drainTargetSeq = None,
          activeDrainGeneration = None
        )
      case None if state.drainWaiters.nonEmpty && state.drainTargetSeq.exists { targetSeq =>
            state.nextSeqToUpload > targetSeq &&
            state.inFlightSeqs.isEmpty &&
            state.pendingMetaWrites == 0 &&
            state.flushedSeq >= targetSeq
          } =>
        timers.cancel(DrainTimerKey)
        state.drainWaiters.foreach(_.trySuccess(state.flushedSeq))
        state.copy(
          isRunning = false,
          drainWaiters = Vector.empty,
          drainTargetSeq = None,
          activeDrainGeneration = None
        )
      case _ =>
        state
    }

  private def failFlusher(
      runtime: Runtime,
      timers: TimerScheduler[Command],
      state: State,
      ex: Throwable
  ): State = {
    val failure = state.failureCause.getOrElse(ex)
    timers.cancel(PollTimerKey)
    timers.cancel(DrainTimerKey)
    state.drainWaiters.foreach(_.tryFailure(failure))
    state.copy(
      isRunning = false,
      pollInProgress = false,
      drainWaiters = Vector.empty,
      drainTargetSeq = None,
      activeDrainGeneration = None,
      failureCause = Some(failure)
    )
  }
}

final class DefaultChunkFlusher(
    val entityId: String,
    spool: ChunkSpool,
    blobWriter: SingleShotBlobWriter,
    bucket: String,
    startSeq: Long,
    parallelism: Int,
    tickInterval: FiniteDuration,
    drainTimeout: FiniteDuration,
    maxUploadAttempts: Int,
    initialRetryBackoff: FiniteDuration,
    metaAdapter: FlusherMetaAdapter[SpoolMeta],
    blobKeyResolver: BlobKeyResolver = PrefixedSequentialBlobKeyResolver
)(using system: ActorSystem[?], ec: ExecutionContext) extends ChunkFlusher {

  require(startSeq >= 0L, s"startSeq must be >= 0: $startSeq")
  require(parallelism > 0, s"parallelism must be > 0: $parallelism")
  require(maxUploadAttempts > 0, s"maxUploadAttempts must be > 0: $maxUploadAttempts")
  require(initialRetryBackoff > Duration.Zero, s"initialRetryBackoff must be > 0: $initialRetryBackoff")

  private val scheduler = system.classicSystem.scheduler
  private val runtime = DefaultChunkFlusherActor.Runtime(
    entityId = entityId,
    startSeq = startSeq,
    parallelism = parallelism,
    tickInterval = tickInterval,
    drainTimeout = drainTimeout,
    readMeta = () => spool.readMeta(entityId),
    updateFlushedSeq = seq => spool.updateFlushedSeq(entityId, seq),
    uploadWithRetry = (seq, prefix) => uploadChunkWithRetry(seq, prefix, maxUploadAttempts, initialRetryBackoff),
    metaAdapter = metaAdapter
  )
  private val flusherActor = system.systemActorOf(
    DefaultChunkFlusherActor(runtime),
    s"chunk-flusher-${UUID.randomUUID()}"
  )

  override def start(): Unit =
    flusherActor ! DefaultChunkFlusherActor.Start

  override def stop(): Unit =
    flusherActor ! DefaultChunkFlusherActor.Stop

  override def drain(): Future[Long] = {
    val reply = Promise[Long]()
    flusherActor ! DefaultChunkFlusherActor.Drain(reply)
    reply.future
  }

  override def flushedSeq: Future[Long] = {
    val reply = Promise[Long]()
    flusherActor ! DefaultChunkFlusherActor.GetFlushedSeq(reply)
    reply.future
  }

  override def isRunning: Future[Boolean] = {
    val reply = Promise[Boolean]()
    flusherActor ! DefaultChunkFlusherActor.GetIsRunning(reply)
    reply.future
  }

  private def uploadChunkWithRetry(
      seq: Long,
      prefix: BlobKeyPrefix,
      attemptsRemaining: Int,
      retryBackoff: FiniteDuration
  ): Future[Long] = {
    uploadChunk(seq, prefix).recoverWith {
      case NonFatal(ex) if attemptsRemaining > 1 =>
        after(retryBackoff, scheduler)(
          uploadChunkWithRetry(seq, prefix, attemptsRemaining - 1, retryBackoff * 2)
        )(ec)
    }(ec)
  }

  private def uploadChunk(seq: Long, prefix: BlobKeyPrefix): Future[Long] = {
    val key = blobKeyResolver.chunkKey(prefix, seq)
    spool.readChunk(entityId, seq).flatMap { bytes =>
      blobWriter.write(bucket, key, bytes).map(_ => seq)
    }(ec)
  }
}

final class DefaultChunkFlusherFactory(
    system: ActorSystem[?],
    blobWriter: SingleShotBlobWriter,
    bucket: String,
    parallelism: Int = 64,
    tickInterval: FiniteDuration = 200.millis,
    drainTimeout: FiniteDuration = 10.minutes,
    maxUploadAttempts: Int = 3,
    initialRetryBackoff: FiniteDuration = 1.second,
    metaAdapter: FlusherMetaAdapter[SpoolMeta],
    blobKeyResolver: BlobKeyResolver = PrefixedSequentialBlobKeyResolver
)(using ec: ExecutionContext) extends ChunkFlusherFactory {

  private given ActorSystem[?] = system

  override def create(entityId: String, spool: ChunkSpool, startSeq: Long): ChunkFlusher =
    new DefaultChunkFlusher(
      entityId = entityId,
      spool = spool,
      blobWriter = blobWriter,
      bucket = bucket,
      startSeq = startSeq,
      parallelism = parallelism,
      tickInterval = tickInterval,
      drainTimeout = drainTimeout,
      maxUploadAttempts = maxUploadAttempts,
      initialRetryBackoff = initialRetryBackoff,
      metaAdapter = metaAdapter,
      blobKeyResolver = blobKeyResolver
    )
}

object DefaultChunkFlusherFactory {
  def fromConfig(
      config: Config,
      system: ActorSystem[?],
      blobWriter: SingleShotBlobWriter,
      bucket: String,
      metaAdapter: FlusherMetaAdapter[SpoolMeta],
      blobKeyResolver: BlobKeyResolver = PrefixedSequentialBlobKeyResolver
  )(using ec: ExecutionContext): DefaultChunkFlusherFactory = {
    val parallelism =
      if (config.hasPath("parallelism")) config.getInt("parallelism")
      else 64
    val tickInterval =
      if (config.hasPath("tick-interval")) Duration(config.getDuration("tick-interval").toMillis, MILLISECONDS)
      else 200.millis
    val drainTimeout =
      if (config.hasPath("drain-timeout")) Duration(config.getDuration("drain-timeout").toMillis, MILLISECONDS)
      else 10.minutes
    val maxUploadAttempts =
      if (config.hasPath("max-upload-attempts")) config.getInt("max-upload-attempts")
      else 3
    val initialRetryBackoff =
      if (config.hasPath("initial-retry-backoff")) {
        Duration(config.getDuration("initial-retry-backoff").toMillis, MILLISECONDS)
      } else 1.second

    new DefaultChunkFlusherFactory(
      system = system,
      blobWriter = blobWriter,
      bucket = bucket,
      parallelism = parallelism,
      tickInterval = tickInterval,
      drainTimeout = drainTimeout,
      maxUploadAttempts = maxUploadAttempts,
      initialRetryBackoff = initialRetryBackoff,
      metaAdapter = metaAdapter,
      blobKeyResolver = blobKeyResolver
    )
  }
}
