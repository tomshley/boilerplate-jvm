package com.tomshley.boilerplate.jvm.durablebufferedflush

import com.tomshley.boilerplate.jvm.durablebufferedflush.{SessionView, SpoolMeta}
import org.apache.pekko.util.Timeout
import org.scalatest.Assertions

import java.time.Instant
import scala.collection.mutable
import scala.concurrent.Future
import scala.concurrent.duration.*
import scala.util.Try

private[durablebufferedflush] trait FlushSpecSupport { self: Assertions =>

  protected val fixedInstant: Instant = Instant.parse("2026-02-01T00:00:00Z")

  protected def makeConfig(
      claimLagSoft: Long = 2L,
      claimLagHard: Long = 4L,
      pauseTimeout: FiniteDuration = 500.millis,
      closeAskTimeout: FiniteDuration = 500.millis,
      closeInspectTimeout: FiniteDuration = 500.millis,
      closeRetryDelay: FiniteDuration = 10.millis,
      closeMaxRetries: Int = 2,
      recoveryParallelism: Int = 2,
      recoveryInspectTimeout: FiniteDuration = 500.millis
  ): FlushConfig =
    FlushConfig(
      backpressure = FlushBackpressureConfig(
        claimLagSoft = claimLagSoft,
        claimLagHard = claimLagHard,
        pauseTimeout = pauseTimeout
      ),
      close = FlushCloseConfig(
        askTimeout = Timeout(closeAskTimeout),
        inspectTimeout = Timeout(closeInspectTimeout),
        retryDelay = closeRetryDelay,
        maxRetries = closeMaxRetries
      ),
      recovery = FlushRecoveryConfig(
        parallelism = recoveryParallelism,
        inspectTimeout = Timeout(recoveryInspectTimeout)
      )
    )

  protected def sessionView(
      isClosed: Boolean = false,
      isAborted: Boolean = false,
      openedAt: Option[Instant] = Some(fixedInstant),
      device: Option[String] = Some("device-1"),
      deviceCorrelationId: Option[String] = Some("subject-1"),
      objectHashHex: String = "hash-123",
      declaredPayloadSize: Long = 1024L,
      claimsCount: Long = 0L,
      totalClaimedBytes: Long = 0L,
      lastClaimSequence: Long = -1L
  ): SessionView[String] =
    SessionView(
      isClosed = isClosed,
      isAborted = isAborted,
      openedAt = openedAt,
      device = device,
      deviceCorrelationId = deviceCorrelationId,
      objectHashHex = objectHashHex,
      declaredPayloadSize = declaredPayloadSize,
      claimsCount = claimsCount,
      totalClaimedBytes = totalClaimedBytes,
      lastClaimSequence = lastClaimSequence
    )

  protected def spoolMeta(
      entityId: String,
      deviceId: String = "device-1",
      objectHashHex: String = "hash-123",
      lastSpooledSeq: Long,
      totalSpooledBytes: Long,
      flushedSeq: Long = -1L,
      declaredPayloadSize: Long = 1024L,
      totalExpectedChunks: Long = 4L,
      isComplete: Boolean = false
  ): SpoolMeta =
    val effectiveExpectedChunks =
      if (isComplete) {
        math.max(1L, lastSpooledSeq + 1L)
      } else {
        totalExpectedChunks
      }

    SpoolMeta(
      entityId = entityId,
      deviceId = deviceId,
      objectHashHex = objectHashHex,
      lastSpooledSeq = lastSpooledSeq,
      totalSpooledBytes = totalSpooledBytes,
      flushedSeq = flushedSeq,
      declaredPayloadSize = declaredPayloadSize,
      totalExpectedChunks = effectiveExpectedChunks,
      createdAt = fixedInstant
    )

  protected final case class TestSessionSummary(view: SessionView[String])

  protected final case class RegisterCall(
      entityId: String,
      device: String,
      deviceCorrelationId: String,
      objectHashHex: String,
      declaredPayloadSize: Long
  )

  protected final case class AbortCall(entityId: String, reason: String)

  protected final case class CloseCall(
      entityId: String,
      expectedClaimsCount: Long,
      expectedTotalBytes: Long,
      expectedLastSequence: Long
  )

  protected final case class DispatchCall(
      entityId: String,
      envelope: String,
      rawBytesLength: Long,
      binding: TestReplyBinding
  )

  protected final case class CreatedFlusher(
      entityId: String,
      startSeq: Long,
      flusher: RecordingChunkFlusher
  )

  protected class RecordingChunkSpool extends ChunkSpool {
    private val metas = mutable.Map.empty[String, SpoolMeta]
    private val chunks = mutable.Map.empty[(String, Long), Array[Byte]]

    val readChunkCalls = mutable.ArrayBuffer.empty[(String, Long)]
    val writeCalls = mutable.ArrayBuffer.empty[(String, Long, Array[Byte])]
    val initializeCalls = mutable.ArrayBuffer.empty[(String, SpoolMeta)]
    val cleanupCalls = mutable.ArrayBuffer.empty[String]

    var listEntitiesHandler: () => Future[Seq[String]] =
      () => Future.successful((metas.keys ++ chunks.keys.map(_._1)).toSeq.distinct.sorted)

    var readMetaHandler: String => Future[Option[SpoolMeta]] =
      entityId => Future.successful(metas.get(entityId))

    var updateMetaHandler: (String, SpoolMeta) => Future[Unit] =
      (entityId, meta) => {
        metas.update(entityId, meta)
        Future.successful(())
      }

    var updateFlushedSeqHandler: (String, Long) => Future[Unit] =
      (entityId, seq) => {
        metas.get(entityId) match {
          case Some(meta) =>
            metas.update(entityId, meta.withFlushed(seq))
            Future.successful(())
          case None =>
            Future.failed(new IllegalStateException(s"missing meta for $entityId"))
        }
      }

    var initializeHandler: (String, SpoolMeta) => Future[SpoolMeta] =
      (entityId, meta) => {
        metas.update(entityId, meta)
        initializeCalls += ((entityId, meta))
        Future.successful(meta)
      }

    var cleanupHandler: String => Future[Unit] =
      entityId => {
        metas.remove(entityId)
        chunks.keys.filter(_._1 == entityId).toList.foreach(chunks.remove)
        cleanupCalls += entityId
        Future.successful(())
      }

    def seedMeta(entityId: String, meta: SpoolMeta): Unit =
      metas.update(entityId, meta)

    def seedChunk(entityId: String, seq: Long, bytes: Array[Byte]): Unit =
      chunks.update((entityId, seq), bytes.clone())

    override def write(entityId: String, seq: Long, bytes: Array[Byte]): Future[Long] = {
      val copy = bytes.clone()
      chunks.update((entityId, seq), copy)
      writeCalls += ((entityId, seq, copy))
      Future.successful(copy.length.toLong)
    }

    override def readChunk(entityId: String, seq: Long): Future[Array[Byte]] = {
      readChunkCalls += ((entityId, seq))
      chunks.get((entityId, seq)) match {
        case Some(bytes) => Future.successful(bytes.clone())
        case None => Future.failed(new java.nio.file.NoSuchFileException(s"$entityId/$seq"))
      }
    }

    override def readMeta(entityId: String): Future[Option[SpoolMeta]] =
      readMetaHandler(entityId)

    override def updateMeta(entityId: String, meta: SpoolMeta): Future[Unit] =
      updateMetaHandler(entityId, meta)

    override def updateFlushedSeq(entityId: String, seq: Long): Future[Unit] =
      updateFlushedSeqHandler(entityId, seq)

    override def initialize(entityId: String, meta: SpoolMeta): Future[SpoolMeta] =
      initializeHandler(entityId, meta)

    override def cleanup(entityId: String): Future[Unit] =
      cleanupHandler(entityId)

    override def listEntities(): Future[Seq[String]] =
      listEntitiesHandler()
  }

  protected class RecordingSessionPort extends SessionPort[String, TestSessionSummary] {
    val inspectCalls = mutable.ArrayBuffer.empty[String]
    val registerCalls = mutable.ArrayBuffer.empty[RegisterCall]
    val abortCalls = mutable.ArrayBuffer.empty[AbortCall]
    val closeCalls = mutable.ArrayBuffer.empty[CloseCall]

    var inspectHandler: String => Future[TestSessionSummary] = entityId =>
      Future.failed(new AssertionError(s"Unexpected inspect for $entityId"))

    var registerHandler: RegisterCall => Future[TestSessionSummary] = call =>
      Future.successful(TestSessionSummary(sessionView(
        device = Some(call.device),
        deviceCorrelationId = Some(call.deviceCorrelationId),
        objectHashHex = call.objectHashHex,
        declaredPayloadSize = call.declaredPayloadSize
      )))

    var abortHandler: AbortCall => Future[TestSessionSummary] = _ =>
      Future.successful(TestSessionSummary(sessionView(isAborted = true)))

    var closeHandler: CloseCall => Future[TestSessionSummary] = call =>
      Future.successful(TestSessionSummary(sessionView(
        isClosed = true,
        claimsCount = call.expectedClaimsCount,
        totalClaimedBytes = call.expectedTotalBytes,
        lastClaimSequence = call.expectedLastSequence
      )))

    override def register(
        entityId: String,
        device: String,
        deviceCorrelationId: String,
        objectHashHex: String,
        declaredPayloadSize: Long
    )(using Timeout): Future[TestSessionSummary] = {
      val call = RegisterCall(entityId, device, deviceCorrelationId, objectHashHex, declaredPayloadSize)
      registerCalls += call
      registerHandler(call)
    }

    override def inspect(entityId: String)(using Timeout): Future[TestSessionSummary] = {
      inspectCalls += entityId
      inspectHandler(entityId)
    }

    override def abort(entityId: String, reason: String)(using Timeout): Future[TestSessionSummary] = {
      val call = AbortCall(entityId, reason)
      abortCalls += call
      abortHandler(call)
    }

    override def closeWithValidation(
        entityId: String,
        expectedClaimsCount: Long,
        expectedTotalBytes: Long,
        expectedLastSequence: Long
    )(using Timeout): Future[TestSessionSummary] = {
      val call = CloseCall(entityId, expectedClaimsCount, expectedTotalBytes, expectedLastSequence)
      closeCalls += call
      closeHandler(call)
    }

    override def toSessionView(summary: TestSessionSummary): SessionView[String] =
      summary.view
  }

  protected final class TestReplyBinding(
      val onConfirmedClaimsCount: Long => Unit,
      val onRejected: Throwable => Unit
  ) {
    private var currentEntityId: String = ""

    def entityId: String = currentEntityId

    def bindEntityId(entityId: String): Unit =
      currentEntityId = entityId

    def clearEntityId(): Unit =
      currentEntityId = ""
  }

  protected class RecordingClaimPort extends ClaimPort[String, TestReplyBinding] {
    private var latestReplyBinding: Option[TestReplyBinding] = None

    val openCalls = mutable.ArrayBuffer.empty[TestReplyBinding]
    val bindCalls = mutable.ArrayBuffer.empty[(TestReplyBinding, String)]
    val clearCalls = mutable.ArrayBuffer.empty[TestReplyBinding]
    val closeCalls = mutable.ArrayBuffer.empty[TestReplyBinding]
    val dispatchCalls = mutable.ArrayBuffer.empty[DispatchCall]

    var decodeHandler: Array[Byte] => Try[String] = bytes => Try(new String(bytes))

    override def decodeEnvelope(bytes: Array[Byte]): Try[String] =
      decodeHandler(bytes)

    override def openReplyBinding(
        onConfirmedClaimsCount: Long => Unit,
        onRejected: Throwable => Unit
    ): TestReplyBinding = {
      val replyBinding = new TestReplyBinding(onConfirmedClaimsCount, onRejected)
      latestReplyBinding = Some(replyBinding)
      openCalls += replyBinding
      replyBinding
    }

    override def bindEntityId(replyBinding: TestReplyBinding, entityId: String): Unit = {
      bindCalls += ((replyBinding, entityId))
      replyBinding.bindEntityId(entityId)
    }

    override def clearEntityId(replyBinding: TestReplyBinding): Unit = {
      clearCalls += replyBinding
      replyBinding.clearEntityId()
    }

    override def closeReplyBinding(replyBinding: TestReplyBinding): Unit =
      closeCalls += replyBinding

    override def dispatchClaim(
        entityId: String,
        envelope: String,
        rawBytesLength: Long,
        replyBinding: TestReplyBinding
    ): Unit =
      dispatchCalls += DispatchCall(entityId, envelope, rawBytesLength, replyBinding)

    def latestBinding: TestReplyBinding =
      latestReplyBinding.getOrElse(fail("expected an open reply binding"))
  }

  protected class RecordingChunkFlusher(val entityId: String) extends ChunkFlusher {
    var startCount: Int = 0
    var stopCount: Int = 0
    var running: Boolean = false
    var currentFlushedSeq: Long = -1L
    var drainResult: Future[Long] = Future.successful(currentFlushedSeq)

    override def start(): Unit = {
      startCount += 1
      running = true
    }

    override def stop(): Unit = {
      stopCount += 1
      running = false
    }

    override def drain(): Future[Long] =
      drainResult

    override def flushedSeq: Future[Long] =
      Future.successful(currentFlushedSeq)

    override def isRunning: Future[Boolean] =
      Future.successful(running)
  }

  protected class RecordingChunkFlusherFactory extends ChunkFlusherFactory {
    private val queued = mutable.Queue.empty[RecordingChunkFlusher]

    val created = mutable.ArrayBuffer.empty[CreatedFlusher]

    def enqueue(flusher: RecordingChunkFlusher): Unit =
      queued.enqueue(flusher)

    override def create(entityId: String, spool: ChunkSpool, startSeq: Long): ChunkFlusher = {
      val flusher = if (queued.nonEmpty) queued.dequeue() else new RecordingChunkFlusher(entityId)
      created += CreatedFlusher(entityId, startSeq, flusher)
      flusher
    }
  }
}
