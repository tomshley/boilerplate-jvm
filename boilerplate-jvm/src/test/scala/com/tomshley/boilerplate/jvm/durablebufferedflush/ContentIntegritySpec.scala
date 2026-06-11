/*
 * Copyright 2026 Tomshley LLC.
 * All Rights Reserved.
 */

package com.tomshley.boilerplate.jvm.durablebufferedflush

import com.tomshley.boilerplate.jvm.durablebufferedflush.internal.FilesystemChunkSpool
import com.tomshley.boilerplate.jvm.utils.RestorableDigestUtil
import org.apache.pekko.actor.testkit.typed.scaladsl.ActorTestKit
import org.apache.pekko.actor.typed.ActorSystem
import org.apache.pekko.util.Timeout
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{Millis, Seconds, Span}
import org.scalatest.wordspec.AnyWordSpec

import java.nio.charset.StandardCharsets
import java.nio.file.attribute.BasicFileAttributes
import java.nio.file.{FileVisitResult, Files, Path, SimpleFileVisitor}
import java.security.MessageDigest
import java.time.Instant
import java.util.HexFormat
import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration.*
import scala.util.Try

/** Content-integrity verification: digest midstate checkpointing in the
  * spool meta, restore across process restart, and the finalize verdict
  * surfaced as [[FinalizeOutcome]] with [[HashMismatchDirective]] dispatch.
  */
final class ContentIntegritySpec
    extends AnyWordSpec
    with Matchers
    with BeforeAndAfterAll
    with ScalaFutures {

  override implicit val patienceConfig: PatienceConfig =
    PatienceConfig(timeout = Span(5, Seconds), interval = Span(50, Millis))

  private val testKit = ActorTestKit("ContentIntegritySpec")
  private given ActorSystem[?] = testKit.system
  private given ExecutionContext = testKit.system.executionContext

  override def afterAll(): Unit = {
    testKit.shutdownTestKit()
    super.afterAll()
  }

  private def sha256Hex(bytes: Array[Byte]): String =
    HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes))

  // Pure fold/finish digest semantics are pinned in
  // com.tomshley.boilerplate.jvm.utils.RestorableDigestUtilSpec — this spec
  // covers the durablebufferedflush integration: checkpointing in the spool
  // meta, restart restore, and the finalize verdict dispatch.

  "FilesystemChunkSpool digest checkpointing" should {

    "advance the midstate with every write and survive a process restart" in {
      withTempDir { rootDir =>
        val entityId = "entity-restart"
        val chunks = (0 until 6).map(i => Array.fill[Byte](64)((i + 1).toByte))
        val declaredHex = sha256Hex(chunks.flatten.toArray)

        // First "process": spool chunks 0..2, then drop the instance.
        val firstSpool = new FilesystemChunkSpool(testKit.system, rootDir)
        firstSpool.initialize(
          entityId,
          SpoolMeta.initial(entityId, "device-1", declaredHex, 384L, 6L)
        ).futureValue
        (0 until 3).foreach(i => firstSpool.write(entityId, i.toLong, chunks(i)).futureValue)

        val checkpoint = firstSpool.readMeta(entityId).futureValue.get
        checkpoint.lastSpooledSeq shouldBe 2L
        checkpoint.digestStateHex should not be empty
        checkpoint.hasDigestCoverage shouldBe true

        // Second "process": a fresh instance over the same root restores
        // the midstate from meta.json and continues the fold.
        val secondSpool = new FilesystemChunkSpool(testKit.system, rootDir)
        (3 until 6).foreach(i => secondSpool.write(entityId, i.toLong, chunks(i)).futureValue)

        val resumed = secondSpool.readMeta(entityId).futureValue.get
        resumed.lastSpooledSeq shouldBe 5L
        RestorableDigestUtil.sha256DigestHex(resumed.digestStateHex) shouldBe declaredHex
      }
    }

    "preserve the absence of coverage for a spool written before digest checkpointing" in {
      withTempDir { rootDir =>
        val entityId = "entity-legacy"
        // Simulate a meta.json persisted by a pre-digest version: the
        // digestStateHex property does not exist at all.
        val legacyJson =
          s"""{"entityId":"$entityId","deviceId":"device-1","objectHashHex":"feed",
             |"lastSpooledSeq":1,"totalSpooledBytes":128,"flushedSeq":1,
             |"declaredPayloadSize":256,"totalExpectedChunks":4,
             |"createdAt":1750000000.000000000}""".stripMargin.replace("\n", "")
        val entityDir = rootDir.resolve(entityId)
        Files.createDirectories(entityDir.resolve("chunks"))
        Files.write(entityDir.resolve("chunks").resolve(f"${0L}%09d.bin"), Array.fill[Byte](64)(1))
        Files.write(entityDir.resolve("chunks").resolve(f"${1L}%09d.bin"), Array.fill[Byte](64)(2))
        Files.write(entityDir.resolve("meta.json"), legacyJson.getBytes(StandardCharsets.UTF_8))

        val spool = new FilesystemChunkSpool(testKit.system, rootDir)
        val legacyMeta = spool.readMeta(entityId).futureValue.get
        legacyMeta.digestStateHex shouldBe None
        legacyMeta.hasDigestCoverage shouldBe false

        // Folding later chunks into a fresh digest would be silently wrong;
        // None must propagate instead.
        spool.write(entityId, 2L, Array.fill[Byte](64)(3)).futureValue
        val advanced = spool.readMeta(entityId).futureValue.get
        advanced.lastSpooledSeq shouldBe 2L
        advanced.digestStateHex shouldBe None
        advanced.hasDigestCoverage shouldBe false
      }
    }
  }

  "Workflow content-hash verification" should {

    "report Verified and close when the recomputed hash equals the declared hash" in {
      val payload = Seq("alpha".getBytes, "beta".getBytes)
      val fixture = newFixture(declaredHex = sha256Hex(payload.flatten.toArray), totalExpectedChunks = 2L)
      val prepared = fixture.prepareFresh()
      fixture.acceptAll(prepared, payload)

      val result = fixture.finalizeAt(prepared, lastSpooledSeq = 1L)

      result.outcome shouldBe a[FinalizeOutcome.Verified]
      fixture.sessionPort.closeCalls should have size 1
      fixture.spool.cleanupCalls should contain(fixture.entityId)
    }

    "report HashMismatch and STILL close under the CloseAnyway directive" in {
      val fixture = newFixture(declaredHex = sha256Hex("expected".getBytes), totalExpectedChunks = 1L)
      val prepared = fixture.prepareFresh()
      fixture.acceptAll(prepared, Seq("tampered".getBytes))

      val result = fixture.finalizeAt(prepared, lastSpooledSeq = 0L)

      result.outcome match {
        case FinalizeOutcome.HashMismatch(declaredHex, actualHex) =>
          declaredHex shouldBe fixture.declaredHex
          actualHex shouldBe sha256Hex("tampered".getBytes)
        case other => fail(s"expected HashMismatch, got $other")
      }
      fixture.sessionPort.closeCalls should have size 1
      fixture.sessionPort.abortCalls shouldBe empty
    }

    "hold the session open on mismatch under HoldOpen, then reset and complete a same-hash retry from sequence zero" in {
      val goodBytes = "the-real-content".getBytes
      val fixture = newFixture(declaredHex = sha256Hex(goodBytes), totalExpectedChunks = 1L)

      // Attempt 1: producer ships tampered bytes.
      val prepared = fixture.prepareFresh()
      fixture.acceptAll(prepared, Seq("mutated-content!".getBytes))
      val held = fixture.finalizeAt(prepared, lastSpooledSeq = 0L, directive = HashMismatchDirective.HoldOpen)

      held.outcome shouldBe a[FinalizeOutcome.HashMismatch]
      held.binding.flusher shouldBe None
      // Nothing domain-visible happened: no close, no abort, no cleanup —
      // the entity and spool await the caller's decision.
      fixture.sessionPort.closeCalls shouldBe empty
      fixture.sessionPort.abortCalls shouldBe empty
      fixture.spool.cleanupCalls shouldBe empty
      fixture.flusherOf(prepared).stopCount shouldBe 1

      // Caller decision: reset the transfer (abort entity + delete spool).
      fixture.workflow.resetTransfer(fixture.entityId, "content hash mismatch").futureValue
      fixture.sessionPort.abortCalls should have size 1
      fixture.spool.cleanupCalls should contain(fixture.entityId)

      // Retry with byte-identical (correct) content restarts at seq 0 and verifies.
      fixture.sessionPort.inspectHandler = _ => Future.successful(fixture.abortedSummary())
      val retry = fixture.prepare()
      retry.lastAcceptedSeq shouldBe -1L
      retry.receivedChunks shouldBe 0L
      fixture.acceptAll(retry, Seq(goodBytes))
      val completed = fixture.finalizeAt(retry, lastSpooledSeq = 0L, directive = HashMismatchDirective.HoldOpen)

      completed.outcome shouldBe FinalizeOutcome.Verified(fixture.declaredHex)
      fixture.sessionPort.closeCalls should have size 1
    }

    "report Unverified(NoDigestCoverage) and close when spooled chunks predate digest checkpointing" in {
      val fixture = newFixture(declaredHex = sha256Hex("anything".getBytes), totalExpectedChunks = 2L)
      // Seed a resumed legacy spool: chunks exist, no digest state.
      fixture.spool.seedMeta(
        fixture.entityId,
        fixture.metaFor(lastSpooledSeq = 1L, flushedSeq = 1L, totalSpooledBytes = 128L, digestStateHex = None)
      )
      val prepared = fixture.prepareResumed(claimsCount = 2L, lastClaimSequence = 1L)

      // HoldOpen only holds on MISMATCH — an unverifiable transfer is not
      // a mismatch verdict and must not strand the session.
      val result = fixture.finalizeAt(prepared, lastSpooledSeq = 1L, directive = HashMismatchDirective.HoldOpen)

      result.outcome shouldBe FinalizeOutcome.Unverified(FinalizeOutcome.UnverifiedReason.NoDigestCoverage)
      fixture.sessionPort.closeCalls should have size 1
    }

    "default the outcome to Unverified(NotEvaluated) for directly constructed results" in {
      val fixture = newFixture(declaredHex = "unused", totalExpectedChunks = 1L)
      val binding = fixture.workflow.openBinding()
      FlushFinalizationResult(binding).outcome shouldBe
        FinalizeOutcome.Unverified(FinalizeOutcome.UnverifiedReason.NotEvaluated)
    }
  }

  // -- fixtures ---------------------------------------------------------------

  private val fixedInstant = Instant.parse("2026-03-23T00:00:00Z")

  private def newFixture(declaredHex: String, totalExpectedChunks: Long): Fixture =
    new Fixture(declaredHex, totalExpectedChunks)

  private final class Fixture(val declaredHex: String, totalExpectedChunks: Long) {
    val entityId = "entity-integrity"
    val spool = new DigestFoldingChunkSpool
    val sessionPort = new RecordingSessionPort
    val claimPort = new RecordingClaimPort
    private val flusherFactory = new RecordingChunkFlusherFactory

    val workflow: Workflow[String, TestSessionSummary, String, TestReplyBinding] =
      Workflow[String, TestSessionSummary, String, TestReplyBinding](
        spool = spool,
        flusherFactory = flusherFactory,
        sessionPort = sessionPort,
        claimPort = claimPort,
        config = FlushConfig(
          backpressure = FlushBackpressureConfig(claimLagSoft = 8L, claimLagHard = 16L, pauseTimeout = 1.second),
          close = FlushCloseConfig(askTimeout = Timeout(1.second), inspectTimeout = Timeout(1.second), retryDelay = 10.millis, maxRetries = 1),
          recovery = FlushRecoveryConfig(parallelism = 1, inspectTimeout = Timeout(1.second), perEntityTimeout = 5.seconds)
        ),
        system = testKit.system
      )

    private val descriptor = FlushTransferDescriptor[String](
      entityId = entityId,
      device = "device-1",
      deviceId = "device-id-1",
      deviceCorrelationId = "subject-1",
      objectHashHex = declaredHex,
      declaredPayloadSize = 1024L,
      totalExpectedChunks = totalExpectedChunks
    )

    def metaFor(lastSpooledSeq: Long, flushedSeq: Long, totalSpooledBytes: Long, digestStateHex: Option[String]): SpoolMeta =
      SpoolMeta(
        entityId = entityId,
        deviceId = descriptor.deviceId,
        objectHashHex = declaredHex,
        lastSpooledSeq = lastSpooledSeq,
        totalSpooledBytes = totalSpooledBytes,
        flushedSeq = flushedSeq,
        declaredPayloadSize = descriptor.declaredPayloadSize,
        totalExpectedChunks = totalExpectedChunks,
        createdAt = fixedInstant,
        digestStateHex = digestStateHex
      )

    def openSummary(claimsCount: Long = 0L, lastClaimSequence: Long = -1L): TestSessionSummary =
      TestSessionSummary(SessionView(
        isClosed = false, isAborted = false, openedAt = Some(fixedInstant),
        device = Some(descriptor.device), deviceCorrelationId = Some(descriptor.deviceCorrelationId),
        objectHashHex = declaredHex, declaredPayloadSize = descriptor.declaredPayloadSize,
        claimsCount = claimsCount, totalClaimedBytes = 0L, lastClaimSequence = lastClaimSequence
      ))

    def abortedSummary(): TestSessionSummary =
      TestSessionSummary(openSummary().view.copy(isAborted = true))

    def prepareFresh(): FlushPreparedTransfer[TestReplyBinding] = {
      sessionPort.inspectHandler = _ => Future.successful(openSummary())
      prepare()
    }

    def prepareResumed(claimsCount: Long, lastClaimSequence: Long): FlushPreparedTransfer[TestReplyBinding] = {
      sessionPort.inspectHandler = _ => Future.successful(openSummary(claimsCount, lastClaimSequence))
      prepare()
    }

    def prepare(): FlushPreparedTransfer[TestReplyBinding] =
      workflow.prepareTransfer(workflow.openBinding(), descriptor).futureValue

    def acceptAll(prepared: FlushPreparedTransfer[TestReplyBinding], chunks: Seq[Array[Byte]]): Unit =
      chunks.zipWithIndex.foldLeft(prepared.lastAcceptedSeq) { case (lastSeq, (bytes, i)) =>
        val seq = prepared.lastAcceptedSeq + 1L + i
        workflow.acceptChunk(entityId, prepared.binding, s"chunk-$seq", bytes, seq, lastSeq).futureValue
        claimPort.confirmClaimsCount(seq + 1L)
        seq
      }

    def finalizeAt(
        prepared: FlushPreparedTransfer[TestReplyBinding],
        lastSpooledSeq: Long,
        directive: HashMismatchDirective = HashMismatchDirective.CloseAnyway
    ): FlushFinalizationResult[TestReplyBinding] = {
      flusherOf(prepared).drainResult = Future.successful(lastSpooledSeq)
      workflow.finalizeTransfer(entityId, prepared.binding, lastSpooledSeq, directive).futureValue
    }

    def flusherOf(prepared: FlushPreparedTransfer[TestReplyBinding]): RecordingChunkFlusher =
      prepared.binding.flusher.getOrElse(fail("expected a flusher")).asInstanceOf[RecordingChunkFlusher]
  }

  /** In-memory spool that mirrors the production digest contract: each
    * accepted write advances the midstate together with `lastSpooledSeq`,
    * and absent coverage propagates as None.
    */
  private final class DigestFoldingChunkSpool extends ChunkSpool {
    private val metas = mutable.Map.empty[String, SpoolMeta]
    val cleanupCalls = mutable.ArrayBuffer.empty[String]
    val initializeCalls = mutable.ArrayBuffer.empty[(String, SpoolMeta)]

    def seedMeta(entityId: String, meta: SpoolMeta): Unit = metas.update(entityId, meta)

    override def write(entityId: String, seq: Long, bytes: Array[Byte]): Future[Long] =
      metas.get(entityId) match {
        case None => Future.failed(new IllegalStateException(s"Spool not initialized for entity $entityId"))
        case Some(meta) =>
          val nextDigestState =
            if (meta.lastSpooledSeq < 0L) Some(RestorableDigestUtil.sha256FoldHex(None, bytes))
            else meta.digestStateHex.map(state => RestorableDigestUtil.sha256FoldHex(Some(state), bytes))
          metas.update(entityId, meta.withSpooled(seq, bytes.length.toLong).withDigestState(nextDigestState))
          Future.successful(bytes.length.toLong)
      }

    override def readChunk(entityId: String, seq: Long): Future[Array[Byte]] =
      Future.failed(new java.nio.file.NoSuchFileException(s"$entityId/$seq"))

    override def readMeta(entityId: String): Future[Option[SpoolMeta]] =
      Future.successful(metas.get(entityId))

    override def updateMeta(entityId: String, meta: SpoolMeta): Future[Unit] = {
      metas.update(entityId, meta)
      Future.successful(())
    }

    override def updateFlushedSeq(entityId: String, seq: Long): Future[Unit] = {
      metas.get(entityId).foreach(meta => metas.update(entityId, meta.withFlushed(seq)))
      Future.successful(())
    }

    override def initialize(entityId: String, meta: SpoolMeta): Future[SpoolMeta] = {
      initializeCalls += ((entityId, meta))
      Future.successful(metas.getOrElseUpdate(entityId, meta))
    }

    override def cleanup(entityId: String): Future[Unit] = {
      metas.remove(entityId)
      cleanupCalls += entityId
      Future.successful(())
    }

    override def listEntities(): Future[Seq[String]] =
      Future.successful(metas.keys.toSeq.sorted)
  }

  private final case class TestSessionSummary(view: SessionView[String])

  private final case class AbortCall(entityId: String, reason: String)
  private final case class CloseCall(entityId: String, expectedClaimsCount: Long, expectedTotalBytes: Long, expectedLastSequence: Long)

  private final class RecordingSessionPort extends SessionPort[String, TestSessionSummary] {
    val registerCalls = mutable.ArrayBuffer.empty[String]
    val abortCalls = mutable.ArrayBuffer.empty[AbortCall]
    val closeCalls = mutable.ArrayBuffer.empty[CloseCall]

    var inspectHandler: String => Future[TestSessionSummary] = entityId =>
      Future.failed(new AssertionError(s"Unexpected inspect for $entityId"))

    override def register(
        entityId: String, device: String, deviceCorrelationId: String,
        objectHashHex: String, declaredPayloadSize: Long, fileName: Option[String]
    )(using Timeout): Future[TestSessionSummary] = {
      registerCalls += entityId
      Future.successful(TestSessionSummary(SessionView(
        isClosed = false, isAborted = false, openedAt = Some(fixedInstant),
        device = Some(device), deviceCorrelationId = Some(deviceCorrelationId),
        objectHashHex = objectHashHex, declaredPayloadSize = declaredPayloadSize,
        claimsCount = 0L, totalClaimedBytes = 0L, lastClaimSequence = -1L
      )))
    }

    override def inspect(entityId: String)(using Timeout): Future[TestSessionSummary] =
      inspectHandler(entityId)

    override def abort(entityId: String, reason: String)(using Timeout): Future[TestSessionSummary] = {
      abortCalls += AbortCall(entityId, reason)
      Future.successful(TestSessionSummary(SessionView(
        isClosed = false, isAborted = true, openedAt = Some(fixedInstant),
        device = Some("device-1"), deviceCorrelationId = Some("subject-1"),
        objectHashHex = "aborted", declaredPayloadSize = 0L,
        claimsCount = 0L, totalClaimedBytes = 0L, lastClaimSequence = -1L
      )))
    }

    override def closeWithValidation(
        entityId: String, expectedClaimsCount: Long, expectedTotalBytes: Long, expectedLastSequence: Long
    )(using Timeout): Future[TestSessionSummary] = {
      closeCalls += CloseCall(entityId, expectedClaimsCount, expectedTotalBytes, expectedLastSequence)
      Future.successful(TestSessionSummary(SessionView(
        isClosed = true, isAborted = false, openedAt = Some(fixedInstant),
        device = Some("device-1"), deviceCorrelationId = Some("subject-1"),
        objectHashHex = "closed", declaredPayloadSize = 0L,
        claimsCount = expectedClaimsCount, totalClaimedBytes = expectedTotalBytes, lastClaimSequence = expectedLastSequence
      )))
    }

    override def toSessionView(summary: TestSessionSummary): SessionView[String] =
      summary.view
  }

  private final class TestReplyBinding(
      val onConfirmedClaimsCount: Long => Unit,
      val onRejected: Throwable => Unit
  ) {
    def confirmClaimsCount(claimsCount: Long): Unit = onConfirmedClaimsCount(claimsCount)
  }

  private final class RecordingClaimPort extends ClaimPort[String, TestReplyBinding] {
    private var latestReplyBinding: Option[TestReplyBinding] = None
    val clearCalls = mutable.ArrayBuffer.empty[TestReplyBinding]

    def confirmClaimsCount(claimsCount: Long): Unit =
      latestReplyBinding.getOrElse(fail("expected reply binding")).confirmClaimsCount(claimsCount)

    override def decodeEnvelope(bytes: Array[Byte]): Try[String] = Try(new String(bytes))

    override def openReplyBinding(
        onConfirmedClaimsCount: Long => Unit,
        onRejected: Throwable => Unit
    ): TestReplyBinding = {
      val replyBinding = new TestReplyBinding(onConfirmedClaimsCount, onRejected)
      latestReplyBinding = Some(replyBinding)
      replyBinding
    }

    override def bindEntityId(replyBinding: TestReplyBinding, entityId: String): Unit = ()

    override def clearEntityId(replyBinding: TestReplyBinding): Unit =
      clearCalls += replyBinding

    override def closeReplyBinding(replyBinding: TestReplyBinding): Unit = ()

    override def dispatchClaim(entityId: String, envelope: String, rawBytesLength: Long, replyBinding: TestReplyBinding): Unit = ()
  }

  private final class RecordingChunkFlusherFactory extends ChunkFlusherFactory {
    override def create(entityId: String, spool: ChunkSpool, startSeq: Long): ChunkFlusher =
      new RecordingChunkFlusher(entityId)
  }

  private final class RecordingChunkFlusher(val entityId: String) extends ChunkFlusher {
    var startCount: Int = 0
    var stopCount: Int = 0
    var drainResult: Future[Long] = Future.successful(-1L)

    override def start(): Unit = startCount += 1
    override def stop(): Unit = stopCount += 1
    override def drain(): Future[Long] = drainResult
    override def flushedSeq: Future[Long] = Future.successful(-1L)
    override def isRunning: Future[Boolean] = Future.successful(stopCount == 0)
  }

  private def withTempDir[A](body: Path => A): A = {
    val dir = Files.createTempDirectory("content-integrity-spec")
    try body(dir)
    finally deleteRecursively(dir)
  }

  private def deleteRecursively(path: Path): Unit = {
    Files.walkFileTree(path, new SimpleFileVisitor[Path]() {
      override def visitFile(file: Path, attrs: BasicFileAttributes): FileVisitResult = {
        Files.deleteIfExists(file)
        FileVisitResult.CONTINUE
      }
      override def postVisitDirectory(dir: Path, exc: java.io.IOException | Null): FileVisitResult = {
        Files.deleteIfExists(dir)
        FileVisitResult.CONTINUE
      }
    })
    ()
  }
}
