package com.tomshley.boilerplate.jvm.reqreply

import com.example.idempotency.StoredIdempotency
import com.tomshley.boilerplate.jvm.security.tokens.{ExpiringSignedValue, Keyring, TokenProfile}
import com.typesafe.config.ConfigFactory
import org.apache.pekko.actor.testkit.typed.scaladsl.{ActorTestKit, LoggingTestKit, ScalaTestWithActorTestKit}
import org.apache.pekko.actor.typed.{ActorRef, Behavior}
import org.apache.pekko.actor.typed.scaladsl.Behaviors
import org.apache.pekko.cluster.MemberStatus
import org.apache.pekko.cluster.typed.{Cluster, Join}
import org.apache.pekko.pattern.StatusReply
import org.apache.pekko.persistence.typed.{PersistenceId, SnapshotAdapter}
import org.apache.pekko.persistence.testkit.scaladsl.{PersistenceTestKit, SnapshotTestKit}
import org.apache.pekko.persistence.testkit.{PersistenceTestKitPlugin, PersistenceTestKitSnapshotPlugin}
import org.apache.pekko.serialization.{SerializationExtension, SerializerWithStringManifest}
import org.scalatest.time.{Millis, Seconds, Span}
import org.scalatest.wordspec.AnyWordSpecLike

import java.time.Instant
import java.util.UUID
import scala.concurrent.{Future, Promise}
import scala.concurrent.duration.*
import scala.util.{Success, Try}

final class IdempotentPersistenceSpec
    extends ScalaTestWithActorTestKit(
      ConfigFactory.parseString(
        """
          |pekko.actor.provider = local
          |pekko.actor.allow-java-serialization = off
          |pekko.actor.serialization-bindings {
          |  "com.example.idempotency.StoredIdempotency$TestSerializable" = jackson-cbor
          |}
          |""".stripMargin)
        .withFallback(PersistenceTestKitPlugin.config)
        .withFallback(PersistenceTestKitSnapshotPlugin.config)
        .withFallback(ConfigFactory.load()))
    with AnyWordSpecLike {

  implicit override val patience: PatienceConfig =
    PatienceConfig(timeout = Span(15, Seconds), interval = Span(100, Millis))

  private val persistenceTestKit = PersistenceTestKit(system)
  private val snapshotTestKit = SnapshotTestKit(system)

  private val clusterKit = ActorTestKit(
    ConfigFactory.parseString(
      """
        |pekko.actor.provider = cluster
        |pekko.remote.artery.canonical.hostname = "127.0.0.1"
        |pekko.remote.artery.canonical.port = 0
        |pekko.cluster.jmx.enabled = off
        |pekko.actor.allow-java-serialization = off
        |pekko.actor.serialization-bindings {
        |  "com.example.idempotency.StoredIdempotency$TestSerializable" = jackson-cbor
        |}
        |""".stripMargin)
      .withFallback(PersistenceTestKitPlugin.config)
      .withFallback(PersistenceTestKitSnapshotPlugin.config)
      .withFallback(ConfigFactory.load()))

  private val clusterPersistenceTestKit = PersistenceTestKit(clusterKit.system)

  private val keyring = Keyring.fromHex(Map(1 -> "00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff"))
  private val profile = TokenProfile("persistence-spec\n", version = 1, payloadLength = 0, grace = Duration.Zero)

  private def freshRequestId(): ExpiringSignedValue = {
    val expiresAt = Instant.now().plusSeconds(60)
    val encoded = ExpiringSignedValue.sign(keyring, profile, UUID.randomUUID().toString, expiresAt)
    ExpiringSignedValue.verify(keyring, profile, encoded, Instant.now()).toOption.get
  }

  private val acknowledgedId = freshRequestId()
  private val rejectedId = freshRequestId()
  private val cleanupFailedId = freshRequestId()
  private val timeoutId = freshRequestId()
  private val acknowledgedProbe = clusterKit.createTestProbe[Idempotent.Command]()
  private val rejectedProbe = clusterKit.createTestProbe[Idempotent.Command]()
  private val cleanupFailedProbe = clusterKit.createTestProbe[Idempotent.Command]()
  private val timeoutProbe = clusterKit.createTestProbe[Idempotent.Command]()
  private val forwardingProbes = Map(
    acknowledgedId.value -> acknowledgedProbe.ref,
    rejectedId.value -> rejectedProbe.ref,
    cleanupFailedId.value -> cleanupFailedProbe.ref,
    timeoutId.value -> timeoutProbe.ref)

  private def durable(entityId: String): Behavior[Idempotent.Command] =
    Idempotent.persistent(
      entityId,
      PersistenceId(Idempotent.EntityKey.name, entityId),
      StoredIdempotency.eventAdapter,
      StoredIdempotency.snapshotAdapter)

  private def forwarding(to: ActorRef[Idempotent.Command]): Behavior[Idempotent.Command] =
    Behaviors.receiveMessage { command =>
      to ! command
      Behaviors.same
    }

  private def persistentRef(key: String, pid: PersistenceId): ActorRef[Idempotent.Command] =
    spawn(Idempotent.persistent(key, pid, StoredIdempotency.eventAdapter, StoredIdempotency.snapshotAdapter))

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    Cluster(clusterKit.system).manager ! Join(Cluster(clusterKit.system).selfMember.address)
    Idempotent.init(clusterKit.system, ctx =>
      forwardingProbes.get(ctx.entityId).fold(durable(ctx.entityId))(forwarding))
    eventually {
      Cluster(clusterKit.system).selfMember.status shouldBe MemberStatus.Up
    }
  }

  override protected def afterAll(): Unit = {
    clusterKit.shutdownTestKit()
    super.afterAll()
  }

  "Idempotent.persistent" should {

    "record no journal event for a status query and not claim the key" in {
      val probe = createTestProbe[StatusReply[Idempotent.Summary]]()
      val pid = PersistenceId.ofUniqueId(s"status-only-${UUID.randomUUID()}")
      val ref = persistentRef("status-key", pid)

      ref ! Idempotent.StatusRequest(probe.ref)
      val reply = probe.receiveMessage()
      reply.isSuccess shouldBe true
      reply.getValue.replyBody shouldBe None
      reply.getValue.isIdempotent shouldBe false
      persistenceTestKit.expectNothingPersisted(pid.id, 500.millis)

      ref ! Idempotent.SingleRequest(None, None, probe.ref)
      probe.receiveMessage().getValue.isIdempotent shouldBe false
      persistenceTestKit.expectNextPersistedType[StoredIdempotency.Started](pid.id)
      testKit.stop(ref)
    }

    "reject a reply before any claim without journaling or changing state" in {
      val probe = createTestProbe[StatusReply[Idempotent.Summary]]()
      val key = s"key-${UUID.randomUUID()}"
      val pid = PersistenceId.ofUniqueId(s"unclaimed-reply-$key")
      val ref = persistentRef(key, pid)

      ref ! Idempotent.SingleReply(Some(Map("h" -> "v")), Some("unexpected"), probe.ref)
      val rejected = probe.receiveMessage()
      rejected.isError shouldBe true
      rejected.getError.getMessage shouldBe "Invalid idempotent state"
      ref ! Idempotent.StatusRequest(probe.ref)
      probe.receiveMessage().getValue shouldBe Idempotent.State.empty.toSummary
      persistenceTestKit.expectNothingPersisted(pid.id, 300.millis)
      testKit.stop(ref)

      val restarted = persistentRef(key, pid)
      restarted ! Idempotent.StatusRequest(probe.ref)
      probe.receiveMessage().getValue shouldBe Idempotent.State.empty.toSummary
      persistenceTestKit.expectNothingPersisted(pid.id, 300.millis)

      restarted ! Idempotent.SingleRequest(None, None, probe.ref)
      val claimed = probe.receiveMessage().getValue
      claimed.idempotencyKey shouldBe key
      claimed.isIdempotent shouldBe false
      persistenceTestKit.expectNextPersistedType[StoredIdempotency.Started](pid.id)
      restarted ! Idempotent.SingleReply(None, Some("accepted"), probe.ref)
      probe.receiveMessage().getValue.replyBody shouldBe Some("accepted")
      persistenceTestKit.expectNextPersistedType[StoredIdempotency.SavedReply](pid.id)
      testKit.stop(restarted)
    }

    "persist the first claim, mark repeats pending duplicates, and keep the first reply" in {
      val probe = createTestProbe[StatusReply[Idempotent.Summary]]()
      val key = s"key-${UUID.randomUUID()}"
      val pid = PersistenceId.ofUniqueId(s"persist-$key")
      val ref = persistentRef(key, pid)

      ref ! Idempotent.SingleRequest(Some(Map("h" -> "v")), Some("req"), probe.ref)
      val claimed = probe.receiveMessage()
      claimed.isSuccess shouldBe true
      claimed.getValue.isIdempotent shouldBe false
      val started =
        persistenceTestKit.expectNextPersistedType[StoredIdempotency.Started](pid.id)
      started.key shouldBe key
      started.headers shouldBe Some(Map("h" -> "v"))
      started.body shouldBe Some("req")

      ref ! Idempotent.SingleRequest(None, None, probe.ref)
      val repeated = probe.receiveMessage()
      repeated.isSuccess shouldBe true
      repeated.getValue.isIdempotent shouldBe true
      persistenceTestKit.expectNextPersistedType[StoredIdempotency.RepeatedRequest](pid.id)

      ref ! Idempotent.SingleReply(Some(Map("rh" -> "rv")), Some("first"), probe.ref)
      val recorded = probe.receiveMessage()
      recorded.isSuccess shouldBe true
      recorded.getValue.replyBody shouldBe Some("first")
      val saved =
        persistenceTestKit.expectNextPersistedType[StoredIdempotency.SavedReply](pid.id)
      saved.headers shouldBe Some(Map("rh" -> "rv"))
      saved.body shouldBe Some("first")

      ref ! Idempotent.SingleReply(Some(Map("other" -> "ignored")), Some("second"), probe.ref)
      val repeatedReply = probe.receiveMessage()
      repeatedReply.isSuccess shouldBe true
      repeatedReply.getValue.isIdempotent shouldBe true
      repeatedReply.getValue.replyBody shouldBe Some("first")
      repeatedReply.getValue.replyHeaders shouldBe Some(Map("rh" -> "rv"))
      persistenceTestKit.expectNextPersistedType[StoredIdempotency.RepeatedReply](pid.id)
      testKit.stop(ref)
    }

    "persist only consumer DTOs with the consumer serializer binding" in {
      val probe = createTestProbe[StatusReply[Idempotent.Summary]]()
      val key = s"key-${UUID.randomUUID()}"
      val pid = PersistenceId.ofUniqueId(s"dto-$key")
      val ref = persistentRef(key, pid)

      ref ! Idempotent.SingleRequest(None, Some("req"), probe.ref)
      probe.receiveMessage().isSuccess shouldBe true
      ref ! Idempotent.SingleReply(None, Some("rep"), probe.ref)
      probe.receiveMessage().isSuccess shouldBe true

      val stored = persistenceTestKit.persistedInStorage(pid.id)
      stored.length shouldBe 2
      stored.foreach { value =>
        value shouldBe a[StoredIdempotency.StoredEvent]
        val serializer = SerializationExtension
          .get(system.classicSystem)
          .serializerFor(value.getClass)
          .asInstanceOf[SerializerWithStringManifest]
        serializer.getClass.getName should include("JacksonCbor")
        serializer.manifest(value.asInstanceOf[AnyRef]) should startWith("com.example.idempotency.")
      }
      testKit.stop(ref)
    }

    "replay a completed reply exactly after restart on the same persistence id" in {
      val probe = createTestProbe[StatusReply[Idempotent.Summary]]()
      val key = s"key-${UUID.randomUUID()}"
      val pid = PersistenceId.ofUniqueId(s"replay-$key")
      val ref = persistentRef(key, pid)

      ref ! Idempotent.SingleRequest(None, Some("req"), probe.ref)
      probe.receiveMessage().isSuccess shouldBe true
      ref ! Idempotent.SingleReply(Some(Map("rh" -> "rv")), Some("rep"), probe.ref)
      val completed = probe.receiveMessage()
      completed.isSuccess shouldBe true
      testKit.stop(ref)

      val restarted = persistentRef(key, pid)
      restarted ! Idempotent.StatusRequest(probe.ref)
      val replayed = probe.receiveMessage()
      replayed.isSuccess shouldBe true
      replayed.getValue.idempotencyKey shouldBe key
      replayed.getValue.replyBody shouldBe Some("rep")
      replayed.getValue.replyHeaders shouldBe Some(Map("rh" -> "rv"))
      replayed.getValue.repliedOn shouldBe completed.getValue.repliedOn
      testKit.stop(restarted)
    }

    "recover a pending claim as pending without re-executing it" in {
      val probe = createTestProbe[StatusReply[Idempotent.Summary]]()
      val key = s"key-${UUID.randomUUID()}"
      val pid = PersistenceId.ofUniqueId(s"pending-$key")
      val ref = persistentRef(key, pid)

      ref ! Idempotent.SingleRequest(None, Some("req"), probe.ref)
      probe.receiveMessage().isSuccess shouldBe true
      testKit.stop(ref)

      val restarted = persistentRef(key, pid)
      restarted ! Idempotent.StatusRequest(probe.ref)
      val status = probe.receiveMessage()
      status.isSuccess shouldBe true
      status.getValue.isIdempotent shouldBe false
      status.getValue.replyBody shouldBe None

      restarted ! Idempotent.SingleRequest(None, None, probe.ref)
      val duplicate = probe.receiveMessage()
      duplicate.isSuccess shouldBe true
      duplicate.getValue.isIdempotent shouldBe true
      duplicate.getValue.replyBody shouldBe None
      testKit.stop(restarted)
    }

    "recover released as fresh" in {
      val probe = createTestProbe[StatusReply[Idempotent.Summary]]()
      val key = s"key-${UUID.randomUUID()}"
      val pid = PersistenceId.ofUniqueId(s"released-$key")
      val ref = persistentRef(key, pid)

      ref ! Idempotent.SingleRequest(None, None, probe.ref)
      probe.receiveMessage().isSuccess shouldBe true
      persistenceTestKit.expectNextPersistedType[StoredIdempotency.Started](pid.id)
      ref ! Idempotent.AbandonRequest(probe.ref)
      probe.receiveMessage().isSuccess shouldBe true
      persistenceTestKit.expectNextPersistedType[StoredIdempotency.Abandoned](pid.id)
      testKit.stop(ref)

      val restarted = persistentRef(key, pid)
      restarted ! Idempotent.SingleRequest(None, None, probe.ref)
      val fresh = probe.receiveMessage()
      fresh.isSuccess shouldBe true
      fresh.getValue.isIdempotent shouldBe false
      testKit.stop(restarted)
    }

    "retain the stored reply across an abandon" in {
      val probe = createTestProbe[StatusReply[Idempotent.Summary]]()
      val key = s"key-${UUID.randomUUID()}"
      val pid = PersistenceId.ofUniqueId(s"retain-$key")
      val ref = persistentRef(key, pid)

      ref ! Idempotent.SingleRequest(None, None, probe.ref)
      probe.receiveMessage().isSuccess shouldBe true
      ref ! Idempotent.SingleReply(None, Some("rep"), probe.ref)
      probe.receiveMessage().isSuccess shouldBe true
      ref ! Idempotent.AbandonRequest(probe.ref)
      val after = probe.receiveMessage()
      after.isSuccess shouldBe true
      after.getValue.replyBody shouldBe Some("rep")
      testKit.stop(ref)
    }

    "snapshot the consumer state DTO and recover through it" in {
      system.settings.config
        .getBoolean("pekko.persistence.testkit.snapshots.serialize") shouldBe true
      system.settings.config
        .getBoolean("pekko.persistence.testkit.events.serialize") shouldBe true

      val probe = createTestProbe[StatusReply[Idempotent.Summary]]()
      val key = s"key-${UUID.randomUUID()}"
      val pid = PersistenceId.ofUniqueId(s"snap-$key")
      val ref = spawn(
        Idempotent
          .persistent(key, pid, StoredIdempotency.eventAdapter, StoredIdempotency.snapshotAdapter)
          .snapshotWhen((_, _, _) => true))

      ref ! Idempotent.SingleRequest(None, Some("req"), probe.ref)
      probe.receiveMessage().isSuccess shouldBe true
      eventually {
        snapshotTestKit.persistedInStorage(pid.id).length shouldBe 1
      }
      val claimStored = snapshotTestKit.persistedInStorage(pid.id).head._2
      claimStored shouldBe a[StoredIdempotency.StoredState]
      val claimState = claimStored.asInstanceOf[StoredIdempotency.StoredState]
      claimState.idempotencyKey shouldBe Some(key)
      claimState.requestCount shouldBe 1
      claimState.replyCount shouldBe 0
      claimState.requestedOn shouldBe defined
      claimState.repliedOn shouldBe empty

      ref ! Idempotent.SingleReply(Some(Map("rh" -> "rv")), Some("rep"), probe.ref)
      val completed = probe.receiveMessage()
      completed.isSuccess shouldBe true
      eventually {
        snapshotTestKit.persistedInStorage(pid.id).length shouldBe 2
      }
      val replyStored = snapshotTestKit.persistedInStorage(pid.id).last._2
      replyStored shouldBe a[StoredIdempotency.StoredState]
      val replyState = replyStored.asInstanceOf[StoredIdempotency.StoredState]
      replyState.requestedOn shouldBe claimState.requestedOn
      replyState.repliedOn shouldBe completed.getValue.repliedOn
      replyState.requestCount shouldBe 1
      replyState.replyCount shouldBe 1
      replyState.replyHeaders shouldBe Some(Map("rh" -> "rv"))
      replyState.replyBody shouldBe Some("rep")
      testKit.stop(ref)

      val restored = createTestProbe[Idempotent.State]()
      val observingAdapter = new SnapshotAdapter[Idempotent.State] {
        override def toJournal(state: Idempotent.State): Any =
          StoredIdempotency.snapshotAdapter.toJournal(state)
        override def fromJournal(value: Any): Idempotent.State = {
          val state = StoredIdempotency.snapshotAdapter.fromJournal(value)
          restored.ref ! state
          state
        }
      }
      val restarted = spawn(
        Idempotent.persistent(key, pid, StoredIdempotency.eventAdapter, observingAdapter))
      restarted ! Idempotent.StatusRequest(probe.ref)
      val recovered = probe.receiveMessage()
      recovered.isSuccess shouldBe true
      recovered.getValue.idempotencyKey shouldBe key
      recovered.getValue.replyBody shouldBe Some("rep")
      recovered.getValue.repliedOn shouldBe completed.getValue.repliedOn
      restored.receiveMessage() shouldBe
        StoredIdempotency.snapshotAdapter.fromJournal(replyState)
      testKit.stop(restarted)
    }

    "record no event for an empty abandon while a release stays journaled" in {
      val probe = createTestProbe[StatusReply[Idempotent.Summary]]()
      val key = s"key-${UUID.randomUUID()}"
      val pid = PersistenceId.ofUniqueId(s"empty-abandon-$key")
      val ref = persistentRef(key, pid)

      ref ! Idempotent.AbandonRequest(probe.ref)
      probe.receiveMessage().getValue shouldBe Idempotent.State.empty.toSummary
      ref ! Idempotent.AbandonRequest(probe.ref)
      probe.receiveMessage().getValue shouldBe Idempotent.State.empty.toSummary
      persistenceTestKit.expectNothingPersisted(pid.id, 300.millis)

      ref ! Idempotent.SingleRequest(None, None, probe.ref)
      probe.receiveMessage().isSuccess shouldBe true
      persistenceTestKit.expectNextPersistedType[StoredIdempotency.Started](pid.id)

      ref ! Idempotent.AbandonRequest(probe.ref)
      probe.receiveMessage().getValue shouldBe Idempotent.State.empty.toSummary
      persistenceTestKit.expectNextPersistedType[StoredIdempotency.Abandoned](pid.id)

      ref ! Idempotent.AbandonRequest(probe.ref)
      probe.receiveMessage().getValue shouldBe Idempotent.State.empty.toSummary
      persistenceTestKit.expectNothingPersisted(pid.id, 300.millis)
      testKit.stop(ref)

      val restarted = persistentRef(key, pid)
      restarted ! Idempotent.StatusRequest(probe.ref)
      val recovered = probe.receiveMessage()
      recovered.isSuccess shouldBe true
      recovered.getValue shouldBe Idempotent.State.empty.toSummary
      testKit.stop(restarted)
    }

    "persist nothing and answer no reply when the journal rejects a claim" in {
      val probe = createTestProbe[StatusReply[Idempotent.Summary]]()
      val terminated = createTestProbe[Nothing]()
      val key = s"key-${UUID.randomUUID()}"
      val pid = PersistenceId.ofUniqueId(s"fail-claim-$key")
      val ref = persistentRef(key, pid)

      persistenceTestKit.failNextPersisted(pid.id)
      ref ! Idempotent.SingleRequest(None, None, probe.ref)
      terminated.expectTerminated(ref, 5.seconds)
      probe.expectNoMessage(300.millis)
      persistenceTestKit.persistedInStorage(pid.id) shouldBe empty

      val restarted = persistentRef(key, pid)
      restarted ! Idempotent.StatusRequest(probe.ref)
      val recovered = probe.receiveMessage(10.seconds)
      recovered.isSuccess shouldBe true
      recovered.getValue.isIdempotent shouldBe false
      recovered.getValue.replyBody shouldBe None
      testKit.stop(restarted)
    }

    "lose only the failed reply write while the claim survives a restart" in {
      val probe = createTestProbe[StatusReply[Idempotent.Summary]]()
      val terminated = createTestProbe[Nothing]()
      val key = s"key-${UUID.randomUUID()}"
      val pid = PersistenceId.ofUniqueId(s"fail-reply-$key")
      val ref = persistentRef(key, pid)

      ref ! Idempotent.SingleRequest(None, None, probe.ref)
      probe.receiveMessage().isSuccess shouldBe true
      persistenceTestKit.expectNextPersistedType[StoredIdempotency.Started](pid.id)

      persistenceTestKit.failNextPersisted(pid.id)
      ref ! Idempotent.SingleReply(None, Some("rep"), probe.ref)
      terminated.expectTerminated(ref, 5.seconds)
      probe.expectNoMessage(300.millis)
      persistenceTestKit.persistedInStorage(pid.id).length shouldBe 1

      val restarted = persistentRef(key, pid)
      restarted ! Idempotent.StatusRequest(probe.ref)
      val recovered = probe.receiveMessage(10.seconds)
      recovered.isSuccess shouldBe true
      recovered.getValue.replyBody shouldBe None

      restarted ! Idempotent.SingleRequest(None, None, probe.ref)
      val duplicate = probe.receiveMessage()
      duplicate.isSuccess shouldBe true
      duplicate.getValue.isIdempotent shouldBe true
      testKit.stop(restarted)
    }
  }

  "Idempotency.reqReply over sharding" should {

    "complete only after the entity acknowledges the recorded reply" in {
      val idempotency = Idempotency(clusterKit.system)
      val completions =
        clusterKit.createTestProbe[Try[Idempotency.RequestReply]]()
      val reply = Idempotency.RequestReply(Some(Map("h" -> "v")), Some("body"))

      val result = idempotency.reqReply(acknowledgedId, Future.successful(reply))
      result.onComplete(completions.ref ! _)(clusterKit.system.executionContext)
      acknowledgedProbe.receiveMessage(5.seconds) match {
        case Idempotent.SingleRequest(_, _, replyTo) =>
          replyTo ! StatusReply.Success(
            Idempotent.Summary(acknowledgedId.value, isIdempotent = false, None, None, None))
        case other => fail(s"expected SingleRequest, got $other")
      }
      val recorded = acknowledgedProbe.receiveMessage(5.seconds) match {
        case r: Idempotent.SingleReply => r
        case other => fail(s"expected SingleReply, got $other")
      }
      recorded.headers shouldBe reply.headers
      recorded.body shouldBe reply.body
      completions.expectNoMessage(300.millis)
      recorded.replyTo ! StatusReply.Success(
        Idempotent.Summary(acknowledgedId.value, isIdempotent = false, Some(Instant.now()), reply.headers, reply.body))
      completions.receiveMessage() shouldBe Success(reply)
      result.futureValue shouldBe reply
    }

    "fail on a record error without releasing the claim" in {
      val idempotency = Idempotency(clusterKit.system)
      val reply = Idempotency.RequestReply(None, Some("body"))
      val failure = new IllegalStateException("record failed")

      LoggingTestKit
        .warn("Idempotency reply recording was not confirmed")
        .expect {
          val result = idempotency.reqReply(rejectedId, Future.successful(reply))
          rejectedProbe.receiveMessage(5.seconds) match {
            case Idempotent.SingleRequest(_, _, replyTo) =>
              replyTo ! StatusReply.Success(
                Idempotent.Summary(rejectedId.value, isIdempotent = false, None, None, None))
            case other => fail(s"expected SingleRequest, got $other")
          }
          rejectedProbe.receiveMessage(5.seconds) match {
            case r: Idempotent.SingleReply =>
              r.replyTo ! StatusReply.error(failure)
            case other => fail(s"expected SingleReply, got $other")
          }
          result.failed.futureValue should be theSameInstanceAs failure
        }
      rejectedProbe.expectNoMessage(300.millis)
    }

    "propagate the callback failure when the claim release is not acknowledged" in {
      val idempotency = Idempotency(clusterKit.system)
      val failure = new IllegalStateException("callback failed")

      LoggingTestKit
        .warn("Idempotency claim release was not confirmed")
        .expect {
          val result = idempotency.reqReply(cleanupFailedId, Future.failed(failure))
          cleanupFailedProbe.receiveMessage(5.seconds) match {
            case Idempotent.SingleRequest(_, _, replyTo) =>
              replyTo ! StatusReply.Success(
                Idempotent.Summary(cleanupFailedId.value, isIdempotent = false, None, None, None))
            case other => fail(s"expected SingleRequest, got $other")
          }
          cleanupFailedProbe.receiveMessage(5.seconds) match {
            case r: Idempotent.AbandonRequest =>
              r.replyTo ! StatusReply.error("release failed")
            case other => fail(s"expected AbandonRequest, got $other")
          }
          result.failed.futureValue should be theSameInstanceAs failure
        }
      cleanupFailedProbe.expectNoMessage(300.millis)
    }

    "keep a claim pending when the durable record write is lost" in {
      val idempotency = Idempotency(clusterKit.system)
      val id = freshRequestId()
      val pid = PersistenceId(Idempotent.EntityKey.name, id.value).id
      val reply = Idempotency.RequestReply(None, Some("body"))
      val gate = Promise[Idempotency.RequestReply]()

      val pending = idempotency.reqReplyOutcome(id, gate.future)
      eventually {
        clusterPersistenceTestKit
          .persistedInStorage(pid)
          .exists(_.isInstanceOf[StoredIdempotency.Started]) shouldBe true
      }
      clusterPersistenceTestKit.failNextPersisted(pid)
      gate.success(reply)
      pending.failed.futureValue

      val status = idempotency.idempotencyStatus(id).futureValue
      status.replyBody shouldBe None
      status.isIdempotent shouldBe false

      val replay = idempotency
        .reqReplyOutcome(id, Future.failed(new AssertionError("duplicate callback ran")))
        .futureValue
      replay shouldBe Idempotency.Outcome.Pending
    }

    "return a completed reqReply only once its reply is journaled" in {
      val idempotency = Idempotency(clusterKit.system)
      val id = freshRequestId()
      val pid = PersistenceId(Idempotent.EntityKey.name, id.value).id
      val reply = Idempotency.RequestReply(Some(Map("h" -> "v")), Some("body"))

      idempotency
        .reqReply(id, Future.successful(reply))
        .futureValue shouldBe reply

      val stored = clusterPersistenceTestKit.persistedInStorage(pid)
      stored.exists(_.isInstanceOf[StoredIdempotency.SavedReply]) shouldBe true
      stored.forall(_.isInstanceOf[StoredIdempotency.StoredEvent]) shouldBe true

      idempotency.idempotencyStatus(id).futureValue.replyBody shouldBe Some("body")

      idempotency
        .reqReply(id, Future.failed(new AssertionError("duplicate callback ran")))
        .futureValue shouldBe reply
    }

    "release the claim on callback failure so a retry runs again" in {
      val idempotency = Idempotency(clusterKit.system)
      val id = freshRequestId()
      val pid = PersistenceId(Idempotent.EntityKey.name, id.value).id
      val failure = new IllegalStateException("downstream")

      idempotency
        .reqReply(id, Future.failed(failure))
        .failed
        .futureValue shouldBe failure

      eventually {
        clusterPersistenceTestKit
          .persistedInStorage(pid)
          .exists(_.isInstanceOf[StoredIdempotency.Abandoned]) shouldBe true
      }

      idempotency
        .reqReply(id, Future.successful(Idempotency.RequestReply(None, Some("body"))))
        .futureValue shouldBe Idempotency.RequestReply(None, Some("body"))
    }
  }

  "Idempotency.release over sharding" should {

    "fail an unacknowledged release without clearing the pending claim" in {
      val idempotency = Idempotency(clusterKit.system)
      val id = freshRequestId()
      val pid = PersistenceId(Idempotent.EntityKey.name, id.value).id

      idempotency.idempotencyResult(id).futureValue
      eventually {
        clusterPersistenceTestKit
          .persistedInStorage(pid)
          .exists(_.isInstanceOf[StoredIdempotency.Started]) shouldBe true
      }

      clusterPersistenceTestKit.failNextPersisted(pid)
      idempotency.release(id).failed.futureValue

      val status = idempotency.idempotencyStatus(id).futureValue
      status.replyBody shouldBe None
      status.isIdempotent shouldBe false

      idempotency
        .reqReply(id, Future.failed(new AssertionError("duplicate callback ran")))
        .futureValue shouldBe Idempotency.RequestReply(None, None)

      idempotency.release(id).futureValue shouldBe Idempotent.State.empty.toSummary
      clusterPersistenceTestKit
        .persistedInStorage(pid)
        .exists(_.isInstanceOf[StoredIdempotency.Abandoned]) shouldBe true

      idempotency
        .reqReply(id, Future.successful(Idempotency.RequestReply(None, Some("body"))))
        .futureValue shouldBe Idempotency.RequestReply(None, Some("body"))
    }

    "preserve a completed reply and write nothing on release" in {
      val idempotency = Idempotency(clusterKit.system)
      val id = freshRequestId()
      val pid = PersistenceId(Idempotent.EntityKey.name, id.value).id
      val reply = Idempotency.RequestReply(Some(Map("h" -> "v")), Some("body"))

      idempotency.reqReply(id, Future.successful(reply)).futureValue shouldBe reply
      val journalSize = clusterPersistenceTestKit.persistedInStorage(pid).length

      val released = idempotency.release(id).futureValue
      released.replyHeaders shouldBe Some(Map("h" -> "v"))
      released.replyBody shouldBe Some("body")
      clusterPersistenceTestKit.persistedInStorage(pid).length shouldBe journalSize

      idempotency
        .reqReply(id, Future.failed(new AssertionError("duplicate callback ran")))
        .futureValue shouldBe reply
    }

    "write nothing for releasing an unclaimed key" in {
      val idempotency = Idempotency(clusterKit.system)
      val id = freshRequestId()
      val pid = PersistenceId(Idempotent.EntityKey.name, id.value).id

      idempotency.release(id).futureValue shouldBe Idempotent.State.empty.toSummary
      idempotency.release(id).futureValue shouldBe Idempotent.State.empty.toSummary
      clusterPersistenceTestKit.persistedInStorage(pid) shouldBe empty
    }
  }

  "Idempotency.reqReplyOutcome over sharding" should {

    "fail without releasing when the record acknowledgement is uncertain" in {
      val idempotency = Idempotency(clusterKit.system)
      val reply = Idempotency.RequestReply(None, Some("body"))

      LoggingTestKit
        .warn("Idempotency reply recording was not confirmed")
        .expect {
          val result = idempotency.reqReplyOutcome(timeoutId, Future.successful(reply))
          timeoutProbe.receiveMessage(5.seconds) match {
            case Idempotent.SingleRequest(_, _, replyTo) =>
              replyTo ! StatusReply.Success(
                Idempotent.Summary(timeoutId.value, isIdempotent = false, None, None, None))
            case other => fail(s"expected SingleRequest, got $other")
          }
          timeoutProbe.receiveMessage(5.seconds) match {
            case _: Idempotent.SingleReply => ()
            case other => fail(s"expected SingleReply, got $other")
          }
          val failed = result.failed.futureValue
          failed shouldBe a[java.util.concurrent.TimeoutException]
        }
      timeoutProbe.expectNoMessage(300.millis)
    }

    "record a deliberately empty typed reply as Completed" in {
      val idempotency = Idempotency(clusterKit.system)
      val id = freshRequestId()
      val pid = PersistenceId(Idempotent.EntityKey.name, id.value).id
      val emptyReply = Idempotency.RequestReply(None, None)

      idempotency
        .reqReplyOutcome(id, Future.successful(emptyReply))
        .futureValue shouldBe Idempotency.Outcome.Completed(emptyReply)

      val saved = clusterPersistenceTestKit
        .persistedInStorage(pid)
        .collect { case r: StoredIdempotency.SavedReply => r }
      saved.length shouldBe 1
      saved.head.body shouldBe empty

      idempotency
        .reqReplyOutcome(id, Future.failed(new AssertionError("duplicate callback ran")))
        .futureValue shouldBe Idempotency.Outcome.Completed(emptyReply)
    }

    "return Pending for a concurrent duplicate until the first reply is journaled" in {
      val idempotency = Idempotency(clusterKit.system)
      val id = freshRequestId()
      val pid = PersistenceId(Idempotent.EntityKey.name, id.value).id
      val started = Promise[Unit]()
      val gate = Promise[Idempotency.RequestReply]()
      val reply = Idempotency.RequestReply(Some(Map("h" -> "v")), Some("body"))

      val first = idempotency.reqReplyOutcome(id, {
        started.success(())
        gate.future
      })
      started.future.futureValue

      idempotency
        .reqReplyOutcome(id, Future.failed(new AssertionError("duplicate callback ran")))
        .futureValue shouldBe Idempotency.Outcome.Pending

      gate.success(reply)
      first.futureValue shouldBe Idempotency.Outcome.Completed(reply)

      val saved = clusterPersistenceTestKit
        .persistedInStorage(pid)
        .collect { case r: StoredIdempotency.SavedReply => r }
      saved.length shouldBe 1
      saved.head.headers shouldBe Some(Map("h" -> "v"))
      saved.head.body shouldBe Some("body")

      idempotency
        .reqReplyOutcome(id, Future.failed(new AssertionError("duplicate callback ran")))
        .futureValue shouldBe Idempotency.Outcome.Completed(reply)
    }
  }

  "StoredIdempotency adapters" should {

    "map each event one-to-one under manifest 1 and fail unknown input explicitly" in {
      val on = Instant.now()
      val cases = List(
        Idempotent.RequestRecorded("k", on, Some(Map("h" -> "v")), Some("b")) ->
          StoredIdempotency.Started("k", on, Some(Map("h" -> "v")), Some("b")),
        Idempotent.RequestRepeated -> StoredIdempotency.RepeatedRequest(),
        Idempotent.ReplyRecorded(on, Some(Map("rh" -> "rv")), Some("rb")) ->
          StoredIdempotency.SavedReply(on, Some(Map("rh" -> "rv")), Some("rb")),
        Idempotent.ReplyRepeated -> StoredIdempotency.RepeatedReply(),
        Idempotent.RequestAbandoned -> StoredIdempotency.Abandoned()
      )
      cases.foreach { case (event, stored) =>
        StoredIdempotency.eventAdapter.toJournal(event) shouldBe stored
        StoredIdempotency.eventAdapter.manifest(event) shouldBe "1"
        StoredIdempotency.eventAdapter.fromJournal(stored, "1").events shouldBe Seq(event)
      }
      an[IllegalArgumentException] should be thrownBy {
        StoredIdempotency.eventAdapter.fromJournal(StoredIdempotency.RepeatedRequest(), "2")
      }
    }

    "round-trip every state field through the snapshot DTO and fail unknown input" in {
      val state = Idempotent.State(
        Some("k"),
        Some(Instant.now().minusSeconds(5)),
        Some(Instant.now()),
        requestCount = 3,
        replyCount = 2,
        requestHeaders = Some(Map("h" -> "v")),
        requestBody = Some("req"),
        replyHeaders = Some(Map("rh" -> "rv")),
        replyBody = Some("rep"))
      val stored = StoredIdempotency.snapshotAdapter.toJournal(state)
      stored shouldBe a[StoredIdempotency.StoredState]
      StoredIdempotency.snapshotAdapter.fromJournal(stored) shouldBe state
      an[IllegalArgumentException] should be thrownBy {
        StoredIdempotency.snapshotAdapter.fromJournal("nonsense")
      }
    }
  }
}
