package com.tomshley.boilerplate.jvm.marshalling

import com.tomshley.boilerplate.jvm.kafka.util.KafkaKeyAvroMessageEnvelope
import com.tomshley.boilerplate.jvm.marshalling.models.MarshallModel
import org.apache.avro.Schema
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.ExecutionContext

// ── Test fixtures — MUST be top-level for avro4s macro derivation ──

final case class TestAvroEvent(id: String, ts: Long, count: Int)
    extends MarshallModel[TestAvroEvent]

// Sealed trait + case objects for Avro ENUM (testing bare Magnolia derivation)
sealed trait TestStatus
case object ACTIVE extends TestStatus
case object INACTIVE extends TestStatus

final case class TestAvroEventWithStatus(id: String, ts: Long, status: TestStatus)
    extends MarshallModel[TestAvroEventWithStatus]

// ── Tests ──

final class AvroMarshallerSpec extends AnyWordSpec with Matchers with ScalaFutures {

  given ExecutionContext = ExecutionContext.global

  "AvroMarshaller" should {
    "toRecord and fromRecord round-trip" in {
      val event = TestAvroEvent("evt-1", 1000L, 42)
      val record = AvroMarshaller.toRecord(event)
      val back = AvroMarshaller.fromRecord[TestAvroEvent](record)
      back shouldBe event
    }

    "schema returns correct field names and types" in {
      val s = AvroMarshaller.schema[TestAvroEvent]
      s.getType shouldBe Schema.Type.RECORD
      s.getField("id") should not be null
      s.getField("ts") should not be null
      s.getField("count") should not be null
    }

    "toRecordAsync and fromRecordAsync round-trip" in {
      val event = TestAvroEvent("evt-2", 2000L, 99)
      val recordF = AvroMarshaller.toRecordAsync(event, summon[ExecutionContext])
      val record = recordF.futureValue
      val backF = AvroMarshaller.fromRecordAsync[TestAvroEvent](record, summon[ExecutionContext])
      val back = backF.futureValue
      back shouldBe event
    }

    "KafkaKeyAvroMessageEnvelope model overload produces valid envelope" in {
      val event = TestAvroEvent("evt-3", 3000L, 7)
      val envelope = KafkaKeyAvroMessageEnvelope("test-service", "key-1", event)
      envelope.serviceName shouldBe "test-service"
      envelope.key shouldBe "key-1"
      envelope.messageBytes should not be empty
    }

    "sealed trait enum round-trips correctly via AvroEnumMarshalling" in {
      val active = TestAvroEventWithStatus("a", 1L, ACTIVE)
      val inactive = TestAvroEventWithStatus("b", 2L, INACTIVE)
      AvroMarshaller.fromRecord[TestAvroEventWithStatus](AvroMarshaller.toRecord(active)).status shouldBe ACTIVE
      AvroMarshaller.fromRecord[TestAvroEventWithStatus](AvroMarshaller.toRecord(inactive)).status shouldBe INACTIVE
    }
  }
}
