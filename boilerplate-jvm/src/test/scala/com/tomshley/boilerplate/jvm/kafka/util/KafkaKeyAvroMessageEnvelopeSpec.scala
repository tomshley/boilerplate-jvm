package com.tomshley.boilerplate.jvm.kafka.util

import com.sksamuel.avro4s.{AvroSchema, ToRecord}
import org.apache.avro.Schema
import org.apache.avro.SchemaBuilder
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.kafka.common.header.internals.RecordHeaders
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.jdk.CollectionConverters.*

// Simple test case class for avro4s
case class TestAvroRecord(value: String)

final class KafkaKeyAvroMessageEnvelopeSpec extends AnyWordSpec with Matchers {

  private val toRecord = ToRecord[TestAvroRecord](AvroSchema[TestAvroRecord])

  private def testRecord(v: String): GenericRecord = toRecord.to(TestAvroRecord(v))

  "KafkaKeyAvroMessageEnvelope" should {
    "store serviceName and key" in {
      val record = testRecord("test-value")
      val envelope = KafkaKeyAvroMessageEnvelope("service", "key123", record)
      envelope.serviceName shouldBe "service"
      envelope.key shouldBe "key123"
      envelope.avroValue shouldBe record
    }

    "generate messageBytes from avroValue" in {
      val record = testRecord("test-value")
      val envelope = KafkaKeyAvroMessageEnvelope("service", "key123", record)
      envelope.messageBytes should not be empty
    }

    "use default headers when not specified" in {
      val record = testRecord("test-value")
      val envelope = KafkaKeyAvroMessageEnvelope("service", "key123", record)
      envelope.headers shouldBe new RecordHeaders()
      envelope.headers.asScala shouldBe empty
    }

    "use custom headers when provided" in {
      val record = testRecord("test-value")
      val customHeaders = new RecordHeaders()
      customHeaders.add("test-header", "test-value".getBytes())
      val envelope = KafkaKeyAvroMessageEnvelope("service", "key123", record, customHeaders)
      envelope.headers shouldBe customHeaders
      envelope.headers.lastHeader("test-header").value() shouldBe "test-value".getBytes()
    }

    "throw descriptive exception on serialization failure" in {
      // Create a GenericRecord with a valid schema but mismatched data to trigger write failure
      val schema = SchemaBuilder
        .record("FailRecord")
        .fields()
        .requiredInt("num")
        .endRecord()
      val badRecord = new GenericData.Record(schema)
      // Leave required field unset — GenericDatumWriter will throw NullPointerException
      
      val envelope = KafkaKeyAvroMessageEnvelope("service", "key123", badRecord)
      
      val exception = intercept[RuntimeException] {
        envelope.messageBytes
      }
      exception.getMessage should include("Failed to serialize Avro message for schema FailRecord")
    }
  }
}
