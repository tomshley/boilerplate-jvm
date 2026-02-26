package com.tomshley.boilerplate.jvm.kafka.util

import org.apache.avro.Schema
import org.apache.avro.SchemaBuilder
import org.apache.avro.specific.SpecificRecord
import org.apache.kafka.common.header.internals.RecordHeaders
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.jdk.CollectionConverters.*

// Simple test implementation of SpecificRecord for testing purposes
case class TestAvroRecord(value: String) extends SpecificRecord {
  override def getSchema: Schema = TestAvroRecord.SCHEMA
  override def get(i: Int): AnyRef = i match {
    case 0 => value.asInstanceOf[AnyRef]
    case _ => throw new IndexOutOfBoundsException(s"Invalid index: $i")
  }
  override def put(i: Int, v: scala.Any): Unit = i match {
    case 0 => // value field is immutable
    case _ => throw new IndexOutOfBoundsException(s"Invalid index: $i")
  }
}

object TestAvroRecord {
  val SCHEMA: Schema = SchemaBuilder
    .record("TestAvroRecord")
    .fields()
    .requiredString("value")
    .endRecord()
}

final class KafkaKeyAvroMessageEnvelopeSpec extends AnyWordSpec with Matchers {

  "KafkaKeyAvroMessageEnvelope" should {
    "store serviceName and key" in {
      val testMessage = TestAvroRecord("test-value")
      val envelope = KafkaKeyAvroMessageEnvelope("service", "key123", testMessage)
      envelope.serviceName shouldBe "service"
      envelope.key shouldBe "key123"
      envelope.avroValue shouldBe testMessage
    }

    "generate messageBytes from avroValue" in {
      val testMessage = TestAvroRecord("test-value")
      val envelope = KafkaKeyAvroMessageEnvelope("service", "key123", testMessage)
      envelope.messageBytes should not be empty
      envelope.messageBytes should have length 11 // Avro binary encoding of "test-value"
    }

    "use default headers when not specified" in {
      val testMessage = TestAvroRecord("test-value")
      val envelope = KafkaKeyAvroMessageEnvelope("service", "key123", testMessage)
      envelope.headers shouldBe new RecordHeaders()
      envelope.headers.asScala shouldBe empty
    }

    "use custom headers when provided" in {
      val testMessage = TestAvroRecord("test-value")
      val customHeaders = new RecordHeaders()
      customHeaders.add("test-header", "test-value".getBytes())
      val envelope = KafkaKeyAvroMessageEnvelope("service", "key123", testMessage, customHeaders)
      envelope.headers shouldBe customHeaders
      envelope.headers.lastHeader("test-header").value() shouldBe "test-value".getBytes()
    }

    "throw descriptive exception on serialization failure" in {
      // Create a mock SpecificRecord that will fail during datumWriter.write
      val failingRecord = new SpecificRecord {
        override def getSchema: Schema = TestAvroRecord.SCHEMA // Valid schema
        override def get(i: Int): AnyRef = throw new RuntimeException("Write error")
        override def put(i: Int, v: scala.Any): Unit = throw new UnsupportedOperationException()
      }
      
      val envelope = KafkaKeyAvroMessageEnvelope("service", "key123", failingRecord)
      
      val exception = intercept[RuntimeException] {
        envelope.messageBytes
      }
      exception.getMessage should include("Failed to serialize Avro message for schema TestAvroRecord")
      exception.getCause shouldBe a[RuntimeException]
      exception.getCause.getMessage shouldBe "Write error"
    }
  }
}
