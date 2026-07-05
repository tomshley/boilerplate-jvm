package com.tomshley.boilerplate.jvm.kafka.util

import com.sksamuel.avro4s.{AvroAlias, AvroName, AvroNamespace}
import com.tomshley.boilerplate.jvm.marshalling.models.MarshallModel
import io.confluent.kafka.schemaregistry.testutil.MockSchemaRegistry
import io.confluent.kafka.serializers.{AbstractKafkaSchemaSerDeConfig, KafkaAvroSerializer}
import org.apache.avro.SchemaBuilder
import org.apache.avro.generic.GenericData
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import java.util.UUID

@AvroName("SerdeEvent")
@AvroNamespace("serde.spec")
final case class SerdeEvent(
    @AvroAlias("subject_id") correlation_id: String,
    payload: String = "",
)
    extends MarshallModel[SerdeEvent]

final class StreamsAvroSerdeSpec extends AnyWordSpec with Matchers:

  "StreamsAvroSerde" should {
    "round-trip a MarshallModel via mock schema registry" in {
      val scope = s"streams-avro-serde-${UUID.randomUUID()}"
      val cfg = SchemaRegistryConfig(
        url = s"mock://$scope",
        autoRegisterSchemas = true,
        useLatestVersion = false,
      )
      val serde = StreamsAvroSerde[SerdeEvent](cfg)
      try
        val original = SerdeEvent("c-1", "payload")
        val bytes = serde.serializer().serialize("serde-events", original)
        serde.deserializer().deserialize("serde-events", bytes) shouldBe original
      finally
        serde.close()
        MockSchemaRegistry.dropScope(scope)
    }

    "return cached serializer and deserializer instances (per-record hot path must not allocate)" in {
      val scope = s"streams-avro-serde-${UUID.randomUUID()}"
      val cfg = SchemaRegistryConfig(
        url = s"mock://$scope",
        autoRegisterSchemas = true,
        useLatestVersion = false,
      )
      val serde = StreamsAvroSerde[SerdeEvent](cfg)
      try
        serde.serializer() should be theSameInstanceAs serde.serializer()
        serde.deserializer() should be theSameInstanceAs serde.deserializer()
      finally
        serde.close()
        MockSchemaRegistry.dropScope(scope)
    }

    "round-trip null as null (Kafka tombstones)" in {
      val scope = s"streams-avro-serde-${UUID.randomUUID()}"
      val cfg = SchemaRegistryConfig(
        url = s"mock://$scope",
        autoRegisterSchemas = true,
        useLatestVersion = false,
      )
      val serde = StreamsAvroSerde[SerdeEvent](cfg)
      try
        serde.serializer().serialize("serde-events", null.asInstanceOf[SerdeEvent]) shouldBe null
        serde.deserializer().deserialize("serde-events", null) shouldBe null
      finally
        serde.close()
        MockSchemaRegistry.dropScope(scope)
    }

    "reuse the construction-time derivation across a stream of records" in {
      val scope = s"streams-avro-serde-${UUID.randomUUID()}"
      val cfg = SchemaRegistryConfig(
        url = s"mock://$scope",
        autoRegisterSchemas = true,
        useLatestVersion = false,
      )
      val serde = StreamsAvroSerde[SerdeEvent](cfg)
      try
        (1 to 50).foreach { i =>
          val event = SerdeEvent(s"c-$i", s"payload-$i")
          val bytes = serde.serializer().serialize("serde-events", event)
          serde.deserializer().deserialize("serde-events", bytes) shouldBe event
        }
      finally
        serde.close()
        MockSchemaRegistry.dropScope(scope)
    }

    "resolve a legacy writer schema onto the reader contract" in {
      val scope = s"streams-avro-serde-${UUID.randomUUID()}"
      val cfg = SchemaRegistryConfig(
        url = s"mock://$scope",
        autoRegisterSchemas = true,
        useLatestVersion = false,
      )
      val topic = "serde-events"
      val legacyWriterSchema =
        SchemaBuilder.record("SerdeEvent").namespace("serde.spec")
          .fields()
          .requiredString("subject_id")
          .endRecord()
      val legacyRecord = new GenericData.Record(legacyWriterSchema)
      legacyRecord.put("subject_id", "c-legacy")

      val writerSerializer = new KafkaAvroSerializer()
      writerSerializer.configure(
        java.util.Map.of(
          AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, s"mock://$scope",
          AbstractKafkaSchemaSerDeConfig.AUTO_REGISTER_SCHEMAS, "true",
        ),
        false,
      )
      val serde = StreamsAvroSerde[SerdeEvent](cfg)
      try
        val wireBytes = writerSerializer.serialize(topic, legacyRecord)
        serde.deserializer().deserialize(topic, wireBytes) shouldBe SerdeEvent("c-legacy")
      finally
        writerSerializer.close()
        serde.close()
        MockSchemaRegistry.dropScope(scope)
    }
  }
