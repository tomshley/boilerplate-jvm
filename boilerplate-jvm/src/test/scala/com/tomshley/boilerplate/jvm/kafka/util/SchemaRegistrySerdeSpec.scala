package com.tomshley.boilerplate.jvm.kafka.util

import io.confluent.kafka.schemaregistry.avro.AvroSchema
import io.confluent.kafka.schemaregistry.testutil.MockSchemaRegistry
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.kafka.common.errors.SerializationException
import org.scalatest.BeforeAndAfterEach
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

/** End-to-end wiring spec for [[SchemaRegistrySerde]].
 *
 * Uses Confluent's `mock://scope` URL scheme so that the
 * [[io.confluent.kafka.serializers.KafkaAvroSerializer]] produced by
 * [[SchemaRegistrySerde.serializer]] resolves to a shared
 * `MockSchemaRegistryClient` for the scope. This lets us assert that the
 * policy flags from [[SchemaRegistryConfig]] actually reach the serializer
 * and shape its runtime behavior, not just its configuration map.
 *
 * These tests are the regression guard for the schemas-as-code contract:
 * with `autoRegisterSchemas = false`, an attempt to publish a schema that
 * the registry has not been seeded with must fail, preventing avro4s
 * version upgrades from silently minting drift versions.
 */
final class SchemaRegistrySerdeSpec extends AnyWordSpec with Matchers with BeforeAndAfterEach {

  private val topic = "events"
  private val subject = s"$topic-value"

  private val recordSchema: Schema = new Schema.Parser().parse(
    """
      |{
      |  "type": "record",
      |  "name": "Event",
      |  "namespace": "serde.spec",
      |  "fields": [
      |    {"name": "id", "type": "string"},
      |    {"name": "payload", "type": "string"}
      |  ]
      |}
      |""".stripMargin
  )

  private def newRecord(id: String, payload: String): GenericRecord =
    val r = new GenericData.Record(recordSchema)
    r.put("id", id)
    r.put("payload", payload)
    r

  // Fresh scope per test to prevent cross-test state leakage in the shared
  // MockSchemaRegistry singleton map.
  private var scope: String = scala.compiletime.uninitialized
  private def mockUrl: String = s"mock://$scope"

  override def beforeEach(): Unit =
    scope = s"serde-spec-${java.util.UUID.randomUUID()}"

  override def afterEach(): Unit =
    MockSchemaRegistry.dropScope(scope)

  "SchemaRegistrySerde.serializer with schemas-as-code defaults" should {

    "refuse to serialize an unregistered schema" in {
      val serializer = SchemaRegistrySerde.serializer(SchemaRegistryConfig(url = mockUrl))
      try
        // No pre-registration — mock registry is empty for this scope.
        // KafkaAvroSerializer throws SerializationException wrapping a
        // RestClientException when auto.register.schemas=false AND the
        // subject has no registered schema yet.
        intercept[SerializationException](serializer.serialize(topic, newRecord("e1", "first")))
      finally serializer.close()
    }

    "successfully serialize against a pre-registered schema and emit Confluent wire format" in {
      val client = MockSchemaRegistry.getClientForScope(scope)
      val registeredId = client.register(subject, new AvroSchema(recordSchema))

      val serializer = SchemaRegistrySerde.serializer(SchemaRegistryConfig(url = mockUrl))
      try
        val bytes = serializer.serialize(topic, newRecord("e2", "second"))

        // Wire format: [0x00][4-byte big-endian schema ID][avro-binary-payload]
        bytes(0) shouldBe 0x00.toByte
        val wireId = ((bytes(1) & 0xff) << 24) |
          ((bytes(2) & 0xff) << 16) |
          ((bytes(3) & 0xff) << 8) |
          (bytes(4) & 0xff)
        wireId shouldBe registeredId
      finally serializer.close()
    }

    "round-trip a pre-registered schema through the paired deserializer" in {
      val client = MockSchemaRegistry.getClientForScope(scope)
      client.register(subject, new AvroSchema(recordSchema))

      val cfg = SchemaRegistryConfig(url = mockUrl)
      val serializer = SchemaRegistrySerde.serializer(cfg)
      val deserializer = SchemaRegistrySerde.deserializer(cfg)
      try
        val original = newRecord("e3", "third")
        val bytes = serializer.serialize(topic, original)
        val decoded = deserializer.deserialize(topic, bytes)

        decoded.get("id").toString shouldBe "e3"
        decoded.get("payload").toString shouldBe "third"
      finally
        serializer.close()
        deserializer.close()
    }
  }

  "SchemaRegistrySerde.serializer with autoRegisterSchemas = true (opt-in, local dev)" should {

    "register and use a new schema when none is pre-registered" in {
      val serializer = SchemaRegistrySerde.serializer(
        SchemaRegistryConfig(url = mockUrl, autoRegisterSchemas = true, useLatestVersion = false)
      )
      try
        val bytes = serializer.serialize(topic, newRecord("e4", "fourth"))
        bytes(0) shouldBe 0x00.toByte

        val client = MockSchemaRegistry.getClientForScope(scope)
        val registered = client.getAllSubjects
        registered should contain(subject)
      finally serializer.close()
    }
  }

  "SchemaRegistrySerde.serializer with RecordNameStrategy" should {

    "register the record full name rather than the topic subject" in {
      val serializer = SchemaRegistrySerde.serializer(
        SchemaRegistryConfig(
          url = mockUrl,
          autoRegisterSchemas = true,
          useLatestVersion = false,
          subjectNameStrategy = Some(SchemaRegistryConfig.SubjectNameStrategy.RecordNameStrategy),
        )
      )
      try
        serializer.serialize(topic, newRecord("e5", "fifth"))(0) shouldBe 0x00.toByte

        val client = MockSchemaRegistry.getClientForScope(scope)
        val registered = client.getAllSubjects
        registered should contain("serde.spec.Event")
        registered should not contain subject
      finally serializer.close()
    }
  }
}
