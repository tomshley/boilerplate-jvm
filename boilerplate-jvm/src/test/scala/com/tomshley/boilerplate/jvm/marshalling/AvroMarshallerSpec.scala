package com.tomshley.boilerplate.jvm.marshalling

import com.sksamuel.avro4s.AvroAlias
import com.tomshley.boilerplate.jvm.kafka.util.{KafkaKeyAvroConsumerEnvelope, KafkaKeyAvroMessageEnvelope}
import com.tomshley.boilerplate.jvm.marshalling.models.MarshallModel
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.ExecutionContext
import scala.jdk.CollectionConverters.*

// ── Test fixtures — MUST be top-level for avro4s macro derivation ──

final case class TestAvroEvent(id: String, ts: Long, count: Int)
    extends MarshallModel[TestAvroEvent]

// Sealed trait + case objects for Avro ENUM (testing bare Magnolia derivation)
sealed trait TestStatus
case object ACTIVE extends TestStatus
case object INACTIVE extends TestStatus

final case class TestAvroEventWithStatus(id: String, ts: Long, status: TestStatus)
    extends MarshallModel[TestAvroEventWithStatus]

// Schema-evolution fixture: `accountId` was renamed from `userId` (carries the
// alias, no Scala default), and `note` was added later (additive, with a Scala
// default).
final case class RenamedFieldEvent(
    @AvroAlias("userId") accountId: String,
    region: String,
    note: String = "n/a",
) extends MarshallModel[RenamedFieldEvent]

// Nested-evolution fixture: the inner record's `accountId` was likewise renamed
// from `userId`, exercising alias resolution below the top level.
final case class InnerPayload(@AvroAlias("userId") accountId: String)
final case class NestedRenameEvent(region: String, payload: InnerPayload)
    extends MarshallModel[NestedRenameEvent]

// Enum-default fixture: a Scala 3 `enum` field with a default. avro4s 5.0.15
// derives an illegal field-level `"default": ""` for `severity`. Paired with an
// aliased rename (`accountId` from `userId`), a legacy writer that omits
// `severity` makes Avro's resolver read that `""` default — previously a
// NullPointerException. `UNSPECIFIED` is listed first, so it is the symbol
// recovered when the out-of-range default is repaired.
enum Severity:
  case UNSPECIFIED, LOW, HIGH

final case class EnumDefaultEvent(
    @AvroAlias("userId") accountId: String,
    severity: Severity = Severity.UNSPECIFIED,
) extends MarshallModel[EnumDefaultEvent]

// ── Tests ──

final class AvroMarshallerSpec extends AnyWordSpec with Matchers with ScalaFutures {

  given ExecutionContext = ExecutionContext.global

  private def stringField(name: String): Schema.Field =
    new Schema.Field(name, Schema.create(Schema.Type.STRING), null, null)

  // Build a record schema borrowing the reader's name/namespace (so Avro matches
  // it by full name) but with an explicit, older set of fields.
  private def recordLike(template: Schema, fields: Schema.Field*): Schema =
    Schema.createRecord(
      template.getName, template.getDoc, template.getNamespace, false, fields.toList.asJava,
    )

  // Old writer schema: `accountId` was named `userId`, and `note` didn't exist
  // yet (both required, no defaults).
  private def legacyWriterSchema(readerSchema: Schema): Schema =
    recordLike(readerSchema, stringField("userId"), stringField("region"))

  // Additive-only writer schema: current field names, but `note` not yet present.
  private def additiveWriterSchema(readerSchema: Schema): Schema =
    recordLike(readerSchema, stringField("accountId"), stringField("region"))

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

    "conformToReaderSchema resolves an aliased rename and fills reader defaults" in {
      val readerSchema = AvroMarshaller.schema[RenamedFieldEvent]
      val legacy = new GenericData.Record(legacyWriterSchema(readerSchema))
      legacy.put("userId", "acct-1")
      legacy.put("region", "us-1")

      val conformed = AvroMarshaller.conformToReaderSchema(legacy, readerSchema)
      conformed.get("accountId").toString shouldBe "acct-1"
      conformed.get("region").toString shouldBe "us-1"
      conformed.get("note").toString shouldBe "n/a"
    }

    "fromRecordResolving decodes a legacy (renamed) writer-schema record" in {
      val readerSchema = AvroMarshaller.schema[RenamedFieldEvent]
      val legacy = new GenericData.Record(legacyWriterSchema(readerSchema))
      legacy.put("userId", "acct-2")
      legacy.put("region", "us-2")

      AvroMarshaller.fromRecordResolving[RenamedFieldEvent](legacy) shouldBe
        RenamedFieldEvent("acct-2", "us-2", "n/a")
    }

    "fromRecordResolvingAsync decodes a legacy (renamed) writer-schema record" in {
      val readerSchema = AvroMarshaller.schema[RenamedFieldEvent]
      val legacy = new GenericData.Record(legacyWriterSchema(readerSchema))
      legacy.put("userId", "acct-async")
      legacy.put("region", "us-async")

      AvroMarshaller
        .fromRecordResolvingAsync[RenamedFieldEvent](legacy, summon[ExecutionContext])
        .futureValue shouldBe RenamedFieldEvent("acct-async", "us-async", "n/a")
    }

    "fromRecordResolving resolves an aliased rename inside a nested record" in {
      val readerSchema = AvroMarshaller.schema[NestedRenameEvent]
      val legacyInner = recordLike(readerSchema.getField("payload").schema, stringField("userId"))
      val legacyOuter = recordLike(
        readerSchema,
        stringField("region"),
        new Schema.Field("payload", legacyInner, null, null),
      )

      val outer = new GenericData.Record(legacyOuter)
      outer.put("region", "us-9")
      val inner = new GenericData.Record(legacyInner)
      inner.put("userId", "acct-9")
      outer.put("payload", inner)

      // Top-level field names are unchanged, so a shallow check would skip
      // resolution and avro4s would throw on the non-null nested `accountId`.
      AvroMarshaller.fromRecordResolving[NestedRenameEvent](outer) shouldBe
        NestedRenameEvent("us-9", InnerPayload("acct-9"))
    }

    "conformToReaderSchema skips the round-trip for additive evolution (no rename)" in {
      val readerSchema = AvroMarshaller.schema[RenamedFieldEvent]
      val rec = new GenericData.Record(additiveWriterSchema(readerSchema))
      rec.put("accountId", "acct-3")
      rec.put("region", "us-3")

      // identity — avro4s fills the missing defaulted field on decode
      AvroMarshaller.conformToReaderSchema(rec, readerSchema) should be theSameInstanceAs rec
      AvroMarshaller.fromRecordResolving[RenamedFieldEvent](rec) shouldBe
        RenamedFieldEvent("acct-3", "us-3", "n/a")
    }

    "conformToReaderSchema is identity for a current-version record" in {
      val readerSchema = AvroMarshaller.schema[RenamedFieldEvent]
      val current = AvroMarshaller.toRecord(RenamedFieldEvent("acct-4", "us-4", "hello"))
      AvroMarshaller.conformToReaderSchema(current, readerSchema) should be theSameInstanceAs current
    }

    "fromRecordResolving propagates Avro's error on a genuinely incompatible aliased record" in {
      val readerSchema = AvroMarshaller.schema[RenamedFieldEvent]
      // Writer carries the aliased field, but typed int where the reader wants
      // string — int is not promotable to string, so resolution must fail loudly.
      val incompatible = recordLike(
        readerSchema,
        new Schema.Field("userId", Schema.create(Schema.Type.INT), null, null),
        stringField("region"),
      )
      val rec = new GenericData.Record(incompatible)
      rec.put("userId", 42)
      rec.put("region", "us-bad")

      an [org.apache.avro.AvroTypeException] should be thrownBy
        AvroMarshaller.fromRecordResolving[RenamedFieldEvent](rec)
    }

    "KafkaKeyAvroConsumerEnvelope.asResolving decodes a legacy (renamed) record" in {
      val readerSchema = AvroMarshaller.schema[RenamedFieldEvent]
      val legacy = new GenericData.Record(legacyWriterSchema(readerSchema))
      legacy.put("userId", "acct-env")
      legacy.put("region", "us-env")

      KafkaKeyAvroConsumerEnvelope("k-1", legacy).asResolving[RenamedFieldEvent] shouldBe
        RenamedFieldEvent("acct-env", "us-env", "n/a")
    }

    "KafkaKeyAvroConsumerEnvelope.asResolvingAsync decodes a legacy (renamed) record" in {
      val readerSchema = AvroMarshaller.schema[RenamedFieldEvent]
      val legacy = new GenericData.Record(legacyWriterSchema(readerSchema))
      legacy.put("userId", "acct-env-async")
      legacy.put("region", "us-env-async")

      KafkaKeyAvroConsumerEnvelope("k-2", legacy)
        .asResolvingAsync[RenamedFieldEvent]
        .futureValue shouldBe RenamedFieldEvent("acct-env-async", "us-env-async", "n/a")
    }

    "fromRecordResolving fills an omitted enum field through an aliased resolve (avro4s \"\" default)" in {
      val readerSchema = AvroMarshaller.schema[EnumDefaultEvent]
      // Legacy writer: `accountId` was still `userId`, and `severity` did not
      // exist yet — exactly the shape that drives Avro's resolver to fill the
      // enum field from its (illegal) reader default.
      val legacy = new GenericData.Record(recordLike(readerSchema, stringField("userId")))
      legacy.put("userId", "acct-enum")

      AvroMarshaller.fromRecordResolving[EnumDefaultEvent](legacy) shouldBe
        EnumDefaultEvent("acct-enum", Severity.UNSPECIFIED)
    }

    "fromRecordResolving preserves a carried enum value through an aliased resolve" in {
      val readerSchema = AvroMarshaller.schema[EnumDefaultEvent]
      val severitySchema = readerSchema.getField("severity").schema
      val legacy = new GenericData.Record(
        recordLike(
          readerSchema,
          stringField("userId"),
          new Schema.Field("severity", severitySchema, null, null),
        )
      )
      legacy.put("userId", "acct-keep")
      legacy.put("severity", new GenericData.EnumSymbol(severitySchema, "HIGH"))

      AvroMarshaller.fromRecordResolving[EnumDefaultEvent](legacy) shouldBe
        EnumDefaultEvent("acct-keep", Severity.HIGH)
    }

    "the avro4s drift this guards against still exists (remove the repair when this fails)" in {
      // Tripwire: when avro4s emits the default as a valid symbol (or omits the
      // field default), this assertion fails — remove withValidEnumDefaults then.
      AvroMarshaller.schema[EnumDefaultEvent].getField("severity").defaultVal() shouldBe ""
    }
  }
}
