package com.tomshley.boilerplate.jvm.marshalling

import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.jdk.CollectionConverters.*

/** Contract: [[AvroCodec]] is the derive-once equivalent of the call-scoped
 *  [[AvroMarshaller]] API — identical semantics (including writer-schema
 *  resolution and evolution behavior), with derivation paid at construction
 *  instead of per call. Every behavioral test here asserts equivalence
 *  against the [[AvroMarshaller]] reference path on the same inputs.
 *
 *  Fixtures (TestAvroEvent, TestAvroEventWithStatus, RenamedFieldEvent,
 *  NestedRenameEvent, EnumDefaultEvent) are the top-level marshalling test
 *  models declared in AvroMarshallerSpec.scala (same package — avro4s macro
 *  derivation requires top-level case classes).
 */
final class AvroCodecSpec extends AnyWordSpec with Matchers {

  private def stringField(name: String): Schema.Field =
    new Schema.Field(name, Schema.create(Schema.Type.STRING), null, null)

  private def recordLike(template: Schema, fields: Schema.Field*): Schema =
    Schema.createRecord(
      template.getName, template.getDoc, template.getNamespace, false, fields.toList.asJava,
    )

  // Old writer schema for RenamedFieldEvent: `accountId` was named `userId`,
  // and `note` didn't exist yet (both required, no defaults).
  private def legacyWriterSchema(readerSchema: Schema): Schema =
    recordLike(readerSchema, stringField("userId"), stringField("region"))

  "AvroCodec" should {

    "expose the same reader schema as AvroMarshaller" in {
      AvroCodec[TestAvroEvent].schema shouldBe AvroMarshaller.schema[TestAvroEvent]
      AvroCodec[RenamedFieldEvent].schema shouldBe AvroMarshaller.schema[RenamedFieldEvent]
    }

    "toRecord matches the AvroMarshaller reference encoding" in {
      val codec = AvroCodec[TestAvroEvent]
      val event = TestAvroEvent("evt-1", 1000L, 42)
      codec.toRecord(event) shouldBe AvroMarshaller.toRecord(event)
    }

    "round-trip toRecord and fromRecord" in {
      val codec = AvroCodec[TestAvroEvent]
      val event = TestAvroEvent("evt-2", 2000L, 7)
      codec.fromRecord(codec.toRecord(event)) shouldBe event
    }

    "round-trip a sealed-trait enum field" in {
      val codec = AvroCodec[TestAvroEventWithStatus]
      codec.fromRecord(codec.toRecord(TestAvroEventWithStatus("a", 1L, ACTIVE))).status shouldBe ACTIVE
      codec.fromRecord(codec.toRecord(TestAvroEventWithStatus("b", 2L, INACTIVE))).status shouldBe INACTIVE
    }

    "reuse one derivation across many records without drift" in {
      val codec = AvroCodec[TestAvroEvent]
      val events = (1 to 100).map(i => TestAvroEvent(s"evt-$i", i.toLong * 100, i))
      events.foreach { event =>
        codec.fromRecord(codec.toRecord(event)) shouldBe event
      }
    }

    "decode a record carrying a value-equal but not reference-equal schema" in {
      // The registry-framed read side hands back records under a schema
      // instance PARSED from the registry — equal by value, never `eq` to the
      // codec's own reader schema. avro4s must fall back to name-based field
      // lookup; the cached derivation must not assume instance identity.
      val codec = AvroCodec[TestAvroEvent]
      val foreignSchema = new Schema.Parser().parse(codec.schema.toString)
      foreignSchema should not be theSameInstanceAs(codec.schema)

      val rec = new GenericData.Record(foreignSchema)
      rec.put("id", "evt-foreign")
      rec.put("ts", 3000L)
      rec.put("count", 9)

      codec.fromRecord(rec) shouldBe TestAvroEvent("evt-foreign", 3000L, 9)
      codec.fromRecordResolving(rec) shouldBe TestAvroEvent("evt-foreign", 3000L, 9)
    }

    "fromRecordResolving decodes a legacy (renamed) writer-schema record" in {
      val codec = AvroCodec[RenamedFieldEvent]
      val legacy = new GenericData.Record(legacyWriterSchema(codec.schema))
      legacy.put("userId", "acct-2")
      legacy.put("region", "us-2")

      val expected = RenamedFieldEvent("acct-2", "us-2", "n/a")
      codec.fromRecordResolving(legacy) shouldBe expected
      // Equivalence with the call-scoped reference path on the same record.
      AvroMarshaller.fromRecordResolving[RenamedFieldEvent](legacy) shouldBe expected
    }

    "fromRecordResolving fills reader defaults on additive evolution" in {
      val codec = AvroCodec[RenamedFieldEvent]
      val additive = recordLike(codec.schema, stringField("accountId"), stringField("region"))
      val rec = new GenericData.Record(additive)
      rec.put("accountId", "acct-3")
      rec.put("region", "us-3")

      codec.fromRecordResolving(rec) shouldBe RenamedFieldEvent("acct-3", "us-3", "n/a")
    }

    "fromRecordResolving resolves an aliased rename inside a nested record" in {
      val codec = AvroCodec[NestedRenameEvent]
      val legacyInner = recordLike(codec.schema.getField("payload").schema, stringField("userId"))
      val legacyOuter = recordLike(
        codec.schema,
        stringField("region"),
        new Schema.Field("payload", legacyInner, null, null),
      )

      val outer = new GenericData.Record(legacyOuter)
      outer.put("region", "us-9")
      val inner = new GenericData.Record(legacyInner)
      inner.put("userId", "acct-9")
      outer.put("payload", inner)

      codec.fromRecordResolving(outer) shouldBe NestedRenameEvent("us-9", InnerPayload("acct-9"))
    }

    "fromRecordResolving fills an omitted enum field through the repaired default" in {
      val codec = AvroCodec[EnumDefaultEvent]
      val legacy = new GenericData.Record(recordLike(codec.schema, stringField("userId")))
      legacy.put("userId", "acct-enum")

      codec.fromRecordResolving(legacy) shouldBe EnumDefaultEvent("acct-enum", Severity.UNSPECIFIED)
    }

    "fromRecordResolving is a plain decode for a current-version record" in {
      val codec = AvroCodec[RenamedFieldEvent]
      val current = codec.toRecord(RenamedFieldEvent("acct-4", "us-4", "hello"))
      codec.fromRecordResolving(current) shouldBe RenamedFieldEvent("acct-4", "us-4", "hello")
    }

    "fromRecordResolving propagates Avro's error on a genuinely incompatible record" in {
      val codec = AvroCodec[RenamedFieldEvent]
      val incompatible = recordLike(
        codec.schema,
        new Schema.Field("userId", Schema.create(Schema.Type.INT), null, null),
        stringField("region"),
      )
      val rec = new GenericData.Record(incompatible)
      rec.put("userId", 42)
      rec.put("region", "us-bad")

      an[org.apache.avro.AvroTypeException] should be thrownBy codec.fromRecordResolving(rec)
    }

    "be safe to share across threads (Kafka Streams StreamThreads)" in {
      given ExecutionContext = ExecutionContext.global
      val codec = AvroCodec[RenamedFieldEvent]
      val legacySchema = legacyWriterSchema(codec.schema)

      val workers = Future.traverse((1 to 8).toList) { worker =>
        Future {
          (1 to 250).map { i =>
            val event = RenamedFieldEvent(s"acct-$worker-$i", s"us-$worker", s"note-$i")
            val roundTripped = codec.fromRecord(codec.toRecord(event))

            val legacy = new GenericData.Record(legacySchema)
            legacy.put("userId", s"legacy-$worker-$i")
            legacy.put("region", s"us-$worker")
            val resolved = codec.fromRecordResolving(legacy)

            roundTripped == event &&
            resolved == RenamedFieldEvent(s"legacy-$worker-$i", s"us-$worker", "n/a")
          }.forall(identity)
        }
      }

      Await.result(workers, 30.seconds) shouldBe List.fill(8)(true)
    }
  }
}
