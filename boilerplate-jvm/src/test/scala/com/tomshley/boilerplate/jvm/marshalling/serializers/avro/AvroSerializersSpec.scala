package com.tomshley.boilerplate.jvm.marshalling.serializers.avro

import com.tomshley.boilerplate.jvm.marshalling.AvroMarshaller
import com.tomshley.boilerplate.jvm.marshalling.models.MarshallModel
import com.tomshley.boilerplate.jvm.utils.TimeUtils
import org.apache.avro.Schema
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import java.io.File
import java.nio.file.{Path, Paths}
import java.time.ZonedDateTime

// ── Test fixtures — MUST be top-level for avro4s macro derivation ──

final case class DateTimeEvent(id: String, ts: TimeUtils.DateTime)
    extends MarshallModel[DateTimeEvent]

final case class ZonedDateTimeEvent(id: String, ts: ZonedDateTime)
    extends MarshallModel[ZonedDateTimeEvent]

final case class FileEvent(id: String, file: File)
    extends MarshallModel[FileEvent]

final case class PathEvent(id: String, path: Path)
    extends MarshallModel[PathEvent]

// ── Tests ──

final class AvroSerializersSpec extends AnyWordSpec with Matchers {

  "Avro serializers" should {

    "round-trip DateTime (Joda)" in {
      val dt = TimeUtils.DateTime.now()
      val event = DateTimeEvent("dt-1", dt)
      val record = AvroMarshaller.toRecord(event)
      val back = AvroMarshaller.fromRecord[DateTimeEvent](record)
      back.id shouldBe "dt-1"
      back.ts.getMillis shouldBe dt.getMillis
    }

    "DateTime schema is STRING" in {
      val s = AvroMarshaller.schema[DateTimeEvent]
      s.getField("ts").schema().getType shouldBe Schema.Type.STRING
    }

    "round-trip ZonedDateTime" in {
      val zdt = ZonedDateTime.parse("2025-06-15T10:30:00+02:00[Europe/Berlin]")
      val event = ZonedDateTimeEvent("zdt-1", zdt)
      val record = AvroMarshaller.toRecord(event)
      val back = AvroMarshaller.fromRecord[ZonedDateTimeEvent](record)
      back.ts shouldBe zdt
    }

    "ZonedDateTime schema is STRING" in {
      val s = AvroMarshaller.schema[ZonedDateTimeEvent]
      s.getField("ts").schema().getType shouldBe Schema.Type.STRING
    }

    "round-trip File" in {
      val f = new File("/tmp/test-file.txt")
      val event = FileEvent("f-1", f)
      val record = AvroMarshaller.toRecord(event)
      val back = AvroMarshaller.fromRecord[FileEvent](record)
      back.file.getAbsolutePath shouldBe f.getAbsolutePath
    }

    "File schema is STRING" in {
      val s = AvroMarshaller.schema[FileEvent]
      s.getField("file").schema().getType shouldBe Schema.Type.STRING
    }

    "round-trip Path" in {
      val p = Paths.get("/tmp/test-path")
      val event = PathEvent("p-1", p)
      val record = AvroMarshaller.toRecord(event)
      val back = AvroMarshaller.fromRecord[PathEvent](record)
      back.path.toAbsolutePath shouldBe p.toAbsolutePath
    }

    "Path schema is STRING" in {
      val s = AvroMarshaller.schema[PathEvent]
      s.getField("path").schema().getType shouldBe Schema.Type.STRING
    }
  }
}
