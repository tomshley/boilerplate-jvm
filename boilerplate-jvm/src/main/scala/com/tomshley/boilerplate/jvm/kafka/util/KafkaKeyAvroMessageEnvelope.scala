package com.tomshley.boilerplate.jvm.kafka.util

import org.apache.avro.specific.SpecificRecord
import org.apache.kafka.common.header.internals.RecordHeaders
import org.apache.pekko.persistence.query.typed.EventEnvelope as PersistenceQueryEventEnvelope
import org.apache.pekko.projection.eventsourced.EventEnvelope as EventSourcedEventEnvelope
import org.apache.pekko.persistence.typed.PersistenceId

final case class KafkaKeyAvroMessageEnvelope(serviceName: String, key: String, avroValue: SpecificRecord, headers: RecordHeaders = new RecordHeaders()) {
  lazy val messageBytes: Array[Byte] = {
    try {
      val schema = avroValue.getSchema
      val datumWriter = new org.apache.avro.specific.SpecificDatumWriter[SpecificRecord](schema)
      val out = new java.io.ByteArrayOutputStream()
      val encoder = org.apache.avro.io.EncoderFactory.get().binaryEncoder(out, null)
      datumWriter.write(avroValue, encoder)
      encoder.flush()
      out.toByteArray
    } catch {
      case e: Exception =>
        throw new RuntimeException(s"Failed to serialize Avro message for schema ${avroValue.getSchema.getFullName}", e)
    }
  }
}

object KafkaKeyAvroMessageEnvelope {
  def apply(serviceName: String, eventsourcedEnvelope: EventSourcedEventEnvelope[?], avroValue: SpecificRecord) = new KafkaKeyAvroMessageEnvelope(
    serviceName,
    PersistenceId.extractEntityId(eventsourcedEnvelope.persistenceId),
    avroValue
  )
  def apply(serviceName: String, persistenceQueryEnvelope: PersistenceQueryEventEnvelope[?], avroValue: SpecificRecord) = new KafkaKeyAvroMessageEnvelope(
    serviceName,
    PersistenceId.extractEntityId(persistenceQueryEnvelope.persistenceId),
    avroValue
  )
}
