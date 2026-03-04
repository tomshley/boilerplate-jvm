package com.tomshley.boilerplate.jvm.kafka.util

import com.sksamuel.avro4s.{Encoder, SchemaFor}
import com.tomshley.boilerplate.jvm.marshalling.AvroMarshaller
import com.tomshley.boilerplate.jvm.marshalling.models.MarshallModel
import org.apache.avro.generic.{GenericDatumWriter, GenericRecord}
import org.apache.kafka.common.header.internals.RecordHeaders
import org.apache.pekko.persistence.query.typed.EventEnvelope as PersistenceQueryEventEnvelope
import org.apache.pekko.projection.eventsourced.EventEnvelope as EventSourcedEventEnvelope
import org.apache.pekko.persistence.typed.PersistenceId

/**
 * Avro message envelope for Kafka production.
 *
 * @param serviceName Carried as metadata for routing/logging; unlike the Proto
 *                    envelope, it is not embedded in the serialized payload.
 * @param avroValue   A GenericRecord (typically built via avro4s RecordFormat).
 * @param headers     Optional Kafka headers (e.g. CloudEvents attributes).
 */
final case class KafkaKeyAvroMessageEnvelope(serviceName: String, key: String, avroValue: GenericRecord, headers: RecordHeaders = new RecordHeaders()) {
  /** Raw Avro binary encoding (no Confluent Schema Registry wire-format header).
   *  Use with ProducerProtoBoilerplate (byte-array producer).
   *  For Confluent wire format, send avroValue directly via ProducerAvroBoilerplate instead.
   */
  lazy val messageBytes: Array[Byte] = {
    try {
      val schema = avroValue.getSchema
      val datumWriter = new GenericDatumWriter[GenericRecord](schema)
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
  def apply(serviceName: String, eventsourcedEnvelope: EventSourcedEventEnvelope[?], avroValue: GenericRecord) = new KafkaKeyAvroMessageEnvelope(
    serviceName,
    PersistenceId.extractEntityId(eventsourcedEnvelope.persistenceId),
    avroValue
  )
  def apply(serviceName: String, persistenceQueryEnvelope: PersistenceQueryEventEnvelope[?], avroValue: GenericRecord) = new KafkaKeyAvroMessageEnvelope(
    serviceName,
    PersistenceId.extractEntityId(persistenceQueryEnvelope.persistenceId),
    avroValue
  )
  def apply[T <: MarshallModel[T]](serviceName: String, key: String, model: T)
      (using Encoder[T], SchemaFor[T]): KafkaKeyAvroMessageEnvelope =
    new KafkaKeyAvroMessageEnvelope(serviceName, key, AvroMarshaller.toRecord(model))
}
