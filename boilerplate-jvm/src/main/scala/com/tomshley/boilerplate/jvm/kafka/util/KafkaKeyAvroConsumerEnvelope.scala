package com.tomshley.boilerplate.jvm.kafka.util

import com.sksamuel.avro4s.{Decoder, SchemaFor}
import com.tomshley.boilerplate.jvm.marshalling.AvroMarshaller
import com.tomshley.boilerplate.jvm.marshalling.models.MarshallModel
import org.apache.avro.generic.GenericRecord
import com.tomshley.boilerplate.jvm.kafka.exceptions.KafkaTombstoneException
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.header.internals.RecordHeaders

import scala.concurrent.{ExecutionContext, Future}

/**
 * Avro consumer envelope for Kafka consumption.
 *
 * @param key       The Kafka record key.
 * @param avroValue A GenericRecord deserialized by ConsumerAvroBoilerplate.
 *                  Not defensively copied — the Confluent deserializer
 *                  creates a fresh GenericRecord per record, so mutation
 *                  by the Kafka client is not a concern in practice.
 * @param headers   Kafka headers from the consumed record (defensively copied).
 */
final case class KafkaKeyAvroConsumerEnvelope(key: String, avroValue: GenericRecord, headers: RecordHeaders = new RecordHeaders()):
  /** Typed deserialization via AvroMarshaller — requires MarshallModel. */
  def as[T <: MarshallModel[T]](using Decoder[T], SchemaFor[T]): T =
    AvroMarshaller.fromRecord[T](avroValue)

  /** Async typed deserialization via AvroMarshaller. */
  def asAsync[T <: MarshallModel[T]](using Decoder[T], SchemaFor[T], ExecutionContext): Future[T] =
    AvroMarshaller.fromRecordAsync[T](avroValue, summon[ExecutionContext])

object KafkaKeyAvroConsumerEnvelope:
  def from(record: ConsumerRecord[String, GenericRecord]): KafkaKeyAvroConsumerEnvelope =
    require(record.key() != null, s"Null-keyed records are not supported (partition=${record.partition()}, offset=${record.offset()})")
    if (record.value() == null) throw KafkaTombstoneException(record.key(), record.partition(), record.offset())
    KafkaKeyAvroConsumerEnvelope(record.key(), record.value(), new RecordHeaders(record.headers()))
