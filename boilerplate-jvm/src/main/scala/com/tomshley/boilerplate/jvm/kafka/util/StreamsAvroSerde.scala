package com.tomshley.boilerplate.jvm.kafka.util

import com.sksamuel.avro4s.{Decoder, Encoder, SchemaFor}
import com.tomshley.boilerplate.jvm.marshalling.AvroMarshaller
import com.tomshley.boilerplate.jvm.marshalling.models.MarshallModel
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.common.serialization.{Deserializer, Serde, Serdes, Serializer}

import java.util

/** Kafka Streams serdes for the avro4s [[MarshallModel]] value layer.
 *
 * [[StreamsAvroSerde.string]] is the shared `String` key serde used by the
 * canonical `String`-keyed Avro value streams; the [[StreamsAvroSerde]] class
 * is the matching value serde.
 */
object StreamsAvroSerde:
  /** Shared `String` key serde for the canonical `String`-keyed value streams. */
  val string: Serde[String] = Serdes.String()

/** Kafka Streams [[Serde]] for Schema Registry-framed Avro VALUES modeled as
 * avro4s [[MarshallModel]] case classes.
 *
 * Bridges the avro4s case class ↔ `GenericRecord` layer ([[AvroMarshaller]])
 * to Confluent's wire format ([[SchemaRegistrySerde]]): on serialize a value
 * becomes a `GenericRecord` then Confluent bytes; on deserialize the bytes
 * become a `GenericRecord` that [[AvroMarshaller.fromRecordResolving]] conforms
 * onto the reader schema — so a renamed or otherwise evolved writer schema
 * still decodes — before avro4s materialises the case class. `null` values
 * round-trip as `null` (Kafka tombstones).
 *
 * VALUE-ONLY: the underlying Confluent serdes are configured with
 * `isKey = false`, so the subject-name strategy resolves the value subject and
 * the `isKey` flag passed to [[configure]] is intentionally ignored. Use
 * [[StreamsAvroSerde.string]] (or another key serde) for keys.
 *
 * [[serializer]] / [[deserializer]] return cached instances: Kafka Streams and
 * Processor-API nodes call them on the per-record hot path, so they must not
 * allocate. Resource ownership lives in [[close]], which closes the underlying
 * Confluent serdes (and their shared Schema Registry client).
 */
final class StreamsAvroSerde[T <: MarshallModel[T]](schemaRegistryConfig: SchemaRegistryConfig)
    (using Encoder[T], Decoder[T], SchemaFor[T])
    extends Serde[T]:

  private val serializerDelegate: Serializer[GenericRecord] =
    SchemaRegistrySerde.serializer(schemaRegistryConfig)

  private val deserializerDelegate: Deserializer[GenericRecord] =
    SchemaRegistrySerde.deserializer(schemaRegistryConfig)

  private val serializerInstance: Serializer[T] =
    new Serializer[T]:
      override def serialize(topic: String, data: T): Array[Byte] =
        Option(data)
          .map(AvroMarshaller.toRecord[T])
          .map(record => serializerDelegate.serialize(topic, record))
          .orNull

      override def close(): Unit =
        serializerDelegate.close()

  private val deserializerInstance: Deserializer[T] =
    new Deserializer[T]:
      override def deserialize(topic: String, data: Array[Byte]): T =
        Option(data)
          .map(bytes => deserializerDelegate.deserialize(topic, bytes))
          .map(record => AvroMarshaller.fromRecordResolving[T](record))
          .orNull.asInstanceOf[T]

      override def close(): Unit =
        deserializerDelegate.close()

  override def serializer(): Serializer[T] = serializerInstance

  override def deserializer(): Deserializer[T] = deserializerInstance

  override def configure(configs: util.Map[String, ?], isKey: Boolean): Unit = ()

  override def close(): Unit =
    serializerDelegate.close()
    deserializerDelegate.close()
