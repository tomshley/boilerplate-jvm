package com.tomshley.boilerplate.jvm.cloudevents

import io.confluent.kafka.serializers.KafkaAvroSerializer
import org.apache.kafka.common.serialization.Serializer

import java.util.Properties
import scala.jdk.CollectionConverters.*

/*
------------------ Message -------------------

Topic Name: mytopic

------------------- key ----------------------

Key: mykey

------------------ headers -------------------

ce_specversion: "1.0"
ce_type: "com.example.someevent"
ce_source: "/mycontext/subcontext"
ce_id: "1234-1234-1234"
//ce_time: "2018-04-05T03:56:24Z"
content-type: application/avro
       .... further attributes ...

------------------- value --------------------

            ... application data encoded in Avro ...

-----------------------------------------------
 */

object CloudEventKafkaAvroSerializer {
  def init(registryURL: String): Serializer[CloudEvent] = {

    val serdeProps = new Properties()
    serdeProps.put("schema.registry.url", registryURL)

    val valueSerializer: Serializer[CloudEvent] = new Serializer[CloudEvent] {
      private val ser = new KafkaAvroSerializer()

      override def configure(configs: java.util.Map[String, ?], isKey: Boolean): Unit =
        ser.configure(configs, isKey)

      override def serialize(topic: String, data: CloudEvent): Array[Byte] =
        if (data == null) Array.emptyByteArray
        else Array.emptyByteArray

      override def close(): Unit = ser.close()
    }

    valueSerializer.configure(serdeProps.asScala.toMap.asJava, false)
    valueSerializer
  }
}
