package com.tomshley.boilerplate.jvm.kafka.util

import io.confluent.kafka.schemaregistry.avro.AvroSchema
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient
import org.slf4j.{Logger, LoggerFactory}

import scala.annotation.tailrec
import scala.concurrent.duration.{Duration, DurationInt}
import scala.jdk.CollectionConverters.*
import scala.util.{Failure, Success, Try}

object SchemaPublication:

  final case class TopicSchemaSettings(
      topicSchemas: Map[String, AvroSchema],
      schemaRegistry: SchemaRegistryConfig,
      identityMapCapacity: Int,
      retriesNum: Int,
      retriesInterval: Duration
  )

  object TopicSchemaSettings:
    def apply(
        topicSchemas: Map[String, AvroSchema],
        schemaRegistry: Option[SchemaRegistryConfig] = None,
        identityMapCapacity: Option[Int] = None,
        retriesNum: Option[Int] = None,
        retriesInterval: Option[Duration] = None
    ): TopicSchemaSettings =
      new TopicSchemaSettings(
        topicSchemas,
        schemaRegistry.getOrElse(SchemaRegistryConfig("http://localhost:8081")),
        identityMapCapacity.getOrElse(200),
        retriesNum.getOrElse(5),
        retriesInterval.getOrElse(500.milliseconds)
      )

  private val logger: Logger = LoggerFactory.getLogger(getClass)

  def publishWithRetry(settings: TopicSchemaSettings): Unit =
    val client = new CachedSchemaRegistryClient(
      settings.schemaRegistry.url,
      settings.identityMapCapacity,
      settings.schemaRegistry.toConfluentConfig.asJava
    )
    settings.topicSchemas.foreach { (topic, schema) =>
      retryRegister(settings.retriesNum, settings.retriesInterval) {
        client.register(topic, schema)
      } match
        case Failure(e) =>
          logger.error(s"Failed to register schema for topic $topic at ${settings.schemaRegistry.url}", e)
        case Success(_) =>
          logger.info(s"Published schema for topic $topic at ${settings.schemaRegistry.url}")
    }

  @tailrec
  private def retryRegister(countdown: Int, interval: Duration)(op: => Unit): Try[Unit] =
    Try(op) match
      case result @ Success(_) => result
      case result @ Failure(_) if countdown <= 0 => result
      case Failure(_) =>
        logger.warn(s"Schema registration failed, retrying in ${interval.toSeconds}s")
        Thread.sleep(interval.toMillis)
        retryRegister(countdown - 1, interval)(op)
