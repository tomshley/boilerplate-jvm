package com.tomshley.boilerplate.jvm.kafka.util

import org.apache.kafka.clients.admin.{Admin, CreateTopicsResult, NewTopic}
import org.apache.kafka.common.errors.TopicExistsException
import org.slf4j.{Logger, LoggerFactory}

import java.time.Duration
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.jdk.CollectionConverters.*
import scala.util.control.NonFatal

/** One-shot async topic bootstrap utility.
 *
 * Creates Kafka topics via the Admin client and returns a Future[Admin].
 * This is a startup/bootstrap operation — it runs once during application
 * initialization, not as part of a reactive stream. The returned Admin
 * client should be closed via CoordinatedShutdown.
 *
 * Consumer stream wiring (e.g. Consumer.committableSource) is handled
 * by the application layer using Pekko Kafka / Alpakka Kafka directly,
 * configured with ConsumerSettings from ConsumerAvroBoilerplate or
 * ConsumerProtoBoilerplate.
 */
object TopicCreation:

  private val logger: Logger = LoggerFactory.getLogger(getClass)
  final case class TopicSettings(partitions: Int, replicationFactor: Short)

  private val closeTimeout: Duration = Duration.ofSeconds(5)

  private[util] def createTopicsWith(
      clientConfig: Map[String, Object],
      topics: Map[String, TopicSettings],
      clientFactory: Map[String, Object] => Admin
  )(using ExecutionContext): Future[Admin] =
    try createTopicsWithClient(clientFactory(clientConfig), topics)
    catch case NonFatal(ex) => Future.failed(ex)

  private def createTopicsWithClient(
      client: Admin,
      topics: Map[String, TopicSettings]
  ): Future[Admin] =
    try
      val newTopics = topics.map { (name, ts) =>
        new NewTopic(name, ts.partitions, ts.replicationFactor)
      }

      logger.info(s"Starting topic creation for: ${topics.keys.mkString(", ")}")

      val createTopicsResult: CreateTopicsResult =
        client.createTopics(newTopics.asJavaCollection)

      createTopicsResult.values().asScala.foreach { (topicName, kFuture) =>
        kFuture.whenComplete { (_, throwable) =>
          if throwable != null then
            logger.warn(s"Topic creation did not complete for $topicName:", throwable)
          else
            newTopics.find(_.name() == topicName).foreach { topic =>
              logger.info(
                s"Topic ${topic.name} created with ${topic.numPartitions} partitions" +
                  s" and ${topic.replicationFactor() - 1} replicas"
              )
            }
        }
      }

      val promise = Promise[Admin]()
      createTopicsResult.all().whenComplete { (_, throwable) =>
        if throwable == null then
          logger.info("Topic creation stage completed.")
          promise.success(client)
        else if throwable.isInstanceOf[TopicExistsException] ||
                Option(throwable.getCause).exists(_.isInstanceOf[TopicExistsException])
        then
          logger.info("Topic creation stage completed (topics already exist).")
          promise.success(client)
        else
          try client.close(closeTimeout) catch case _: Exception => ()
          logger.error("Topic creation failed", throwable)
          promise.failure(throwable)
      }
      promise.future
    catch
      case NonFatal(ex) =>
        try client.close(closeTimeout) catch case _: Exception => ()
        Future.failed(ex)

  def createTopics(
      clientConfig: Map[String, Object],
      topics: Map[String, TopicSettings]
  )(using ExecutionContext): Future[Admin] =
    createTopicsWith(
      clientConfig,
      topics,
      clientFactory = cfg => Admin.create(cfg.asJava)
    )
