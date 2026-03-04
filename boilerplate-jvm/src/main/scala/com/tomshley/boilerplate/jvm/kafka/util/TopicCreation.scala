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
object TopicCreation {

  private val logger: Logger = LoggerFactory.getLogger(getClass)
  final case class TopicSettings(partitions: Int, replicationFactor: Short)

  private val closeTimeout: Duration = Duration.ofSeconds(5)

  private[util] def createTopicsWith(
    clientConfig: Map[String, Object],
    topics: Map[String, TopicSettings],
    clientFactory: Map[String, Object] => Admin
  )(using ExecutionContext): Future[Admin] =
    try {
      createTopicsWithClient(clientFactory(clientConfig), topics)
    } catch {
      case NonFatal(ex) => Future.failed(ex)
    }

  private def createTopicsWithClient(
    client: Admin,
    topics: Map[String, TopicSettings]
  ): Future[Admin] =
    try {
      val newTopics = topics.map(t => {
        new NewTopic(t._1, t._2.partitions, t._2.replicationFactor)
      })

      logger.info(
        s"Starting the topics creation for: ${topics.keys.mkString(", ")}"
      )

      val createTopicsResult: CreateTopicsResult =
        client.createTopics(newTopics.asJavaCollection)

      createTopicsResult.values().asScala.foreach {
        case (topicName, kFuture) =>
          kFuture.whenComplete {
            case (_, throwable: Throwable) if Option(throwable).isDefined =>
              logger.warn(s"Topic creation didn't complete for $topicName:", throwable)

            case _ =>
              newTopics.find(_.name() == topicName).map { topic =>
                logger.info(
                  s"""|Topic ${topic.name}
                  | has been successfully created with ${topic.numPartitions} partitions
                  | and replicated ${topic
                       .replicationFactor() - 1} times""".stripMargin
                    .replaceAll("\n", "")
                )
              }
          }
      }

      val promise = Promise[Admin]()
      createTopicsResult.all().whenComplete { (_, throwable) =>
        if (throwable == null) {
          logger.info("Topic creation stage completed.")
          promise.success(client)
        } else if (throwable.isInstanceOf[TopicExistsException] ||
                   Option(throwable.getCause).exists(_.isInstanceOf[TopicExistsException])) {
          logger.info("Topic creation stage completed. (Topics already created)")
          promise.success(client)
        } else {
          try { client.close(closeTimeout) } catch { case _: Exception => () }
          logger.error("Topic creation failed", throwable)
          promise.failure(throwable)
        }
      }
      promise.future
    } catch {
      case NonFatal(ex) =>
        try { client.close(closeTimeout) } catch { case _: Exception => () }
        Future.failed(ex)
    }

  def createTopics(clientConfig: Map[String, Object],
                   topics: Map[String, TopicSettings])(using ExecutionContext): Future[Admin] = {
    createTopicsWith(
      clientConfig,
      topics,
      clientFactory = (cfg: Map[String, Object]) => Admin.create(cfg.asJava)
    )
  }

}
