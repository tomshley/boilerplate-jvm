package com.tomshley.boilerplate.jvm.kafka.util

import org.apache.kafka.clients.admin.{Admin, CreateTopicsResult, NewTopic}
import org.apache.kafka.common.errors.TopicExistsException
import org.slf4j.{Logger, LoggerFactory}

import java.util.concurrent.{ExecutionException, TimeUnit}
import scala.jdk.CollectionConverters.*
import scala.util.{Failure, Success, Try}

object TopicCreation {

  private val logger: Logger = LoggerFactory.getLogger(getClass)
  private final val defaultTopicCreationTimeout: (Long, TimeUnit) =
    (5, TimeUnit.SECONDS)
  final case class TopicSettings(partitions: Int, replicationFactor: Short)

  private[util] def createTopicsWith(
    clientConfig: Map[String, Object],
    topics: Map[String, TopicSettings],
    topicCreationTimeout: Option[(Long, TimeUnit)],
    clientFactory: Map[String, Object] => Admin,
    exitFn: Int => Nothing
  ): Admin = {
    val client = clientFactory(clientConfig)
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
            logger.warn("Topic creation didn't complete:", throwable)

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

    val (timeOut, timeUnit): (Long, TimeUnit) =
      topicCreationTimeout.getOrElse(defaultTopicCreationTimeout)

    Try(createTopicsResult.all().get(timeOut, timeUnit)) match {

      case Failure(ex) if ex.getCause.isInstanceOf[TopicExistsException] =>
        logger.info("Topic creation stage completed. (Topics already created)")

      case failure @ Failure(_: InterruptedException | _: ExecutionException) =>
        logger.error("The topic creation failed to complete")
        failure.exception.printStackTrace()
        exitFn(2)

      case Failure(exception) =>
        logger.error("The following exception occurred during the topic creation")
        exception.printStackTrace()
        exitFn(3)

      case Success(_) =>
        logger.info("Topic creation stage completed.")
    }

    client
  }

  def createTopics(clientConfig: Map[String, Object],
                   topics: Map[String, TopicSettings],
                   topicCreationTimeout: Option[(Long, TimeUnit)]): Admin = {
    createTopicsWith(
      clientConfig,
      topics,
      topicCreationTimeout,
      clientFactory = (cfg: Map[String, Object]) => Admin.create(cfg.asJava),
      exitFn = (code: Int) => sys.exit(code)
    )
  }

}
