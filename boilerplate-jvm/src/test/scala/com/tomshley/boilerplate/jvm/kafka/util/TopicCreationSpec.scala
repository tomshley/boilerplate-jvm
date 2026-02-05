package com.tomshley.boilerplate.jvm.kafka.util

import org.apache.kafka.clients.admin.{Admin, CreateTopicsResult, NewTopic}
import org.apache.kafka.common.KafkaFuture
import org.apache.kafka.common.errors.TopicExistsException
import org.apache.kafka.common.internals.KafkaFutureImpl
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import java.util.concurrent.TimeUnit
import java.lang.reflect.{InvocationHandler, Method, Proxy}
import java.util.Collections
import scala.jdk.CollectionConverters.*

final class TopicCreationSpec extends AnyWordSpec with Matchers {

  private final case class ExitCalled(code: Int) extends RuntimeException

  "TopicCreation.createTopicsWith" should {
    "not call exit when topics already exist" in {
      val allFuture = new KafkaFutureImpl[Void]()
      allFuture.completeExceptionally(
        new TopicExistsException("exists")
      )

      val emptyValues =
        Collections
          .emptyMap[String, KafkaFuture[CreateTopicsResult.TopicMetadataAndConfig]]()

      val createTopicsResultWithAll = new CreateTopicsResult(
        emptyValues
      ) {
        override def all(): KafkaFuture[Void] = allFuture
      }

      val handler: InvocationHandler = new InvocationHandler {
        override def invoke(proxy: Object, method: Method, args: Array[Object] | Null): Object = {
          method.getName match {
            case "createTopics" =>
              createTopicsResultWithAll
            case "close" =>
              java.lang.Boolean.TRUE
            case "toString" =>
              "AdminStub"
            case _ =>
              throw new UnsupportedOperationException(method.getName)
          }
        }
      }

      val adminStub: Admin = Proxy
        .newProxyInstance(
          classOf[Admin].getClassLoader,
          Array(classOf[Admin]),
          handler
        )
        .asInstanceOf[Admin]

      noException should be thrownBy {
        TopicCreation.createTopicsWith(
          clientConfig = Map.empty,
          topics = Map("t" -> TopicCreation.TopicSettings(1, 1.toShort)),
          topicCreationTimeout = Some((1L, TimeUnit.SECONDS)),
          clientFactory = _ => adminStub,
          exitFn = code => throw ExitCalled(code)
        )
      }
    }
  }
}
