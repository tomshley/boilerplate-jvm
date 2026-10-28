/*
 * copyright 2017–present tomshley llc
 *
 * licensed under the apache license, version 2.0 (the "license");
 * you may not use this file except in compliance with the license.
 * you may obtain a copy of the license at
 *
 * http://www.apache.org/licenses/license-2.0
 *
 * unless required by applicable law or agreed to in writing, software
 * distributed under the license is distributed on an "as is" basis,
 * without warranties or conditions of any kind, either express or implied.
 * see the license for the specific language governing permissions and
 * limitations under the license.
 *
 * @author thomas schena @sgoggles <https://github.com/sgoggles> | <https://gitlab.com/sgoggles>
 *
 */

package com.tomshley.boilerplate.jvm.kafka.util

import org.apache.kafka.clients.admin.{Admin, CreateTopicsResult}
import org.apache.kafka.common.KafkaFuture
import org.apache.kafka.common.errors.TopicExistsException
import org.apache.kafka.common.internals.KafkaFutureImpl
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import java.lang.reflect.{InvocationHandler, Method, Proxy}
import java.util.Collections
import java.util.concurrent.ExecutionException
import scala.concurrent.ExecutionContext

final class TopicCreationSpec extends AnyWordSpec with Matchers with ScalaFutures {

  given ExecutionContext = ExecutionContext.global

  private def adminStubWith(createTopicsResult: CreateTopicsResult): Admin = {
    val handler: InvocationHandler = new InvocationHandler {
      override def invoke(proxy: Object, method: Method, args: Array[Object] | Null): Object = {
        method.getName match {
          case "createTopics" => createTopicsResult
          case "close"        => java.lang.Boolean.TRUE
          case "toString"     => "AdminStub"
          case _              => throw new UnsupportedOperationException(method.getName)
        }
      }
    }
    Proxy
      .newProxyInstance(
        classOf[Admin].getClassLoader,
        Array(classOf[Admin]),
        handler
      )
      .asInstanceOf[Admin]
  }

  private def resultWith(allFuture: KafkaFuture[Void]): CreateTopicsResult = {
    val emptyValues =
      Collections.emptyMap[String, KafkaFuture[CreateTopicsResult.TopicMetadataAndConfig]]()
    new CreateTopicsResult(emptyValues) {
      override def all(): KafkaFuture[Void] = allFuture
    }
  }

  "TopicCreation.createTopicsWith" should {
    "succeed when topics are created successfully" in {
      val allFuture = new KafkaFutureImpl[Void]()
      allFuture.complete(null)

      val future = TopicCreation.createTopicsWith(
        clientConfig = Map.empty,
        topics = Map("t" -> TopicCreation.TopicSettings(1, 1.toShort)),
        clientFactory = _ => adminStubWith(resultWith(allFuture))
      )

      future.futureValue shouldBe a[Admin]
    }

    "succeed when TopicExistsException is wrapped in ExecutionException" in {
      val allFuture = new KafkaFutureImpl[Void]()
      allFuture.completeExceptionally(
        new ExecutionException(new TopicExistsException("exists"))
      )

      val future = TopicCreation.createTopicsWith(
        clientConfig = Map.empty,
        topics = Map("t" -> TopicCreation.TopicSettings(1, 1.toShort)),
        clientFactory = _ => adminStubWith(resultWith(allFuture))
      )

      future.futureValue shouldBe a[Admin]
    }

    "succeed when TopicExistsException is delivered unwrapped" in {
      val allFuture = new KafkaFutureImpl[Void]()
      allFuture.completeExceptionally(
        new TopicExistsException("exists")
      )

      val future = TopicCreation.createTopicsWith(
        clientConfig = Map.empty,
        topics = Map("t" -> TopicCreation.TopicSettings(1, 1.toShort)),
        clientFactory = _ => adminStubWith(resultWith(allFuture))
      )

      future.futureValue shouldBe a[Admin]
    }

    "fail the future on non-TopicExistsException errors" in {
      val allFuture = new KafkaFutureImpl[Void]()
      allFuture.completeExceptionally(
        new RuntimeException("broker down")
      )

      val future = TopicCreation.createTopicsWith(
        clientConfig = Map.empty,
        topics = Map("t" -> TopicCreation.TopicSettings(1, 1.toShort)),
        clientFactory = _ => adminStubWith(resultWith(allFuture))
      )

      future.failed.futureValue shouldBe a[RuntimeException]
    }

    "capture synchronous clientFactory exception as failed Future" in {
      val future = TopicCreation.createTopicsWith(
        clientConfig = Map.empty,
        topics = Map("t" -> TopicCreation.TopicSettings(1, 1.toShort)),
        clientFactory = _ => throw new RuntimeException("bad config")
      )

      future.failed.futureValue shouldBe a[RuntimeException]
    }
  }
}
