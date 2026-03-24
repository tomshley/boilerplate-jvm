package com.tomshley.boilerplate.jvm.marshalling

import org.json4s.*
import org.json4s.jackson.Serialization.{read, write}

import models.MarshallModel

import scala.concurrent.{ExecutionContext, Future}
import scala.reflect.Manifest

trait JsonMarshaller {

  val marshallerFormats: Formats = DefaultFormats
  given formats: Formats = marshallerFormats
  /** serializeWithDefaults
   *
   * @param model T
   * @tparam T T <: MarshallModel[ T ]
   * @return String
   */
  final def serializeWithDefaults[T <: MarshallModel[T] : Manifest](model: T) : String = {
    write[T](model)
  }

  /** deserializeWithDefaults
   *
   * @param json String
   * @tparam T T <: MarshallModel[ T ]
   * @return T
   */
  final def deserializeWithDefaults[T <: MarshallModel[T] : Manifest](json: String): T = {
    read[T](json)
  }

  /** serializeWithDefaultsAsync
   *
   * @param model T
   * @tparam T T <: MarshallModel[ T ]
   * @return String
   */
  final def serializeWithDefaultsAsync[T <: MarshallModel[T] : Manifest](model: T, ec: ExecutionContext) : Future[String] = {
    given exec: ExecutionContext = ec
    Future { write[T](model) }
  }

  /** deserializeWithDefaultsAsync
   *
   * @param json String
   * @tparam T T <: MarshallModel[ T ]
   * @return T
   */
  final def deserializeWithDefaultsAsync[T <: MarshallModel[T] : Manifest](json: String, ec: ExecutionContext): Future[T] = {
    given exec: ExecutionContext = ec
    Future { read[T](json) }
  }

  final def serializeCustomMap(model: Map[String, Any]): String = {
    write(model)
  }

  final def deserializeCustomMap(json: String): Map[String, Any] = {
    read(json)
  }

}
object JsonMarshaller extends JsonMarshaller
