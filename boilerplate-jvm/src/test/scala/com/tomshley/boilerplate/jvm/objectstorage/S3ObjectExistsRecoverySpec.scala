package com.tomshley.boilerplate.jvm.objectstorage

import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import software.amazon.awssdk.services.s3.model.S3Exception

import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.control.NonFatal

/**
 * Tests the exact recoverWith pattern used in S3BlobStoreBoilerplate.objectExists
 * to verify the approved exception-handling behavior:
 *   - 404 S3Exception → Future.successful(false)
 *   - non-404 S3Exception → Future.failed(original exception)
 *   - other NonFatal → Future.failed(original exception)
 */
final class S3ObjectExistsRecoverySpec extends AnyWordSpec with Matchers with ScalaFutures {

  private def applyRecovery(failing: Future[Boolean]): Future[Boolean] =
    failing.recoverWith {
      case ex: S3Exception if ex.statusCode() == 404 => Future.successful(false)
      case NonFatal(ex) => Future.failed(ex)
    }

  "S3BlobStoreBoilerplate.objectExists recovery" should {
    "return false for a 404 S3Exception" in {
      val notFound = S3Exception.builder()
        .statusCode(404)
        .message("Not Found")
        .build()

      applyRecovery(Future.failed(notFound)).futureValue shouldBe false
    }

    "propagate the original exception for a 500 S3Exception" in {
      val serverError = S3Exception.builder()
        .statusCode(500)
        .message("Internal Server Error")
        .build()

      val ex = applyRecovery(Future.failed(serverError)).failed.futureValue
      ex shouldBe serverError
      ex shouldBe a[S3Exception]
    }

    "propagate the original exception for a 403 S3Exception" in {
      val forbidden = S3Exception.builder()
        .statusCode(403)
        .message("Forbidden")
        .build()

      val ex = applyRecovery(Future.failed(forbidden)).failed.futureValue
      ex shouldBe forbidden
    }

    "propagate the original exception for a non-S3 exception" in {
      val original = new IllegalStateException("connection reset")

      val ex = applyRecovery(Future.failed(original)).failed.futureValue
      ex shouldBe original
      ex shouldBe a[IllegalStateException]
    }
  }
}
