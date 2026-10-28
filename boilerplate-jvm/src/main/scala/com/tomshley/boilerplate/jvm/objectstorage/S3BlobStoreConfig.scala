/*
 * copyright 2026 tomshley llc
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

package com.tomshley.boilerplate.jvm.objectstorage

/**
 * Configuration for S3 BlobStore implementation.
 * 
 * @param region AWS region (e.g., "us-east-1")
 * @param accessKeyId AWS access key ID
 * @param secretAccessKey AWS secret access key
 * @param endpoint Optional custom endpoint (for MinIO, local dev)
 * @param pathStyleAccess Use path-style access (for MinIO, local dev)
 */
case class S3BlobStoreConfig(
    region: String,
    accessKeyId: Option[String] = None,
    secretAccessKey: Option[String] = None,
    endpoint: Option[String] = None,
    pathStyleAccess: Boolean = false
) {
  override def toString: String = {
    val maskedKeyId = accessKeyId match {
      case Some(keyId) if keyId.length > 4 => s"****${keyId.takeRight(4)}"
      case Some(_) => "****"
      case None => "<none>"
    }
    s"S3BlobStoreConfig(region=$region, accessKeyId=$maskedKeyId, secretAccessKey=<redacted>, endpoint=$endpoint, pathStyleAccess=$pathStyleAccess)"
  }
}
