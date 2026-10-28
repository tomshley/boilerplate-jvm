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

package com.tomshley.boilerplate.jvm.health

import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.cluster.{Cluster, MemberStatus}
import org.slf4j.LoggerFactory

import scala.concurrent.Future

@deprecated("Use TypedClusterHealthCheck", "next")
class ClusterHealthCheck(system: ActorSystem) extends (() => Future[Boolean]) {
  private val log = LoggerFactory.getLogger(getClass)

  private val cluster = Cluster(system)
  override def apply(): Future[Boolean] = {
    log.info("ClusterHealthCheck called")

    Future.successful(cluster.selfMember.status == MemberStatus.Up)
  }
}
