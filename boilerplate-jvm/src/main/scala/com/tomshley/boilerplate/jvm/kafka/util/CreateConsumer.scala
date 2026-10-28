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

import org.apache.pekko.actor.typed.ActorSystem
import org.apache.pekko.kafka.ConsumerSettings

/** Configuration-only trait for Kafka consumer settings.
 *
 * Unlike CreateProducer (which constructs SendProducers, either shared and
 * managed per ActorSystem or caller-owned), this trait only produces
 * ConsumerSettings. Consumer lifecycle is managed by Pekko Kafka source
 * operators (e.g. Consumer.committableSource), which the application wires
 * directly.
 */
protected[util] trait CreateConsumer[K, V]:
  def consumerSettings(system: ActorSystem[?]): ConsumerSettings[K, V]
