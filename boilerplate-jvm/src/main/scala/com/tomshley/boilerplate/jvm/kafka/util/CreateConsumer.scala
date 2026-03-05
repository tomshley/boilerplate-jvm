package com.tomshley.boilerplate.jvm.kafka.util

import org.apache.pekko.actor.typed.ActorSystem
import org.apache.pekko.kafka.ConsumerSettings

/** Configuration-only trait for Kafka consumer settings.
 *
 * Unlike CreateProducer (which manages a singleton SendProducer lifecycle),
 * this trait only produces ConsumerSettings. Consumer lifecycle is managed
 * by Pekko Kafka source operators (e.g. Consumer.committableSource),
 * which the application wires directly.
 */
protected[util] trait CreateConsumer[K, V]:
  def consumerSettings(system: ActorSystem[?]): ConsumerSettings[K, V]
