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

import com.tomshley.boilerplate.jvm.kafka.internal.ManagedProducer
import org.apache.pekko.actor.typed.{ActorSystem, DispatcherSelector, ExtensionId}
import org.apache.pekko.kafka.ProducerSettings
import org.apache.pekko.kafka.scaladsl.SendProducer

import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try

/** `SendProducer` construction for Kafka producers.
 *
 * `init` returns the managed `SendProducer` this factory shares with every
 * `init` caller on the same `ActorSystem`. Coordinated shutdown invokes
 * `close`, respecting the effective close ownership and close timeout.
 * Callers must not close this shared producer themselves. Kafka
 * construction runs asynchronously: failures reach the returned `Future`
 * and are not reused by later `init` calls. No construction deadline or
 * automatic retry policy is applied here; startup policy (e.g.
 * `Startup.gated`) belongs to the caller.
 *
 * `sendProducer` is the preferred path for a caller with its own restart
 * lifecycle: it builds the same way but shares nothing and registers no
 * shutdown task. The caller owns closing it, and can rebuild a fresh
 * producer on each restart instead of inheriting the shared one.
 */
protected[util] trait CreateProducer[K, V]:
  private val managed = new ExtensionId[ManagedProducer[K, V]]:
    override def createExtension(system: ActorSystem[?]): ManagedProducer[K, V] =
      ManagedProducer.start(system, () => sendProducer(system))

  final def init(system: ActorSystem[?]): Future[SendProducer[K, V]] =
    Try(managed(system).acquire()).fold(Future.failed, identity)

  /** Builds a `SendProducer` without sharing it or registering a shutdown
    * task — the caller owns the lifecycle and closes it itself.
    *
    * `producerSettings` runs on Pekko's blocking dispatcher, `enrichAsync`
    * is applied once on the dispatcher the settings configure, and the raw
    * producer is built on the dispatcher of the enriched settings.
    *
    * The producer is built explicitly so a construction failure surfaces as
    * a failed `Future` instead of hiding inside `SendProducer`'s memoized
    * producer future; the ready instance is then injected via
    * `withProducerFactory`, which keeps the enriched close ownership and
    * close timeout. `withProducer` must not be substituted: it forces
    * `closeProducerOnStop = false`, which would silently stop
    * `SendProducer.close` from closing anything.
    *
    * `SendProducer` still evaluates `enrichAsync` inside its own
    * `createKafkaProducerAsync` — Pekko's `enriched` runs on both branches of
    * its `producerFactorySync` match — so a live enrich would build a second
    * configured serializer per construction that is immediately discarded.
    * The enrich is therefore neutralized before the settings are handed over.
    */
  final def sendProducer(system: ActorSystem[?]): Future[SendProducer[K, V]] =
    given ExecutionContext = ExecutionContext.parasitic
    for
      preparation <- Future.fromTry(Try(system.dispatchers.lookup(DispatcherSelector.blocking())))
      settings <- Future(producerSettings(system))(using preparation)
      enrichment <- Future.fromTry(Try(system.classicSystem.dispatchers.lookup(settings.dispatcher)))
      enriched <- Future(settings.enriched)(using enrichment).flatten
      effective = enriched.withEnrichAsync(s => Future.successful(s))
      construction <- Future.fromTry(Try(system.classicSystem.dispatchers.lookup(effective.dispatcher)))
      kafkaProducer <- effective.createKafkaProducerAsync()(using construction)
    yield SendProducer(effective.withProducerFactory(_ => kafkaProducer))(using system)

  def producerSettings(system: ActorSystem[?]): ProducerSettings[K, V]
