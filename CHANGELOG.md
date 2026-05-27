# Changelog

All notable changes to this project are documented in this file.

This project follows Semantic Versioning.

---

## [2.2.0] — 2026-05-27

### Added — `durablebufferedflush` spool durability & pressure

- **`OrphanSpoolSweeper` sweep-duration logging** — every scheduled sweep
  now records its outcome at INFO (success) or WARN (failure) with a
  `durationMs=<n>` field for SLO instrumentation. A new `protected`
  testing seam `OrphanSpoolSweeperImpl.onSweepComplete(SweepOutcome)` is
  invoked once per sweep completion to permit deterministic observation
  in tests without taking a logback dependency.
- **`OrphanSpoolSweeper` Pekko `CircuitBreaker` failure-bounding** — the
  sweeper now wraps its call into `RecoveryManager.reconcileOrphans` in a
  Pekko `CircuitBreaker` parameterised by the new
  `FlushSweeperConfig.maxSweepDuration` (call timeout) and
  `maxConsecutiveFailures` (open threshold). When the breaker opens,
  subsequent ticks fast-fail with `Circuit Breaker is open` until the
  configured reset timeout elapses; lifecycle transitions
  (CLOSED → OPEN → HALF-OPEN → CLOSED) are logged at INFO.
- **`RecoveryManager` per-entity timeout** — recovery and reconciliation
  steps for a single entity are now bounded by the new
  `FlushRecoveryConfig.perEntityTimeout`. A stuck `isActive` predicate
  or `recoverSession` call no longer blocks the whole sweep; the
  timed-out entity is treated as a per-entity failure (counted, surfaced,
  but not allowed to halt progress on its peers).
- **`SpoolSizeReporter`** — sibling capability trait that
  `ChunkSpool & SpoolSizeReporter` implementations satisfy to report
  current spool occupancy. Both methods are asynchronous and actor-owned
  — `currentSizeBytes(): Future[Long]` is O(1) (one mailbox enqueue +
  Promise allocation) and `recountFromFilesystem(): Future[Long]` walks
  the spool root and emits a `ReplaceWith` message to the size-accounting
  actor so the counter is corrected for drift on every reconciliation
  cycle. There is no atomic primitive and no shared mutable state on
  the size-accounting path — the count lives entirely inside the actor's
  behavior recursion, in the same architectural slot as `AdmissionGate`
  and `SpoolPressureMonitor`.
- **`SpoolPressureLevel`** — Scala 3 `enum` with three cases
  (`Low` / `High` / `Critical`) emitted by `SpoolPressureMonitor` on
  level transitions.
- **`SpoolPressureConfig`** — configuration carrier with two thresholds
  (alert / critical), two clear edges (alert-clear / critical-clear,
  for hysteresis), monitor and reconciliation cadences, optional
  configured capacity ceiling, and an operator-suggested
  `suggestedRetryAfter` carried by `SpoolPressureCriticalException`.
  Ships a `SpoolPressureConfig.Disabled` sentinel and a
  `SpoolPressureConfig.fromConfig(parent)` HOCON reader that returns
  `Disabled` when the `pressure { ... }` block is absent.
- **`SpoolPressureMonitor`** — periodic monitor that samples
  `SpoolSizeReporter` on a fast tick, applies the threshold and
  hysteresis state machine, emits transitions to
  `onLevelChange` subscribers (synchronous; exceptions contained), and
  invokes the slow `recountFromFilesystem` cadence to correct hot-path
  counter drift. Mirrors `OrphanSpoolSweeper`'s idempotent
  `start` / `stop` lifecycle and tick re-entry guard. A second factory
  overload accepts a caller-supplied `capacityProvider: () => Long`
  for filesystem-aware capacity (canonically
  `min(configured, fs.totalSpace)`).
- **`AdmissionController`** — single-bit gate queried once per session in
  `Workflow.prepareTransfer`. The default implementation is an internal
  typed actor whose state is a `Boolean` carried through behavior
  recursion — no `var`, no atomic, no shared mutable memory. All three
  methods (`isOpen()`, `open()`, `close(reason)`) return `Future`, fold
  cleanly into `Workflow.prepareTransfer`'s existing async chain, and
  the two transitions log only on the edge. Ships an
  `AdmissionController.AlwaysOpen` sentinel (pre-completed futures, no
  actor traffic) used as the default by the six-argument
  `Workflow.apply`. In-flight sessions are never gated — closing
  admission affects new sessions only.
- **`SpoolPressureCriticalException`** — typed backpressure signal
  raised by `Workflow.prepareTransfer` when the admission controller
  refuses a new session. Carries the operator-suggested `retryAfter`
  for transport-agnostic surfacing (TCP error frame, HTTP
  `503 Retry-After`, gRPC status, etc.).
- **`Workflow.apply` admission-aware overload** — new 8-argument factory
  that takes `admissionController: AdmissionController` and
  `pressureConfig: SpoolPressureConfig`. The existing 6-argument
  factory continues to compile and wires the
  `AdmissionController.AlwaysOpen` + `SpoolPressureConfig.Disabled`
  defaults, so call sites that do not yet wire pressure are unaffected.

### Changed

- **`ChunkSpool.filesystem` return type** widened from `ChunkSpool` to
  `ChunkSpool & SpoolSizeReporter`. Source-compatible (intersection is a
  subtype of `ChunkSpool`); new consumers may narrow to the reporter
  capability without a runtime cast.
- **`FlushSweeperConfig`** now carries `maxSweepDuration` and
  `maxConsecutiveFailures` (parsed from HOCON keys
  `sweeper.max-sweep-duration` and `sweeper.max-consecutive-failures`).
  Defaults are `interval × 10` and `3` respectively — a loose bound for
  background reconciliation so that a transient slow downstream does not
  flap the breaker.
- **`FlushRecoveryConfig`** now carries `perEntityTimeout` (parsed from
  `recovery.per-entity-timeout`, default `120.seconds`). Sized to absorb
  realistic minute-scale tail latencies on backing object-storage /
  messaging round-trips.
- **`OrphanSpoolSweeperImpl`** is no longer `final`. The testing seam
  `onSweepComplete` lives there and tests subclass the impl to observe
  outcomes deterministically.

### Migration notes

- Downstream projects that already supply HOCON config for
  `recovery` / `sweeper` need not add the new keys — defaults are
  conservative and preserve existing behavior.
- Downstream projects that want spool-pressure backpressure must
  (a) construct a `SpoolPressureMonitor` against the
  `ChunkSpool & SpoolSizeReporter` returned by `ChunkSpool.filesystem`,
  (b) construct an `AdmissionController`, (c) wire
  `monitor.onLevelChange { case Critical => admission.close(...); case _ => admission.open() }`
  — the returned `Future[Unit]` may be ignored on the monitor tick
  (transitions are idempotent and the gate actor enqueues messages in
  order), or piped to an observer for telemetry,
  (d) build the `Workflow` via the new 8-argument
  `Workflow.apply(..., admissionController, pressureConfig, system)`
  overload, and (e) handle `SpoolPressureCriticalException` at the
  transport boundary.

---

## [2.1.1] — 2026-04-27

### Changed
- Bump `magicroot-sbt-projectsettings` plugin `2.0.3 -> 2.0.4`. magicroot
  2.0.4 modernises `apacheCommonsIOVersion` from the ancient
  `20030203.000550` Feb-2003 1.x build to `2.16.1`.

### Removed
- Drop the local `dependencyOverrides += "commons-io" % "commons-io" % "2.15.1"`
  workaround. With magicroot 2.0.4 shipping a modern `commons-io`
  baseline through `javaProject` (composed into the `LibProjectPekko*`
  plugin family), the consumer-side override is no longer needed to
  keep `commons-compress 1.27` and `org.apache.commons.io.Charsets`
  resolvable under modern testcontainers.

---

## [2.1.0] — 2026-04-24

> MINOR bump (not PATCH) per `ThisBuild / versionScheme := Some("semver-spec")`.
> Changes in this release are source-compatible but carry a default-policy
> change (see below) and add fields to `SchemaRegistryConfig`, which per
> standard Scala case-class semantics rewrites the synthetic `apply` / `copy`
> / `unapply` / primary-constructor signatures and is therefore not
> binary-compatible with `2.0.1` jars.

### Changed
- **`SchemaRegistryConfig`**: Introduced opinionated schemas-as-code defaults for the
  Confluent Schema Registry serializer:
  - `autoRegisterSchemas` defaults to `false` (was `KafkaAvroSerializer`'s built-in
    default of `true`).
  - `useLatestVersion` defaults to `true` (was `KafkaAvroSerializer`'s built-in
    default of `false`).
  - These flags are emitted from the new `toSerializerConfig`, reach the
    `KafkaAvroSerializer` via `SchemaRegistrySerde.serializer`, and are
    overridable from HOCON via `schema-registry.auto-register-schemas` and
    `schema-registry.use-latest-version`, or programmatically via the
    constructor arguments.
- **Config surfaces split** on `SchemaRegistryConfig`:
  - New: `toSerializerConfig` — URL + auth + `auto.register.schemas` +
    `use.latest.version`. Consumed by `KafkaAvroSerializer.configure`.
  - New: `toClientConfig` — URL + auth only. Consumed by
    `KafkaAvroDeserializer.configure` and `CachedSchemaRegistryClient`,
    neither of which honour the serializer-only gate flags; keeping the
    gate flags out of these surfaces prevents dead-weight config and
    removes a forward-compat liability.
  - `SchemaRegistrySerde.serializer` now calls `toSerializerConfig`;
    `SchemaRegistrySerde.deserializer` and `SchemaPublication.publishWithRetry`
    now call `toClientConfig`.

### Deprecated
- `SchemaRegistryConfig.toConfluentConfig` — aliased to `toSerializerConfig`
  for one-release source compatibility. New code should pick
  `toSerializerConfig` or `toClientConfig` explicitly based on the target
  Confluent surface. Scheduled for removal in `3.0.0`.

### Fixed
- **Schema drift via runtime auto-registration** (root cause of Confluent Cloud
  UI rendering `{"__raw__": "…"}` for messages published by avro4s-based
  producers): with `auto.register.schemas = true` (the prior default), each
  boilerplate upgrade that shipped a new avro4s (notably `5.0.15`, included in
  `2.0.0`) silently minted new schema-registry versions whose encoder output
  differed from the hand-registered `.avsc` source of truth — including
  `"default": ""` for enum fields, which is not a legal Avro default and
  causes downstream Avro deserializers to fall back to the raw-bytes view.
  The new defaults stop the producer from minting those drift versions;
  subjects are now registered exclusively by the schema-repo runbook / CI
  job, and producers encode against the registry's latest version.

### Migration notes
- Downstream projects that rely on the previous auto-registration behavior
  (typically local or test setups that never seed the registry) must either:
  - pre-register schemas via their schema-repo runbook before starting
    producers, or
  - opt back in explicitly with `schema-registry.auto-register-schemas = true`
    in HOCON (or pass `autoRegisterSchemas = true` to `SchemaRegistryConfig`).
    When opting into auto-registration, also set `useLatestVersion = false`
    to make intent explicit — Confluent resolves the ambiguous
    `(autoRegister=true, useLatest=true)` state by taking the auto-register
    branch (verified against `kafka-avro-serializer` 7.5.x bytecode), so the
    default `useLatestVersion = true` becomes dead code in that configuration.
- **Source-compatible**: existing call sites of `SchemaRegistryConfig`,
  `SchemaRegistrySerde`, `ProducerAvroBoilerplate`, and
  `ConsumerAvroBoilerplate` recompile unchanged against `2.1.0`; the two
  new fields are appended with default values and `toConfluentConfig` is
  preserved as a deprecated alias.
- **Binary-incompatible**: appending fields to the `SchemaRegistryConfig`
  case class changes the primary constructor and the generated
  `apply`, `copy`, and `unapply` signatures per standard Scala case-class
  semantics. Downstream consumers pinned against `2.0.1` jars must
  recompile against `2.1.0`. This is the reason this is a MINOR bump
  rather than a PATCH under the declared `semver-spec` scheme.

---

## [2.0.1] — 2026-04-23

### Changed
- **magicroot-sbt-projectsettings**: Updated to `2.0.3`.
  - magicroot `2.0.3` adds `org.postgresql:r2dbc-postgresql:1.0.7.RELEASE`
    to `pekkoPersistenceLibraries` because upstream `pekko-persistence-r2dbc`
    1.1.0 demoted the driver from compile scope to `<scope>test</scope>`.
  - `boilerplate-jvm` itself does not consume `pekkoPersistenceLibraries`,
    so the runtime classpath of services that depend on boilerplate is
    unchanged. This bump is purely an alignment refresh so the workspace
    tree pins a single magicroot version across `boilerplate-jvm` and
    downstream service repos.

### Notes
- magicroot `2.0.2` artifact is yanked (missing r2dbc-postgresql driver
  broke downstream ingress; see magicroot 2.0.3 CHANGELOG).
- No source code changes in this release — single-line plugin pin bump.

---

## [2.0.0] — 2026-04-23

Major version bump signals the coordinated Pekko family upgrade + SDK
refresh inherited from `magicroot-sbt-projectsettings 2.0.2`. The
`boilerplate-jvm` API surface itself is unchanged — no renames, no
removals. The major is a heads-up that the vendored Pekko stack runtime
has moved forward (`1.1.x` → `1.5.x`) and downstream services should
regression-test before promoting to production.

### Changed
- **magicroot-sbt-projectsettings**: Updated to `2.0.2`
  - Inherits Pekko `1.1.x` → `1.5.x` (core, http, management, kafka-connector
    milestone → GA). Includes the `PekkoProjectionVersion` split — pekko-projection-*
    and pekko-persistence-r2dbc are pinned at `1.1.0` (their latest stable),
    independent of `PekkoManagementVersion` `1.2.1`.
  - magicroot `2.0.0` and `2.0.1` were never consumable (bad projection pin + partial
    publish); `2.0.2` is the first usable 2.x release.
  - `pekko-connectors-kafka 1.1.0-M1` → `1.1.0` GA resolves the
    `Transactional.flow` stall against cp-kafka 7.6 observed in
    downstream E2E tests, and bumps the transitive `kafka-clients` to
    `3.8.0`.
  - Library refresh: avro4s `5.0.15`, logback `1.5.32`, scalatest `3.2.20`,
    twilio `12.0.0`, aws-sdk `2.42.39`, scalapb `0.11.20`, protobuf-java
    `3.25.5`.
  - Deliberately held: Confluent `7.6.0` (8.x needs staged migration),
    Testcontainers `1.20.0` (2.x relocates `KafkaContainer`).
- **avro4s encoder return types** (`boilerplate-jvm/marshalling/serializers/avro/package.scala`):
  Changed `Encoder.encode` return types from `T => Any` to `T => AnyRef` for
  the custom `TimeUtils.DateTime`, `ZonedDateTime`, `File`, and `Path`
  encoders to match avro4s `5.0.15`'s tightened `Encoder` trait signature.
  All implementations already returned `String` (which is `AnyRef`); the
  change is purely a type-annotation refinement, not a behavior change.

### Migration notes
- Downstream projects should bump both pins together:
  - `project/plugins.sbt`: `magicroot-sbt-projectsettings 2.0.2`
  - `build.sbt`: `boilerplate-jvm 2.0.0`
- No `boilerplate-jvm` API surface changes. `AvroMarshaller`,
  `TwilioClient`, `ProducerAvroBoilerplate`, `ConsumerAvroBoilerplate`,
  `SchemaRegistrySerde`, `TopicCreation`, `PekkoClusterMain`, and the
  `durablebufferedflush` / `managedmain` packages retain their existing
  signatures.
- Recommended downstream validation sweep: run integration tests that
  exercise `Transactional.source` / `Transactional.flow`, pekko-http
  routes, pekko-cluster-sharding entity recovery, and the Confluent
  Schema Registry serde path before promoting.

---

## [1.10.4] — 2026-04-23

### Changed
- **magicroot-sbt-projectsettings**: Updated to 1.3.22
  - Consumes the Confluent Platform bump from `6.2.0` to `7.6.0` in
    `PekkoProjectSettings` (`KafkaAvroVersion`, `KafkaStreamsVersion`).
    Aligns the transitive `kafka-clients` artifact (`2.8.0` → `3.6.0`)
    with the `confluentinc/cp-kafka:7.6.0` broker used in downstream
    service CI. The prior 2.8-era client's transactional producer
    protocol handshake hung indefinitely against a 3.6 broker, blocking
    downstream E2E transactional tests.

---

## [1.10.3] — 2026-03-26

### Changed
- **magicroot-sbt-projectsettings**: Updated to 1.3.18
  - Consumes dockerUpdateLatest fix that prevents branch/release/hotfix pipelines from publishing plain latest Docker tag

---

## [1.10.2] — 2026-03-26

### Added
- **CloseValidationFailure.CannotAbortClosedSession** — new fatal error type for abort attempts on already-closed sessions
- **CloseValidationFailure classification** — updated `classifyCode`, `classifyMessage`, and `classify` methods to handle `CannotAbortClosedSession`

---

## [1.10.1] — 2026-03-25

### Changed
- **magicroot-sbt-projectsettings**: Updated to 1.3.17 for STS dependency support in pekkoStorageLibraries

---

## [1.9.0] — 2026-03-24

### Added
- **TypedHealthCheck** — typed actor system health check returning `Future[Boolean]`
- **TypedClusterHealthCheck** — cluster-aware health check that reports true when member status is `Up` or `WeaklyUp`
- **TypedClusterHealthCheckRoutes** — standalone HTTP health endpoint with coordinated shutdown unbind and bind-failure logging
- **ChecksumUtil.toSHA256** — SHA-256 hex digest for cryptographically strong hashing
- **TimeUtils.toFiniteDuration** — extension for `java.time.Duration` to Scala `FiniteDuration` conversion
- **InfiniteIterationPromiseDeprecatedSpec** — test coverage for deprecated `InfiniteIterationPromise`
- **FlushConfigSpec** — test coverage for `FlushConfig` typed configuration parsing
- **TypedHealthCheckSpec**, **TypedClusterHealthCheckSpec**, **TypedClusterHealthCheckRoutesSpec** — health check test suites
- **StartupGatedSpec** — test coverage for `ManagedMain` startup gate
- **SingleShotBlobWriterSpec** — test coverage for single-shot blob writes
- **S3ObjectExistsRecoverySpec** — test coverage for S3 existence check error recovery
- **ChecksumUtilSpec** — SHA-256 test coverage
- **TimeUtilsDeprecatedSpec** — test coverage for deprecated time utilities

### Changed
- **ClaimDispatch** renamed to **ClaimPort** — clearer port/adapter naming
- **SessionOps** renamed to **SessionPort** — clearer port/adapter naming
- **JsonMarshaller** — extracted `given formats` to trait level, removing redundant per-method declarations
- **S3BlobStoreBoilerplate** — `objectExists` uses `recoverWith` with proper `Future` composition instead of `recover` with side-effecting throw
- **Idempotency** — updated to `given` syntax for timeout; cleaner `flatMap` structure
- **TwilioClient** — replaced if-else with idiomatic pattern matching on `configInstanceMaybe`
- **ConfigKeyUtil** — updated to Scala 3 `using` parameter syntax; eliminated `.get` call
- **StaticAssetRouting** — replaced nested if-else with `match`/`case` pattern matching
- **FlushConfig** — `retryDelay` uses `toScala` duration conversion instead of manual millisecond extraction
- **WebServerRoutingBoilerplate** — `routes` returns `Seq.empty` instead of throwing `NotImplementedError`
- **CloseBarrier** — fixed indentation for `Future.successful(())` branch
- **InfiniteIterationPromise** — marked `@deprecated` in favor of `Pekko Behaviors.withTimers`
- **LoadStatusStartup** — modernized to Scala 3 syntax
- **ClusterHealthCheck**, **ClusterHealthCheckRoutes**, **HealthCheck** — marked `@deprecated` in favor of typed equivalents
- All test specs use `BeforeAndAfterAll` with proper `ActorTestKit` shutdown to prevent resource leaks

### Removed
- **TransportContext** — unused empty class removed
- **SessionOps** file — replaced by **SessionPort**
- **ClaimDispatch** file — replaced by **ClaimPort**

---

## [1.8.0] — 2026-03-24

### Added
- **durablebufferedflush** — new `durablebufferedflush` package with write-ahead-log semantics for chunk spool-flush workflow
  - **ChunkSpool** — local filesystem spool with fsync durability, zero-padded 9-digit sequence filenames, atomic `meta.json` updates
  - **ChunkFlusher** — background flusher with configurable batch size and interval, contiguous watermark tracking
  - **ClaimLagMonitor** — actor-based lag monitoring between spooled and flushed sequences
  - **CloseValidationFailure** — structured close-time validation with gap detection and recovery hints
  - **ExpectedCountRegistry** — tracks expected chunk counts per entity for close validation
  - **RecoveryManager** — startup recovery scanning of spool directories, resumes in-flight flushes
  - **SessionOps** — session lifecycle operations (open, claim, close) integrated with spool
  - **Workflow** — orchestrates spool → flush → close with WAL semantics
  - **ClaimPort** — trait for decoding envelopes and dispatching claims with reply bindings
  - **BlobKeyResolver** — trait for mapping entity + sequence to blob storage keys
  - **FlushConfig** — typed configuration for flush intervals, batch sizes, and timeouts
- **ManagedMain** — `Startup` and `StartupGate` for ordered async initialization with dependency tracking

### Changed
- **TcpServerBoilerplate** — enhanced connection lifecycle: track latest handler state via `AtomicReference`, `watchTermination` to invoke `onConnectionClosed` on stream termination
- **TcpServerHandlerBoilerplate** — added `onConnectionClosed(state: State, cause: Option[Throwable])` lifecycle hook with default no-op

---

## [1.7.3] — 2026-03-19

### Added
- **TcpServerBoilerplate** — `onConnectionClosed` lifecycle hook: tracks latest handler state via `AtomicReference`, invokes `onConnectionClosed(state, cause)` callback on TCP stream termination
- **TcpServerHandlerBoilerplate** — `onConnectionClosed(state: State, cause: Option[Throwable])` with default no-op

---

## [1.7.2] — 2026-03-18

### Changed
- Removed IDE-specific entries from `.gitignore`

---

## [1.7.1] — 2026-03-08

### Changed
- **S3BlobStoreConfig** — `accessKeyId` and `secretAccessKey` changed from `String` to `Option[String] = None` (**breaking**: callers must wrap values in `Some(...)`)
- **S3BlobStoreBoilerplate** — falls back to `DefaultCredentialsProvider` (IAM role, env vars, instance metadata) when credentials are `None`; logs warning when only one of accessKeyId/secretAccessKey is provided

---

## [1.7.0] — 2026-03-04

Release of 1.6.0 develop changes to main. See 1.6.0 for details.

---

## [1.6.0] — 2026-03-04

### Added
- **AvroMarshaller** — trait/object for Avro serialization of `MarshallModel[T]` instances via avro4s, with sync and async methods mirroring `JsonMarshaller`
- **Opt-in Avro serializers** — `given SchemaFor/Encoder/Decoder` for `DateTime`, `ZonedDateTime`, `File`, `Path` (all as Avro STRING) in `serializers.avro` package
- **KafkaKeyAvroMessageEnvelope model overload** — new `apply[T <: MarshallModel[T]]` accepting domain models directly (auto-converts to `GenericRecord`)
- **AvroMarshallerSpec** — round-trip, schema, async, enum, and envelope tests
- **AvroSerializersSpec** — round-trip and schema-type tests for all 4 custom Avro serializers
- **MintedPimpedBytes** — immutable pimped byte array type with content-based equality, cached hashCode, and transparent Jackson CBOR serialization (`basics` package)
- **Array[Byte].toMintedPimpedBytes** extension — pimp-my-library conversion to MintedPimpedBytes
- **CreateConsumer** — trait for Kafka consumer settings factory (mirrors `CreateProducer`)
- **ConsumerAvroBoilerplate** — Avro consumer settings with Confluent Schema Registry (mirrors `ProducerAvroBoilerplate`)
- **ConsumerProtoBoilerplate** — Proto/byte-array consumer settings (mirrors `ProducerProtoBoilerplate`)
- **KafkaKeyAvroConsumerEnvelope** — inbound Avro envelope with optional `MarshallModel` deserialization (mirrors `KafkaKeyAvroMessageEnvelope`)
- **KafkaKeyProtoConsumerEnvelope** — inbound Proto envelope with typed extraction (mirrors `KafkaKeyProtoMessageEnvelope`)
- **ProducerAvroBoilerplate.producerSettings overload** — accepts explicit `schemaRegistryUrl` for override
- **ConsumerAvroBoilerplate.consumerSettings overload** — accepts explicit `schemaRegistryUrl` for override

### Changed
- **TopicCreation** — fully async (`Future[Admin]`), removed blocking `.get()` and `sys.exit`; callers compose timeout/failure handling via `Future`
- **WebServerBoilerplate** — added bind-failure handling (`system.terminate()` on failure) and `addToCoordinatedShutdown` (matches `GrpcServerBoilerplate` / `TcpServerBoilerplate`)
- `AvroMarshaller` uses `MarshallModel[T]` (not a separate `AvroMarshallModel`) — one marker trait for all marshalling formats

---

## [1.5.3] — 2026-03-03

### Added
- **MintedPimpedBytes** — immutable byte array wrapper with content-based equality, cached hashCode, and transparent Jackson CBOR serialization via `@JsonValue`/`@JsonCreator(mode = DELEGATING)`
- **Array[Byte].toMintedPimpedBytes** extension method for pimp-my-library conversion

---

## [1.5.2] — 2026-03-02

### Changed
- **KafkaKeyAvroMessageEnvelope** — switched from `SpecificRecord` to `GenericRecord` for Avro serialization (aligns with avro4s `ToRecord` output)
- **ProducerAvroBoilerplate** — `ProducerSettings[String, SpecificRecord]` → `ProducerSettings[String, GenericRecord]`; removed `SPECIFIC_AVRO_READER_CONFIG`; removed redundant `configure()` override on typed serializer wrapper

---

## [1.5.1] — 2026-02-26

- Initial OSS standardization (LICENSE, NOTICE, CONTRIBUTING, CODE_OF_CONDUCT, SECURITY, CHANGELOG, ROADMAP)

---

## [1.5.0] — 2026-02-11

### Added
- **SingleShotBlobWriter** — new `objectstorage` trait + `DefaultSingleShotBlobWriter` for single-call blob writes via `BlobStoreBoilerplate`
- **InMemoryClaimCheck** — lightweight in-memory `ClaimCheck` test helper replacing `InMemoryContentEnricher`

### Removed
- **ClaimCheck default implementation** — `DefaultClaimCheck`, `ClaimCheck.apply` factory, and `ContentEnricher` trait removed; `ClaimCheck` is now a pure trait for downstream implementation
- **ClaimCheckBoilerplate** — removed persistence-backed claim-check orchestration
- **ClaimCheckState** — removed durable state model for claim-check
- **ClaimCheckLifecycleEvent** — removed lifecycle event hierarchy
- **ClaimCheckStorageAdapter** — removed multipart-upload storage adapter bridge
- **filetransfer package** — `ChunkedTransferBoilerplate`, `ChunkedTransferState`, `TransferLifecycleEvent` and all associated tests removed

### Changed
- `ClaimCheck` trait no longer imports `ExecutionContext` (pure interface, no default impl)
- `ClaimCheckSpec` updated to use `InMemoryClaimCheck` directly

---

## [1.3.0] — 2026-02-09

### Added
- **ClaimCheckStorageAdapter** — trait bridging the Claim-Check pattern with blob storage; manages multipart upload sessions, part accumulation, duplicate-part detection, retry-after-failure semantics, and `ClaimTicket` production
- **ChecksumUtil companion object** — `ChecksumUtil` is now available as both a trait and an object
- **ChecksumUtil.computeCrc32** — CRC32 checksum computation for byte arrays
- **ClaimCheckStorageAdapterSpec** — comprehensive test suite covering store, complete, abort, duplicate detection, retry, and part-count validation
- **ChecksumUtilSpec** — CRC32 test coverage (determinism, known value, empty input)

### Changed
- `ChecksumUtil` trait now has a companion object for direct invocation without mixing in

---

## [1.2.0] — 2026-02-01

### Added
- ClaimCheckState, ClaimCheckBoilerplate, CborSerializable markers for claimcheck and filetransfer packages
- `crc32` field on `ItemReceived` event
- `withPendingKey`/`withClaim` abstract helpers on `ClaimCheckState`
- ClaimCheckStateSpec, ClaimCheckBoilerplateSpec test suites
