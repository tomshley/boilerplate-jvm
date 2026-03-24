# Changelog

All notable changes to this project are documented in this file.

This project follows Semantic Versioning.

---

## [Unreleased]

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
