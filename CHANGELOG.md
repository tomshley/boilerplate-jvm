# Changelog

All notable changes to this project are documented in this file.

This project follows Semantic Versioning.

---

## [Unreleased]

### Added
- **MintedPimpedBytes** — immutable pimped byte array type with content-based equality, cached hashCode, and transparent Jackson CBOR serialization (`basics` package)
- **Array[Byte].toMintedPimpedBytes** extension — pimp-my-library conversion to MintedPimpedBytes

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
