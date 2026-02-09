# Changelog

All notable changes to this project are documented in this file.

This project follows Semantic Versioning.

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

---

## Unreleased

- Initial OSS standardization (LICENSE, NOTICE, CONTRIBUTING, CODE_OF_CONDUCT, SECURITY, CHANGELOG, ROADMAP)
