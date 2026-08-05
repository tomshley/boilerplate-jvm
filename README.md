<p>
  <img src="assets/brand/logo.svg" alt="Tomshley Logo" width="200"/>
</p>

# Tomshley Boilerplate JVM

Hexagonal architecture boilerplate for JVM/Scala projects.

This repository is part of the **Tomshley – OSS IP Division** and is maintained by **Tomshley LLC**.

---

## Overview

This project provides opinionated boilerplate for building JVM applications using hexagonal architecture patterns with Scala and SBT.

---

## Authenticated tokens (`security.tokens`)

Short, opaque, expiring identifiers that a service can verify offline — no lookup,
no shared database, no reversible encryption.

```scala
import com.tomshley.boilerplate.jvm.basics.MintedPimpedBytes
import com.tomshley.boilerplate.jvm.security.tokens.*
import java.time.Instant
import scala.concurrent.duration.*

val keyring = Keyring.fromHex(Map(3 -> sys.env("MY_TOKEN_KEY_3"))) // 32 hex bytes, e.g. openssl rand -hex 32
val profile = TokenProfile("my-family-v1\n", version = 1, payloadLength = 12, grace = 72.hours)

val token = CompactMacToken.mint(keyring, profile, MintedPimpedBytes(subjectBytes), expiryDaysSinceEpoch)

CompactMacToken.verify(keyring, profile, candidate, Instant.now()) match
  case Right(verified)                          => admit(verified.payload, verified.kid)
  case Left(RejectionReason.Expired)            => renew()
  case Left(RejectionReason.SignatureMismatch)  => quarantine()
  case Left(_)                                  => quarantine()
```

- **Keys stay inside.** `Keyring` exposes slot metadata and rotation only; no method
  returns key bytes, and `toString` prints slot ids. Rotation is `withKey` (add the
  next slot) then `retire` (once the old slot's tokens have drained).
- **Verification is total.** `verify` never throws — every outcome is a
  `RejectionReason`, and the reasons stay distinct so a tampered token never looks
  like a merely stale one. Comparison is constant-time and sealed in the package.
- **The caller owns the semantics.** Domain separator, version, payload length, and
  expiry grace are yours (`TokenProfile`); so is the decode ladder over older
  identifier generations (`TokenShapes.isUuidShaped` / `isDecimalUint32`).
- **`ExpiringSignedValue`** does the same for variable-length values, and
  `reqreply.SignedValueDirectives` wires it into pekko-http routes. Both replace
  the removed `ExpiringValue`/`InsecureSaltedEncryptionUtil`: AES-ECB with
  a static salt, reversible, and unauthenticated despite their `*Hmac` names.

Signed values are **authenticated, not encrypted** — put facts whose integrity
matters in them, never secrets.

---

## Contributing

See CONTRIBUTING.md.

---

## Security

See SECURITY.md.

---

## License

Apache License 2.0. See LICENSE and NOTICE.md.

---

## Credits

Maintained by Tomshley LLC.
Tomshley and the Tomshley logo are trademarks of Tomshley LLC.
