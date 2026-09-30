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

## Managed server setup

`ManagedSetup` is an immutable Pekko `Setup` value carried by `ActorSystemSetup`: build it once, pass it to `ActorSystemMain` (or `PekkoClusterMain`), and every `WebServerBoilerplate`, `GrpcServerBoilerplate`, and `TcpServerBoilerplate` listener started inside that actor system picks it up. The defaults apply only to these boilerplate listeners — anything bound directly through `Http().newServerAt`, `Tcp().bind`, or Pekko Management is unaffected. The setup-aware `apply` overloads return the `ActorSystem` handle so callers own shutdown; the original two-argument `apply` remains `Unit` for the existing fire-and-forget contract.

```scala
val setup = ManagedSetup.tls(httpsContext, createSSLEngine)
ActorSystemMain("service", setup, system => {
  WebServerBoilerplate.start(interface, webPort, system, routes)
  GrpcServerBoilerplate.start(interface, grpcPort, system, service)
  TcpServerBoilerplate.start(interface, tcpPort, system, handler)
  Startup.Ready
})
```

`ManagedSetup.tls` takes a Pekko `HttpsConnectionContext` (for example `ConnectionContext.httpsServer(...)`) for the HTTP transports and a `() => SSLEngine` factory invoked per accepted TCP connection, which must return a fresh engine in server mode. The caller owns certificate provisioning and renewal, trust configuration, client authentication, and HTTP/2 settings; these helpers change no global HTTP or SSL configuration. Without a managed setup the four-argument overloads keep the existing named parameters (`service`, `routes`, `handler`) and bind plaintext for deployments that terminate TLS before the application, and the five-argument `start` overloads taking an `HttpsConnectionContext` or `() => SSLEngine` remain for listeners that want TLS without managed setup.

A start can carve out an explicit local exception. Local configuration applies after managed defaults; this copy changes only TLS selection and preserves the already-configured handler:

```scala
WebServerBoilerplate.start(interface, port, system, routes, configure = _.copy(httpsContext = None))
```

`StartupTask` pairs an acquire with a release for once-per-system integrations — telemetry clients, schema registries, anything the service needs before it serves:

```scala
val telemetry = ManagedSetup(
  startup = Vector(StartupTask("telemetry", initializeTelemetry, closeTelemetry))
)
ActorSystemMain("service", setup.andThen(telemetry), system => {
  WebServerBoilerplate.start(interface, webPort, system, routes)
  Startup.Ready
})
```

`initializeTelemetry` and `closeTelemetry` are consumer-supplied functions of shape `ActorSystem[?] => Future[A]` and `(ActorSystem[?], A) => Future[Done]`; this module ships no telemetry adapter.

Startup tasks run sequentially in declared order inside a gate before `bootstrap` and the body — a failed acquisition or a startup-timeout expiry terminates the system before either runs.

Each task's release registers with coordinated shutdown before its acquire runs and executes in the native `before-actor-system-terminate` phase, after listeners have unbound. Tasks in the same phase run in parallel. The native phase timeout bounds how long coordinated shutdown waits; it does not cancel an acquisition or release already in flight. A cleanup waits for its pending acquisition to complete, and a task whose acquisition never started is not acquired by its cleanup.

A task whose acquisition fails partway must clean up its own partial allocation before failing — `release` only ever sees successfully acquired resources — and a failed release logs and does not stop sibling cleanup.

`ManagedSetup` values are immutable and per-system: `andThen` composes them without side effects, and the same value used on two actor systems acquires and releases independently in each.

All transports share one binding lifecycle: coordinated-shutdown registration completes before the binding future does, and a failed bind terminates the actor system. The HTTP transports register under a deadline read from `tomshley-boilerplate-transport.http.shutdown-timeout` (default `3 s`, environment override `TOMSHLEY_BOILERPLATE_TRANSPORT_HTTP_SHUTDOWN_TIMEOUT`). Keep the deadline below the `service-requests-done` phase timeout of `pekko.coordinated-shutdown` so it is honored. The managed startup gate is bounded by `tomshley-boilerplate-managedmain.startup-timeout` (default `30 s`, environment override `TOMSHLEY_BOILERPLATE_MANAGEDMAIN_STARTUP_TIMEOUT`). TCP shutdown unbinds the listener during `service-unbind`; it does not drain active TCP sessions.

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
