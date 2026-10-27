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

## Idempotent request/reply (`reqreply`)

`Idempotent` is a complete cluster-sharded idempotency entity — claim, record,
release, and status — and `Idempotency.reqReplyOutcome` is the matching client
flow: claim a request id, run a callback for the fresh claim, and record the
reply so repeats replay the stored answer instead of re-running.

```scala
Idempotent.init(system)
val idempotency = Idempotency(system)
val outcome = idempotency.reqReplyOutcome(requestId, doWorkOnce())
```

`reqReplyOutcome` returns `Outcome.Completed(reply)` once a reply is stored
and `Outcome.Pending` when a duplicate claim has no recorded reply yet;
failures remain failed futures. `reqReply` is the compatibility form — it maps
`Pending` to `RequestReply(None, None)` and `Completed` to the reply, which
cannot distinguish pending from a deliberately recorded empty response. Use
`reqReplyOutcome` where that distinction matters.

The default entity keeps its bookkeeping in memory: claims and recorded replies
disappear when the entity stops, is passivated or rebalanced, or the node
restarts. Journal-backed durability is opt-in. `Idempotent.persistent` returns
a plain `EventSourcedBehavior`, so the caller chooses the journal, snapshot,
and supervision through Pekko itself — `withJournalPluginId`, `snapshotWhen`,
`withRetention`, `Behaviors.supervise` — and owns every type that lands in
storage. Pekko's `EventAdapter` and `SnapshotAdapter` translate between the
library's internal `Event`/`State` and the caller's own stored classes, so
persistence schemas, manifests, and bindings stay caller-owned:

```scala
Idempotent.init(system, ctx =>
  Idempotent
    .persistent(ctx.entityId, PersistenceId("Idempotent", ctx.entityId),
      myEventAdapter, mySnapshotAdapter)
    .withJournalPluginId("pekko.persistence.journal.my-plugin")
    .snapshotWhen(mySnapshotPredicate)
    .withRetention(myRetentionCriteria))
```

An explicit `SnapshotAdapter` is required even when snapshotting is disabled.
This keeps the consumer-owned snapshot schema explicit if snapshotting is
later enabled through Pekko's native settings. An identity default would
expose the library's `State` to snapshot serialization instead of lifting it
to a consumer-owned representation.

For a fresh claim, `reqReplyOutcome` completes only after the entity
acknowledges recording the reply, and the returned `Completed` response is the
entity's acknowledged stored reply — even when another record wins first. A
duplicate with a recorded reply replays the stored headers/body as
`Completed`; a duplicate with none returns `Pending` without invoking the
callback. A deliberately recorded empty response is a legitimate `Completed`
outcome, not pending; `repliedOn` — also exposed by the read-only
`idempotencyStatus` lookup — identifies a recorded reply.

A failed or thrown callback attempts to release its claim and propagates the
original failure. If that cleanup is not acknowledged, a warning is logged
and the claim may remain pending. A reply-record acknowledgement failure does
not release the claim and logs its own warning: the write or the callback's
external side effect may already have happened, so an unconfirmed record is an
uncertain outcome, not evidence to release. An acknowledgement establishes
recording at the entity, not survival of later in-memory state loss or
exactly-once external effects.

Between a fresh claim and the recorded reply's acknowledgement there is an
interval in which an in-memory shard can be rebalanced, passivated, or lost —
after the callback's external side effect has run. A later retry on a fresh
claim may then execute the callback again. Deduplication is by key alone: the
client sends no request payload, so a repeat is never compared by content.
Direct `SingleRequest`/`SingleReply` commands may store headers and body as
context, but a repeat still wins by key — reuse a request id only for the same
logical operation. Likewise the logical key and the `PersistenceId` used by
`Idempotent.persistent` are a caller-owned mapping: keep the association
stable across recovery instead of deriving it from arbitrary unique ids.

Recovered in-flight claims are not automatically re-executed or expired.
`idempotency.release(requestId)` explicitly abandons an incomplete claim and
returns its acknowledged summary; it leaves a completed reply intact and is
a no-op for an unclaimed key. In persistent mode a state-changing release is
journaled before acknowledgement; a failed release future must not be treated
as confirmation. The low-level equivalent is `Idempotent.AbandonRequest`.

Release is a recovery operation, not cancellation or a fenced takeover. The
caller must first reconcile the external outcome and ensure the previous
execution and its outstanding messages cannot later complete or mutate the
key. A timeout or pending status is not sufficient evidence. Keep new attempts
quiescent during recovery; an unacknowledged release can still arrive later,
so do not blindly retry it after admitting a new claim.

Duplicate claims and duplicate reply-record commands increment the existing
attempt counters and each append a journal event in persistent mode. They are
not read-only cache hits. The counters retain their existing semantics; callers
choose retention/compaction using Pekko's native persistence settings and use
`idempotencyStatus` for read-only observation.

A caller-built behavior remains an option — `Idempotent.init(system, ctx => ...)`
takes any `Behavior[Command]` factory under the same entity key.

---

## Same-origin subresource proxying (`reqreply.proxy`)

`SubresourceReverseProxy` serves declared third-party subresources — a tracker
script, a vendor beacon — from the site's own origin through a declared local
route. All parsing and validation happens once at wiring: `Endpoint.create` and
`SubresourceReverseProxy.create` return `Either[ConfigError, ...]` values the
caller handles, and `proxyRoutes` refuses proxies whose mounted paths would
shadow each other.

```scala
import com.tomshley.boilerplate.jvm.reqreply.proxy.{SubresourceReverseProxy, SubresourceReverseProxyDirectives}
import org.apache.pekko.http.scaladsl.model.HttpMethods

val wired =
  for
    script <- SubresourceReverseProxy.Endpoint.create("/script.js", Set(HttpMethods.GET))
    beacon <- SubresourceReverseProxy.Endpoint.create("/api/send", Set(HttpMethods.POST))
    proxy  <- SubresourceReverseProxy.create(
                name = "analytics",
                upstream = "https://upstream.example",
                mount = "/vendor",
                endpoints = Seq(script, beacon),
                timeout = configuredTimeout)          // caller-owned config value
    route  <- SubresourceReverseProxyDirectives.proxyRoutes(Seq(proxy))
  yield route
```

The snippet needs a `given ActorSystem[?]` in scope and a `configuredTimeout`
`FiniteDuration` from the caller's own configuration. Only the declared
endpoint paths answer — exact matches under the mount, only the declared
methods, and the inbound query is never forwarded — so the upstream surface is
the closed declared set, not an open proxy. `timeout` bounds the wait for
upstream response headers (a late reply is drained so it cannot hold a pool
slot); it is not transport cancellation or a whole-body deadline, and
connection-pool, idle, and TLS settings stay with the caller's HTTP client —
the `Send` overloads accept a custom `HttpRequest => Future[HttpResponse]` for
exactly that.

Application headers are optional allowlists in both directions — the defaults
are `user-agent`/`referer`/`accept-language` upstream and cache validators back.
Separately, protocol metadata is always carried because the entity bytes pass
through untouched: `content-encoding` in both directions; `content-range`,
`cache-control`, `vary`, `expires`, `age`, and `date` on responses; and
`content-type`/`content-length`, which ride on the entity itself. `cookie`,
`authorization`, `host`, and the `forwarded`/`x-forwarded-*` family can never
ride upstream, and `set-cookie`/`www-authenticate` can never come back —
declaring one fails wiring. Fields a `Connection` header nominates are stripped
per RFC 9110, and nominating protected metadata fails the exchange outright
(`400` inbound, `502` from upstream). Chunk extensions and trailers are dropped
on streamed bodies. First-party proxying changes who the browser connects to —
it does not make vendor content trustworthy.

---

## Static assets (`staticassets`)

`StaticAssetRouting.getStaticAssetRoute` serves files under `/static/` from the
classpath: the request path below that prefix is resolved against the class
loader and the file extension is mapped to a content type through
`StaticAssetType`. An unsupported extension, a path containing `.` or `..`
segments, or a missing resource rejects with
`StaticAssetRoutingRejection`, leaving the rendered response to the caller's
rejection handler; lookup and read failures carry their cause on the
rejection for diagnostics. Content types come from Pekko's media-type
registry: `text/css` and `text/javascript` carry `charset=UTF-8`, JSON types
render without a charset parameter, and images and fonts are binary.

Correct caching is one line, through a `CachePolicy` — a plain
``PartialFunction[Uri.Path, `Cache-Control`]`` applied to the request path
below `/static/`:

```scala
import com.tomshley.boilerplate.jvm.staticassets.StaticAssetCaching
import com.tomshley.boilerplate.jvm.staticassets.StaticAssetCaching.{immutableWhere, revalidate}

getStaticAssetRoute(system, StaticAssetCaching.immutable)                        // every file immutable
getStaticAssetRoute(system, immutableWhere("""\d+-[0-9a-f]{8}""".r) orElse revalidate)
```

`StaticAssetCaching` provides `immutable` (`public, max-age=31536000,
immutable` on every path), `revalidate` (`no-cache` on every path) and
`immutableWhere(segment: Regex)` (immutable on paths where some decoded path
segment fully matches, undefined otherwise — an encoded slash stays inside
its segment). Policies combine with the standard `orElse`; a caller may also
pass its own `PartialFunction` in place of any named policy. Where a policy is
undefined, no `Cache-Control` is sent. The library ships no default hash
pattern — which of a caller's paths carry content-derived names is the
caller's decision.

The one-argument overload sends the bare entity — no `ETag`, no
`Cache-Control`, and conditional-request headers are ignored. The
two-argument overload lets clients revalidate the bytes with an ETag; the
caller's cache policy controls freshness.

- **ETag:** a strong `"<first 16 hex digits of the SHA-256 of the bytes>"`.
- **Conditional requests:** Pekko's `conditional` directive answers a matching
  `If-None-Match` — including weak tags and `*` — with `304 Not Modified` and
  no body, and an `If-Match` that does not match — a weak tag never does —
  with `412`.
- **Cache-Control:** added to `200` and `304` responses where the policy is
  defined; every other status, `412` included, is left untouched. The policy
  is consulted only for a resource that exists and has a supported extension;
  rejections are unchanged. A `Cache-Control` the route sets itself is
  authoritative — the policy supplies the header only where none was sent, so
  an explicit `no-store` is neither duplicated nor overridden.

Each route value owns an index — an actor on the given actor system — that
computes a path's validator and byte length on its first request and keeps
them for the life of the system. The stored data is derived and write-once:
there is no eviction, because the key space is the packaged asset set, and a
path whose lookup or read fails is never remembered — the next request
retries. A `200` streams the resource with a known `Content-Length`; a `304`
needs no I/O at all. How long a first read and digest of an asset may take
before the request is rejected is
`tomshley-boilerplate-staticassets.ask-timeout`
(`TOMSHLEY_BOILERPLATE_STATICASSETS_ASK_TIMEOUT`), 5 seconds by default. A
running process serves the validator and length it first computed: a packaged
jar cannot change underneath it, but a dev-mode exploded classes directory
edited in place keeps its old index entries until restart.

For a route the named overload does not cover — an added response header, for
example — `StaticAssetDirectives` exposes the same pieces the route itself
composes: `staticAssets` and `cacheControlled` as ordinary Pekko directives
that join with `&`, and `staticAssetEntity` for the body:

```scala
import com.tomshley.boilerplate.jvm.staticassets.StaticAssetDirectives.*
import org.apache.pekko.http.scaladsl.model.headers.RawHeader
import org.apache.pekko.http.scaladsl.server.Directives.{complete, conditional, respondWithHeader}

val assets = staticAssets(system)            // once, at wiring: it owns the index
val custom = assets { (path, asset) =>
  (cacheControlled(cachePolicy)(path) & conditional(asset.etag) & respondWithHeader(RawHeader("x-asset-build", "42"))) {
    complete(staticAssetEntity(asset))
  }
}
```

The `ETag` describes the stored bytes. If a route transforms the body before
it reaches the wire — compressing it, say — the tag no longer describes the
representation sent, and the route must attach a validator for the
transformed representation together with the right `Vary` header, or use
Pekko's `conditional` with a weak tag where that is the appropriate strength.

The standalone `staticAssets` directive resolves resources through the class
loader that loaded `StaticAssetDirectives`; `getStaticAssetRoute` uses the
class loader of the class mixing in `StaticAssetRouting`. Digest work runs on
the materializer of the actor system passed in; response bodies materialize
where the route runs. Build the directive once at wiring: the returned value
owns the index actor.

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
