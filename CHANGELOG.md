# Changelog

All notable changes to GA-PubSub are documented in this file.

This project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html)
and [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).

Security fixes are marked with `[SECURITY]`.

---

## [1.0.0] — 2026-06-06

### 🎉 Initial Release

GA-PubSub 1.0.0 is the first stable, production-ready release of the
enterprise-grade, security-first, transport-agnostic eventing platform
for JavaScript and TypeScript ecosystems.

---

### Core (`ga-pubsub`)

#### Added

**Event Bus Engine**
- `EventBus` class with full lifecycle management (`subscribe`, `subscribeOnce`, `unsubscribe`, `unsubscribeAll`, `publish`, `destroy`)
- Priority-ordered subscriber dispatch — higher priority runs first
- Asynchronous middleware pipeline (Express-style `next()` chaining)
- Schema validation layer supporting Zod, JSON Schema (Ajv), Valibot, and custom validators
- Per-event authorization layer with async support and wildcard matching
- Request/Response (RPC) pattern via `bus.request()` / `bus.respond()` with configurable timeout
- `SubscriptionHandle` returned from `subscribe()` with `.unsubscribe()` method

**Wildcard Matching Engine**
- `*` — matches exactly one segment (`user.*` → `user.created`)
- `**` — matches zero or more segments (`payments.**` → `payments.invoice.line.created`)
- Combined patterns (`*.invoice.*`)
- O(1) exact-match lookup via hash map index
- O(k) wildcard matching per pattern via pre-compiled segment arrays
- LRU-bounded pattern compilation cache (4,096 entries max) — prevents memory exhaustion from adversarial input

**Replay Engine**
- Per-event-name FIFO ring buffer with configurable `limit`
- Time-based TTL retention with lazy eviction on read
- Periodic 60-second sweep to prevent TTL staleness
- Late subscriber replay with `replayLastMs` and custom `replayFilter` options
- Wildcard replay (`user.*` replays all stored `user.*` events)
- `storeHistory: false` opt-out per publish

**Security Layer** (`[SECURITY]`)
- HMAC-SHA256 envelope signing via Web Crypto API (`crypto.subtle`)
- Constant-time signature verification (prevents timing oracle attacks)
- Replay attack prevention via monotonic sequence numbers, tracked per `(namespace, source)` pair
- Token bucket rate limiter per source — configurable rate + burst
- Payload size enforcement (default 64 KB) applied before any processing
- Subscription limit enforcement (DOS protection)
- Payload sanitization via `structuredClone` (JSON fallback) — strips `__proto__` / prototype pollution vectors
- Event TTL expiration
- Sequence tracker bounded to 10,000 sources; rate limiter bounded to 50,000 sources

**Namespace Registry**
- `getBus(namespace, options)` — returns or creates isolated `EventBus` per namespace
- `destroyNamespace(namespace)` — tears down bus + transport + all subscriptions
- `destroyAll()` — test teardown utility
- `listNamespaces()` — introspect active namespaces
- `createScopedBus(bus, scope)` — prefix-scoped bus wrapper for module-level isolation

**Metrics & Observability**
- `bus.getMetrics()` returns a full `BusMetrics` snapshot
- Sliding-window P95 publish latency (1,000-sample ring buffer)
- Counters: publishCount, subscribeCount, unsubscribeCount, failedDeliveries, middlewareRejections, authorizationDenials, validationFailures, replayCount, requestCount, requestTimeouts
- Gauge: activeSubscriptions, historySize
- `TelemetryHooks` interface for OpenTelemetry / custom integration
- `TelemetryDispatcher` wraps all hooks in try-catch so telemetry bugs never crash the bus

**Built-in Middleware**
- `loggingMiddleware({ level })` — console logging
- `timestampMiddleware()` — records `processingStartedAt` in metadata
- `ttlGuardMiddleware()` — drops expired events early in pipeline
- `namespaceGuardMiddleware(expected)` — rejects cross-namespace envelopes
- `correlationMiddleware(getCorrelationId)` — propagates correlation IDs from async context

**Validator Adapters** (`ga-pubsub/validators`)
- `zodValidator(name, schema)` — adapts Zod schemas
- `jsonSchemaValidator(name, schema, ajv)` — adapts Ajv + JSON Schema
- `valibotValidator(name, schema)` — adapts Valibot schemas
- `customValidator(name, fn)` — wraps plain functions
- `andValidator(name, ...validators)` — all-must-pass composite
- `orValidator(name, ...validators)` — any-must-pass composite

**Framework Integrations** (`ga-pubsub/integrations`)
- `createReactHook(React)` — returns `useEventBus()` hook with auto-cleanup on unmount
- `createVueComposable(Vue)` — returns `useEventBus()` composable with `onUnmounted` cleanup
- `createExpressMiddleware(bus, options)` — attaches bus to `req.eventBus`, propagates correlation/tenant/user headers
- `createNestModule(options)` — NestJS dynamic module with `PUBSUB_BUS` injection token
- `withServerlessHandler(bus, handler)` — wraps serverless functions with lifecycle events
- `PUBSUB_BUS` injection token constant

**Error Types**
- `GAPubSubError` (base)
- `ValidationFailedError`
- `AuthorizationDeniedError`
- `RequestTimeoutError`
- `PayloadTooLargeError`
- `RateLimitExceededError`
- `TamperDetectedError`
- `ReplayAttackError`
- `SubscriptionLimitError`
- `MiddlewareAbortError`

---

### Transports

#### `@ga-pubsub/websocket` v1.0.0
- Client mode for browser and Node.js
- JWT authentication (token factory for rotation support)
- TLS/WSS support
- Heartbeat/pong with configurable interval and timeout
- Automatic reconnection with exponential backoff + jitter
- Per-message ACK with timeout
- Send queue for offline buffering during reconnect
- System events: `$system.connected`, `$system.disconnected`, `$system.reconnecting`, `$system.reconnected`
- Configurable max reconnect attempts (0 = unlimited)

#### `@ga-pubsub/sse` v1.0.0
- EventSource-based subscription (browser-native reconnect)
- HTTP POST publish channel with Bearer token auth
- `Last-Event-ID` header support for gap recovery
- `createSSEWriter(res)` server-side helper for Express/Fastify/Hono/Koa

#### `@ga-pubsub/broadcast-channel` v1.0.0
- Zero-network cross-tab / cross-worker event sharing
- Web BroadcastChannel API (Chrome 54+, Firefox 38+, Safari 15.4+)
- Same-origin policy enforced by browser
- Optional local echo (`echoLocal: true`)

#### `@ga-pubsub/redis` v1.0.0
- **Mode 1: Redis Pub/Sub** — at-most-once, ultra-low latency
- **Mode 2: Redis Streams** — at-least-once with consumer groups, persistent
- Consumer group support with ACK/NACK semantics
- Cluster and Sentinel support via ioredis
- TLS via ioredis options
- Configurable stream polling interval and batch size
- Auto-create consumer groups on first connection

#### `@ga-pubsub/kafka` v1.0.0
- Apache Kafka topic mapping (`event.name` → `ga-pubsub.event.name`)
- Consumer group load balancing
- Partition key defaults to `tenantId ?? correlationId` (preserves ordering in business flows)
- Configurable delivery: `at-most-once` (acks:0), `at-least-once` (acks:1), `best-effort` (acks:all + idempotent producer)
- Auto-create topics via Kafka Admin
- Configurable partition count and replication factor

#### `@ga-pubsub/nats` v1.0.0
- **NATS Core** — at-most-once, sub-millisecond latency
- **NATS JetStream** — persistent streams, durable consumers, at-least-once
- Perfect wildcard semantic match: `*` → `*`, `**` → `>`
- Queue groups for load-balanced worker pools
- Connection status watcher (disconnect, reconnect, error events)
- Auto stream creation in JetStream mode

#### `@ga-pubsub/rabbitmq` v1.0.0
- AMQP topic exchange mapping (GA-PubSub wildcards → AMQP routing keys)
- `*` → `*` (one AMQP word), `**` → `#` (zero or more words)
- Publisher confirms for at-least-once / best-effort delivery
- Dead-letter exchange (DLX) support for failed messages
- Per-message AMQP expiration from envelope TTL
- Channel prefetch for backpressure control
- Separate pub/sub channels (avoids head-of-line blocking)
- TLS via `amqps://` connection URL

---

### Testing

- Unit tests for all core modules
- Integration tests for end-to-end workflows
- Security tests for HMAC signing, replay attack prevention, prototype pollution, DOS protection
- Stress tests: 100k subscriptions, 10k sequential publishes, 100 concurrent RPC pairs
- Chaos tests: 50% subscriber crash rate, random unsubscribe storms
- `contractTestTransport(name, factory)` reusable transport conformance test suite
- Coverage target: >95% lines/functions, >90% branches

---

### Documentation

- Full `README.md` with usage, API reference, and integration examples
- `SECURITY.md` with vulnerability reporting process and security model
- `CONTRIBUTING.md` with development setup and contribution guidelines
- `docs/otel-integration.example.ts` — full OpenTelemetry instrumentation example

---

[1.0.0]: https://github.com/ga-pubsub/ga-pubsub/releases/tag/v1.0.0
