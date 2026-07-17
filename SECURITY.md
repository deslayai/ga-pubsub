# Security Policy

## Supported Versions

| Version | Supported          |
|---------|--------------------|
| 1.x     | ✅ Active support   |
| < 1.0   | ❌ Not supported    |

---

## Reporting a Vulnerability

**Please do NOT open a public GitHub issue for security vulnerabilities.**

Send a detailed report to: **security@ga-pubsub.dev**

Include:
- Description of the vulnerability
- Steps to reproduce (proof-of-concept code if possible)
- Impact assessment (what can an attacker achieve?)
- Suggested remediation (if any)

You will receive an acknowledgment within **48 hours** and a full response within **7 days**.

We follow coordinated disclosure — we ask that you give us **90 days** before public disclosure to issue a fix and release.

---

## Security Model

### Defense in Depth

GA-PubSub implements security at every layer, not as an afterthought:

```
┌─────────────────────────────────────────────────────────────┐
│ Layer 1 — Payload Size Enforcement (before any work)        │
│   Rejects oversized payloads before middleware runs         │
├─────────────────────────────────────────────────────────────┤
│ Layer 2 — Rate Limiting (token bucket per source)           │
│   Prevents event flooding and resource exhaustion           │
├─────────────────────────────────────────────────────────────┤
│ Layer 3 — Payload Sanitization                              │
│   Deep-clones via structuredClone; strips __proto__         │
├─────────────────────────────────────────────────────────────┤
│ Layer 4 — HMAC-SHA256 Envelope Signing                      │
│   Detects payload tampering and spoofed envelopes           │
├─────────────────────────────────────────────────────────────┤
│ Layer 5 — Replay Attack Prevention                          │
│   Monotonic sequence numbers per (namespace, source)        │
├─────────────────────────────────────────────────────────────┤
│ Layer 6 — Schema Validation                                 │
│   Validates payload structure before dispatch               │
├─────────────────────────────────────────────────────────────┤
│ Layer 7 — Authorization                                     │
│   Per-event authorizer functions; async-capable             │
└─────────────────────────────────────────────────────────────┘
```

### Namespace Isolation (Multi-Tenant)

Each namespace (`getBus('tenant-a')`) gets a completely independent
`EventBus` instance with no shared state. Cross-namespace event leakage
is **structurally impossible** — there is no pub/sub routing between
namespaces at the code level.

### Signing and Verification

- Signatures use **HMAC-SHA256** via `crypto.subtle` (Web Crypto API)
- Verification uses **constant-time comparison** to prevent timing oracle attacks
- The signing secret is never stored in envelopes, logs, or metadata
- Signature covers: `id, event, namespace, payload, timestamp, sequence, source, version, tenantId, userId, ttl, causationId, correlationId`
- **Metadata is intentionally excluded** from the signature — it is informational only

### Secrets Handling

- Signing secrets are passed as constructor options and kept in closure scope
- Secrets are never serialized into envelopes
- Secrets are never included in error messages or log output
- Key derivation uses `crypto.subtle.importKey` — keys are not stored as raw bytes after import

### Prototype Pollution Prevention

All payloads are deep-cloned before processing:
1. **Preferred:** `structuredClone()` (Node.js 17+ / modern browsers)
2. **Fallback:** JSON round-trip (`JSON.parse(JSON.stringify(...))`)

Both approaches strip `__proto__` and `constructor.prototype` keys,
preventing prototype pollution attacks.

### LRU Bounds on All Maps

Every bounded map structure has an eviction policy to prevent memory
exhaustion from adversarial input:

| Structure | Max entries | Eviction |
|-----------|-------------|----------|
| Pattern compilation cache | 4,096 patterns | LRU (oldest first) |
| Rate limiter buckets | 50,000 sources | LRU (oldest first) |
| Sequence tracker | 10,000 sources | LRU (oldest first) |

### Error Isolation

- Middleware errors are caught — the bus does not crash
- Subscriber errors are caught individually — one crashing subscriber does not affect others
- `onError` hook crashes are caught — the hook cannot bring down the bus
- Replay errors are caught per-envelope — one bad replay does not block others

---

## Known Limitations

### In-Memory Transport

The default in-memory transport provides **no persistence**. Events are
lost if the process restarts. For production systems requiring durability,
use the Redis Streams, Kafka, NATS JetStream, or RabbitMQ transports.

### Signing is Opt-In

`enableSigning` defaults to `false`. For untrusted transports (WebSocket,
SSE, Redis Pub/Sub) you should **explicitly enable signing and verification**:

```typescript
const bus = new EventBus({
  enableSigning:    true,
  verifySignatures: true,
  signingSecret:    process.env.PUBSUB_SECRET, // never hardcode
});
```

### Wildcard Pattern Cache

The LRU cache for compiled wildcard patterns is bounded to 4,096 entries.
In adversarial environments where patterns are user-controlled, consider
validating and allowlisting patterns before they reach the bus.

### No Built-In TLS for In-Process Buses

The in-memory transport does not need TLS (no network). For transports
that cross network boundaries (WebSocket, Redis, Kafka, NATS, RabbitMQ),
**always use TLS** (wss://, rediss://, TLS broker config).

---

## Dependency Security

The `ga-pubsub` core package has **zero runtime dependencies**.
Transport packages use peer dependencies, so you control the version
of `ioredis`, `kafkajs`, `nats`, `amqplib`, etc.

Run `npm audit` regularly for transport peer dependencies.

---

## Changelog

Security fixes are marked with `[SECURITY]` in `CHANGELOG.md` and
announced in the GitHub Security Advisories tab.
