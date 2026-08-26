<div align="center">

# GA-PubSub Core

**A lightweight, zero-dependency pub/sub event bus for TypeScript — browser, Node.js, and edge runtimes.**

[![License: Elastic-2.0](https://img.shields.io/badge/License-Elastic%202.0-blue.svg)](./LICENSE)
[![TypeScript](https://img.shields.io/badge/TypeScript-5.x-blue.svg)](https://www.typescriptlang.org/)
[![ESM](https://img.shields.io/badge/module-ESM-orange.svg)]()

[Website](https://deslay-ai.web.app/ga-pubsub-docs/reference) · [Community & Support](https://deslay-ai.web.app/community) · [PRO Pricing](https://deslay-ai.web.app/ga-pubsub-docs/pricing)

</div>

---

## What is GA-PubSub?

GA-PubSub is a publish/subscribe event bus built for modern TypeScript projects. It works the same way in a React SPA, a Node.js microservice, a Cloudflare Worker, or a Deno edge function — no adapter needed.

The core package is available under the Elastic License 2.0 (ELv2). [PRO](https://deslay-ai.web.app/ga-pubsub-docs/pricing) adds HMAC signing, independent inbound/outbound rate limits, authorization, replay-attack prevention, multi-tenancy, and nine transport adapters.

---

## Features (free, ELv2)

- **Pub / Sub** — subscribe to exact event names or wildcard patterns (`order.*`, `payments.**`)
- **Priority ordering** — subscribers run highest-priority first within the same event
- **`subscribeOnce`** — auto-unsubscribes after the first delivery
- **Middleware pipeline** — intercept, enrich, or abort events before they reach subscribers
- **Schema validation** — register a validator per event name; invalid payloads are rejected
- **Request / Response (RPC)** — `bus.request()` + `bus.respond()` with configurable timeout
- **Replay engine** — late subscribers receive previously published events (ring-buffer, wildcard-aware)
- **TTL enforcement** — expired local, replayed, and transported envelopes are dropped before subscriber delivery
- **Metrics** — built-in counters for publishes, deliveries, failures, auth denials, and p95 latency
- **Subscription limit** — optional cap to prevent runaway listeners
- **Cancel in-flight requests** — `handle.cancel()` rejects the response promise immediately
- **Zero dependencies** — pure TypeScript, ships as ESM + CJS dual build

---

## Installation

```bash
npm install ga-pubsub
```

---

## Quick Start

```typescript
import { EventBus } from 'ga-pubsub';

const bus = new EventBus({ namespace: 'my-app' });

// Subscribe
bus.subscribe('user.created', (envelope) => {
  console.log('New user:', envelope.payload);
});

// Publish
await bus.publish('user.created', { id: '42', name: 'Ajith' });

// Wildcard — matches user.created, user.updated, user.deleted …
bus.subscribe('user.*', (e) => console.log(e.event, e.payload));

// One-shot
bus.subscribeOnce('app.ready', () => console.log('ready!'));
```

---

## Middleware

```typescript
import { EventBus, loggingMiddleware, ttlGuardMiddleware } from 'ga-pubsub';

const bus = new EventBus();

// Built-in middleware
bus.use(loggingMiddleware({ level: 'debug' }));
bus.use(ttlGuardMiddleware());

// Custom middleware — enrich every envelope
bus.use(async (envelope, next) => {
  envelope.payload = { ...envelope.payload as object, enrichedAt: Date.now() };
  await next();
});
```

---

## Schema Validation

```typescript
bus.registerSchema('order.placed', {
  name: 'OrderSchema',
  validate: (payload) => {
    const p = payload as { orderId?: unknown; total?: unknown };
    if (typeof p.orderId !== 'string') return { valid: false, errors: [{ path: 'orderId', message: 'required string' }] };
    if (typeof p.total !== 'number')   return { valid: false, errors: [{ path: 'total',   message: 'required number' }] };
    return { valid: true };
  },
});

// Invalid payload → throws ValidationFailedError
await bus.publish('order.placed', { orderId: 123 });
```

---

## Request / Response (RPC)

```typescript
// Responder
bus.respond('math.add', (e) => {
  const { a, b } = e.payload as { a: number; b: number };
  return { result: a + b };
});

// Requester
const { response } = bus.request('math.add', { a: 3, b: 7 }, { timeoutMs: 2000 });
const { payload } = await response;
console.log(payload.result); // 10
```

---

## Replay

```typescript
const bus = new EventBus({ replay: { limit: 100, replayWildcards: true, maxEventTypes: 1000 } });

await bus.publish('app.started', { version: '2.0' });

// Late subscriber still receives the event
bus.subscribe('app.*', (e) => {
  console.log('replayed:', e.event);
});
```

---

## API Reference

| Method | Description |
|--------|-------------|
| `bus.publish(event, payload, opts?)` | Publish an event; runs middleware then notifies subscribers |
| `bus.subscribe(pattern, handler, opts?)` | Subscribe; returns `{ unsubscribe() }` |
| `bus.subscribeOnce(pattern, handler, opts?)` | Subscribe for one delivery only |
| `bus.use(middleware)` | Add a middleware function to the pipeline |
| `bus.registerSchema(event, validator)` | Register a payload validator |
| `bus.request(event, payload, opts?)` | Send an RPC request; returns `{ response, cancel }` |
| `bus.respond(event, handler)` | Register an RPC responder |
| `bus.getMetrics()` | Return current metrics snapshot |
| `bus.destroy()` | Tear down all subscriptions and clear replay history |

---

## Error Classes

| Class | Code | When thrown |
|-------|------|-------------|
| `GAPubSubError` | base | Base class for all errors |
| `ValidationFailedError` | `VALIDATION_FAILED` | Schema check fails |
| `RequestTimeoutError` | `REQUEST_TIMEOUT` | No responder within `timeoutMs` |
| `SubscriptionLimitError` | `SUBSCRIPTION_LIMIT` | `maxSubscriptions` reached |
| `MiddlewareAbortError` | `MIDDLEWARE_ABORT` | Middleware throws to stop delivery |

---

## PRO Edition

The free core is intentionally lean. For licensed security and external transports, see [PRO pricing](https://deslay-ai.web.app/ga-pubsub-docs/pricing):

| Feature | Core | PRO |
|---------|:----:|:---:|
| Pub / Sub, wildcards, priority | ✅ | ✅ |
| Middleware, schema validation | ✅ | ✅ |
| Replay engine, RPC, metrics | ✅ | ✅ |
| HMAC envelope signing | — | ✅ |
| Rate limiting (token bucket) | — | ✅ |
| Authorization (`bus.authorize`) | — | ✅ |
| Replay-attack prevention | — | ✅ |
| Payload size enforcement | — | ✅ |
| Multi-tenant namespace registry | — | ✅ |
| Scoped bus (auto-prefix) | — | ✅ |
| 9 adapters (HTTP, WebSocket, SSE, Socket.IO, BroadcastChannel, Redis, Kafka, NATS, RabbitMQ) | — | ✅ |
| Runtime license key validation | — | ✅ |

→ **[View PRO pricing & details](https://deslay-ai.web.app/ga-pubsub-docs/pricing)**
→ **[PRO customer support](https://deslay-ai.web.app/ga-pubsub-docs/services)**

---

## Contributing & Community

Questions, ideas, and bug reports are welcome.

→ **[Join the community](https://deslay-ai.web.app/community)**

If you find a bug, open an issue on GitHub. For security disclosures, contact the author directly through the community page.

---

## Copy-paste local example

The published package includes [`examples/usage.ts`](./examples/usage.ts). It covers typed publish/subscribe, structural wildcards, priority, one-time subscriptions, middleware, schema validation, replay, TTL, request/response, telemetry, metrics, and cleanup.

```bash
mkdir ga-pubsub-training && cd ga-pubsub-training
npm init -y
npm install ga-pubsub@3.1.0 tsx
```

Copy the example to `usage.ts`, then run:

```bash
npx tsx usage.ts
```

Core does not require a license, broker, or runtime dependency.

---

## License

**Elastic License 2.0 (ELv2)** — © 2026 Ajithraj G
