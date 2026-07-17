# GA-PubSub

> **Enterprise-grade, security-first, transport-agnostic eventing platform for JavaScript and TypeScript ecosystems.**

GA-PubSub combines the simplicity of frontend pub/sub with the extensibility of enterprise messaging platforms — without the operational complexity.

```
Browser ──┐
React   ──┤
Angular ──┤               ┌──────────────────────────┐
Vue     ──┼──► bus.publish │                          │──► Memory (default)
Node.js ──┤   bus.subscribe│     GA-PubSub Core       │──► WebSocket
Express ──┤   bus.request  │   EventBus + Middleware  │──► SSE
NestJS  ──┤   bus.respond  │   Replay + Auth + Schema │──► BroadcastChannel
Serverless┤               │   Security + Metrics     │──► Redis
Edge    ──┘               └──────────────────────────┘──► Kafka / NATS
```

---

## Packages

| Package | Description | Runtime |
|---------|-------------|---------|
| [`ga-pubsub`](./packages/core) | Core engine — zero runtime deps | Browser + Node.js + Edge |
| [`@ga-pubsub/websocket`](./packages/websocket) | WebSocket transport (client + server) | Browser + Node.js |
| [`@ga-pubsub/sse`](./packages/sse) | Server-Sent Events transport | Browser + Node.js |
| [`@ga-pubsub/broadcast-channel`](./packages/broadcast-channel) | Cross-tab BroadcastChannel transport | Browser |
| [`@ga-pubsub/redis`](./packages/redis) | Redis Pub/Sub + Streams transport | Node.js |
| [`@ga-pubsub/kafka`](./packages/kafka) | Apache Kafka transport | Node.js |
| [`@ga-pubsub/nats`](./packages/nats) | NATS Core + JetStream transport | Node.js |

---

## Quick Start

```bash
npm install ga-pubsub
```

```typescript
import { getBus } from 'ga-pubsub';

const bus = getBus('myapp', {
  replay: { limit: 10, ttl: 60_000 },
  maxPayloadBytes: 65_536,
  enableSigning: true,
  signingSecret: process.env.PUBSUB_SECRET,
});

// Subscribe
bus.subscribe('user.*', (envelope) => {
  console.log(`User event: ${envelope.event}`, envelope.payload);
});

// Publish
await bus.publish('user.created', {
  id: 'usr_123',
  email: 'alice@example.com',
});
```

---

## Core Concepts

### Event Envelope

Every event is wrapped in a standardized, signed envelope:

```typescript
{
  id:            "a1b2c3d4-...",     // Unique event ID (UUIDv4)
  event:         "payments.invoice.created",
  namespace:     "tenant-a",
  payload:       { invoiceId: "inv_123", amount: 999 },
  timestamp:     1700000000000,
  correlationId: "f9e8d7c6-...",    // Business flow ID
  causationId:   "e1d2c3b4-...",    // Parent event ID
  source:        "billing-service",
  version:       "1",
  tenantId:      "tenant-a",
  signature:     "a3f9b2...",        // HMAC-SHA256 (when signing enabled)
  sequence:      42,                 // Monotonic (replay attack prevention)
}
```

### Wildcard Subscriptions

```typescript
bus.subscribe('user.*',    handler); // user.created, user.deleted — ONE segment
bus.subscribe('payments.**', handler); // payments.invoice.created.retry — ANY depth
bus.subscribe('**',        handler); // ALL events in namespace
```

### Middleware Pipeline

```typescript
// Middlewares run in order before subscriber dispatch
bus.use(ttlGuardMiddleware());                     // Built-in: drop expired events
bus.use(loggingMiddleware({ level: 'debug' }));    // Built-in: log all events
bus.use(timestampMiddleware());                    // Built-in: record processing time

// Custom middleware
bus.use(async (envelope, next) => {
  envelope.metadata = { ...envelope.metadata, region: 'us-east-1' };
  await next(); // Call next to continue; omit to abort
});
```

### Schema Validation

```typescript
import { zodValidator } from 'ga-pubsub/validators';
import { z } from 'zod';

const UserCreatedSchema = z.object({
  id:    z.string().uuid(),
  email: z.string().email(),
  role:  z.enum(['admin', 'user', 'guest']),
});

bus.registerSchema('user.created', zodValidator('UserCreated', UserCreatedSchema));

// Validation failures throw ValidationFailedError BEFORE dispatch
await bus.publish('user.created', { id: 'not-a-uuid', email: 'bad' });
// throws: ValidationFailedError: [invalid_string] Invalid uuid, [invalid_string] Invalid email
```

### Authorization

```typescript
bus.authorize('payment.**', async (envelope, ctx) => {
  // ctx is populated from SubscriberOptions.authContext or middleware
  return ctx.roles?.includes('finance') ?? false;
});

// Unauthorized publish throws AuthorizationDeniedError
await bus.publish('payment.processed', { amount: 1000 });
```

### Request / Response (RPC)

```typescript
// Responder (service side)
bus.respond<{ userId: string }, { name: string }>('user.fetch', async (envelope) => {
  const user = await db.users.findById(envelope.payload.userId);
  return { name: user.name };
});

// Requester (caller side)
const { response } = bus.request<{ userId: string }, { name: string }>(
  'user.fetch',
  { userId: 'usr_123' },
  { timeoutMs: 5_000 }
);

const envelope = await response;
console.log(envelope.payload.name); // "Alice"
```

### Replay

```typescript
const bus = getBus('app', {
  replay: { limit: 50, ttl: 300_000 } // Keep last 50 events for 5 minutes
});

await bus.publish('app.config.loaded', { version: '2.1.0' });

// Late subscriber immediately receives the stored event
bus.subscribe('app.config.loaded', (e) => {
  initializeApp(e.payload.version);
});

// With time filter
bus.subscribe('audit.log', callback, {
  replay: true,
  replayLastMs: 30 * 60 * 1000, // only last 30 minutes
});
```

---

## Security

### Signing & Verification

```typescript
const bus = getBus('secure', {
  enableSigning:    true,
  verifySignatures: true,
  signingSecret:    process.env.SIGNING_SECRET, // NEVER log or expose
});

// All published envelopes are HMAC-SHA256 signed
// Inbound envelopes with invalid/missing signatures are silently dropped
```

### Rate Limiting

```typescript
const bus = getBus('api', {
  rateLimit: 100, // max 100 events/sec per source
});
```

### Payload Size Limits

```typescript
const bus = getBus('untrusted', {
  maxPayloadBytes: 16_384, // 16 KB maximum
});
// Exceeding throws PayloadTooLargeError before middleware runs
```

### Subscription Limits (DOS protection)

```typescript
const bus = getBus('public', {
  maxSubscriptions: 1_000,
});
// Exceeding throws SubscriptionLimitError
```

### Replay Attack Prevention

```typescript
const bus = getBus('realtime', {
  enableReplayPrevention: true,
  // Monotonically increasing sequence numbers are enforced
  // Out-of-order or repeated sequences throw ReplayAttackError
});
```

### Namespace Isolation

```typescript
const tenantA = getBus('tenant-a');
const tenantB = getBus('tenant-b');

// Cross-tenant publish is structurally impossible —
// tenantA and tenantB are completely independent EventBus instances
await tenantA.publish('order.created', payload);
// tenantB subscribers NEVER receive this event
```

---

## Transport Adapters

### WebSocket

```typescript
import { WebSocketTransport } from '@ga-pubsub/websocket';

const bus = getBus('app', {
  transport: new WebSocketTransport({
    url:                 'wss://events.example.com/ws',
    token:               () => auth.getAccessToken(),
    heartbeatInterval:   30_000,
    maxReconnectAttempts: 0,       // unlimited
    requireAck:          true,
  })
});
```

### Redis

```typescript
import Redis from 'ioredis';
import { RedisTransport } from '@ga-pubsub/redis';

const bus = getBus('app', {
  transport: new RedisTransport({
    client:        new Redis({ host: 'redis.internal', tls: {} }),
    keyPrefix:     'myapp',
    enableStreams:  true,           // persistent
    consumerGroup: 'payment-svc',
    delivery:      'at-least-once',
  })
});
```

### Kafka

```typescript
import { Kafka } from 'kafkajs';
import { KafkaTransport } from '@ga-pubsub/kafka';

const bus = getBus('app', {
  transport: new KafkaTransport({
    kafka:         new Kafka({ brokers: ['kafka:9092'] }),
    consumerGroup: 'analytics-service',
    topics:        ['payments.**', 'users.*'],
    delivery:      'best-effort', // idempotent producer, all replicas ACK
  })
});
```

### NATS

```typescript
import { connect } from 'nats';
import { NATSTransport } from '@ga-pubsub/nats';

const nc = await connect({ servers: 'nats://events.internal:4222' });
const bus = getBus('app', {
  transport: new NATSTransport({
    nc,
    enableJetStream: true,
    stream:          'GA_PUBSUB',
    queueGroup:      'notification-workers',
  })
});
```

---

## Framework Integrations

### React

```tsx
import { createReactHook } from 'ga-pubsub/integrations';
import { getBus } from 'ga-pubsub';
import React from 'react';

const bus = getBus('app');
const useEventBus = createReactHook(React);

function CartBadge() {
  const { lastEnvelope } = useEventBus<{ items: number }>(bus, 'cart.updated');
  return <span>{lastEnvelope?.payload.items ?? 0}</span>;
}

function CheckoutButton() {
  const { publish } = useEventBus(bus, 'checkout.initiated');
  return <button onClick={() => publish({ source: 'hero' })}>Checkout</button>;
}
```

### Vue 3

```ts
import { createVueComposable } from 'ga-pubsub/integrations';
import { ref, readonly, onUnmounted } from 'vue';
import { getBus } from 'ga-pubsub';

const bus = getBus('app');
const useEventBus = createVueComposable({ ref, readonly, onUnmounted });

// In component setup()
const { lastEnvelope, publish } = useEventBus<{ count: number }>(bus, 'counter.updated');
```

### Express

```typescript
import express from 'express';
import { createExpressMiddleware } from 'ga-pubsub/integrations';
import { getBus } from 'ga-pubsub';

const bus = getBus('api');
const app = express();
app.use(createExpressMiddleware(bus));

app.post('/orders', async (req, res) => {
  // req.eventBus auto-propagates correlation/tenant/user from headers
  await req.eventBus.publish('order.created', req.body);
  res.status(201).json({ ok: true });
});
```

### NestJS

```typescript
// app.module.ts
import { Module } from '@nestjs/common';
import { createNestModule, PUBSUB_BUS } from 'ga-pubsub/integrations';

@Module({
  imports: [
    createNestModule({
      namespace:     'myapp',
      enableSigning: true,
      signingSecret: process.env.PUBSUB_SECRET,
    })
  ]
})
export class AppModule {}

// payment.service.ts
import { Injectable, Inject } from '@nestjs/common';
import { PUBSUB_BUS, EventBus } from 'ga-pubsub/integrations';

@Injectable()
export class PaymentService {
  constructor(@Inject(PUBSUB_BUS) private readonly bus: EventBus) {}

  async processPayment(orderId: string, amount: number) {
    await this.bus.publish('payment.processed', { orderId, amount });
  }
}
```

---

## Observability

### Built-in Metrics

```typescript
const metrics = bus.getMetrics();
// {
//   publishCount:          12_450,
//   subscribeCount:        38,
//   failedDeliveries:      2,
//   middlewareRejections:  0,
//   authorizationDenials:  0,
//   validationFailures:    7,
//   replayCount:           150,
//   requestCount:          890,
//   requestTimeouts:       1,
//   p95LatencyMs:          0.42,
//   activeSubscriptions:   38,
//   historySize:           245,
// }
```

### OpenTelemetry Integration

```typescript
import { metrics as otelMetrics, trace } from '@opentelemetry/api';

const meter   = otelMetrics.getMeter('ga-pubsub');
const tracer  = trace.getTracer('ga-pubsub');
const pubCount = meter.createCounter('ga_pubsub_publish_total');
const latency  = meter.createHistogram('ga_pubsub_publish_latency_ms');

const bus = getBus('app', {
  telemetry: {
    onPublish(envelope, latencyMs) {
      pubCount.add(1, { event: envelope.event, namespace: envelope.namespace });
      latency.record(latencyMs, { event: envelope.event });
    },
    onError(error, ctx) {
      console.error(`[${ctx.phase}]`, error.message, ctx.eventName);
    },
    onValidationFailure(eventName, errors) {
      meter.createCounter('ga_pubsub_validation_failures').add(1, { event: eventName });
    },
  }
});
```

---

## Multi-Tenant Architecture

```typescript
import { getBus, destroyNamespace, listNamespaces } from 'ga-pubsub';

// Each tenant gets a completely isolated bus
function getTenantBus(tenantId: string) {
  return getBus(`tenant-${tenantId}`, {
    tenantId,
    maxPayloadBytes:  65_536,
    maxSubscriptions: 10_000,
    rateLimit:        500,
  });
}

// Tenant onboarding
const busTenantA = getTenantBus('acme-corp');
await busTenantA.publish('tenant.initialized', { plan: 'enterprise' });

// Tenant offboarding — destroys bus + clears subscriptions + disconnects transport
await destroyNamespace('tenant-acme-corp');

// Audit all active namespaces
console.log(listNamespaces()); // ['default', 'tenant-acme-corp', 'analytics']
```

---

## Performance Targets

| Metric | Target | Implementation |
|--------|--------|----------------|
| Exact subscription lookup | O(1) | Hash map index |
| Wildcard matching | O(k) per pattern | Pre-compiled trie + LRU cache |
| Memory per subscription | ~200 bytes | Minimal record structure |
| Max subscriptions | 100,000+ | Verified by stress test |
| Throughput (in-memory) | 1M+ events/day | Sequential async dispatch |
| P95 publish latency | <1ms | Ring-buffer sampled |
| Startup overhead | None | Lazy initialization |

---

## Security Checklist

- [x] HMAC-SHA256 envelope signing
- [x] Signature verification (constant-time comparison)
- [x] Replay attack prevention (monotonic sequence tracking)
- [x] Rate limiting (token bucket per source)
- [x] Payload size enforcement
- [x] Event TTL expiration
- [x] Prototype pollution prevention (structured clone / JSON round-trip)
- [x] Namespace isolation (structurally separate bus instances)
- [x] Authorization layer (per-event authorizer functions)
- [x] Schema validation (Zod / JSON Schema / custom)
- [x] No unsafe defaults (signing off by default — must be explicitly enabled)
- [x] LRU-bounded pattern cache (prevents memory exhaustion from adversarial patterns)
- [x] Bounded rate-limit source tracking (prevents memory exhaustion from source flooding)
- [x] No sensitive data logged (secrets, signatures excluded from all log output)

---

## Testing

```bash
# Unit + integration tests
npm test

# With coverage (target: >95%)
npm run test:coverage

# Stress tests (100k subscriptions, 10k sequential publishes)
npm run test:stress

# Security-focused tests
npm run test:security
```

---

## License

MIT © GA-PubSub Contributors
