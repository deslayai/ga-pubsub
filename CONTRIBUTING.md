# Contributing to GA-PubSub

Thank you for your interest in contributing. This guide covers everything you need to go from zero to a merged pull request.

---

## Table of Contents

1. [Development setup](#development-setup)
2. [Project structure](#project-structure)
3. [Architecture decisions](#architecture-decisions)
4. [Writing code](#writing-code)
5. [Writing tests](#writing-tests)
6. [Adding a transport](#adding-a-transport)
7. [Pull request process](#pull-request-process)
8. [Release process](#release-process)

---

## Development Setup

### Prerequisites

- Node.js ≥ 18.0.0 (Web Crypto API required)
- npm ≥ 9.0.0

### First-time setup

```bash
# Clone
git clone https://github.com/ga-pubsub/ga-pubsub.git
cd ga-pubsub

# Install all workspace dependencies
npm install

# Build all packages
npm run build

# Verify everything passes
npm test
```

### Daily development loop

```bash
# Watch mode for the core package
cd packages/core && npm run dev

# Run tests in watch mode
npm run test:watch

# Run a single test file
npx vitest run tests/unit/bus.test.ts

# Run tests matching a pattern
npx vitest run -t "wildcard"

# Full coverage report
npm run test:coverage
# → opens coverage/index.html
```

### Environment for transport integration tests

Integration tests for Redis, Kafka, NATS, and RabbitMQ require running
broker instances. The easiest way is Docker Compose:

```bash
# Start all brokers
docker compose -f tests/docker-compose.yml up -d

# Run integration tests against live brokers
npx vitest run tests/integration/transports/

# Stop brokers
docker compose -f tests/docker-compose.yml down
```

The `tests/docker-compose.yml` starts:
- Redis 7 on port 6379
- Kafka + Zookeeper on ports 9092/2181
- NATS 2.x with JetStream on port 4222
- RabbitMQ 3.x management on port 5672/15672

---

## Project Structure

```
ga-pubsub/
├── packages/
│   ├── core/                    # ga-pubsub — zero runtime deps
│   │   └── src/
│   │       ├── types.ts         # All TypeScript interfaces and error types
│   │       ├── bus.ts           # EventBus — central engine
│   │       ├── wildcard.ts      # Pattern matching engine + SubscriptionIndex
│   │       ├── security.ts      # HMAC, rate limiting, sanitization, IDs
│   │       ├── replay.ts        # Replay engine — ring buffer + TTL
│   │       ├── metrics.ts       # MetricsCollector + TelemetryDispatcher
│   │       ├── validators.ts    # Zod/AJV/Valibot/custom validator adapters
│   │       ├── integrations.ts  # React/Vue/Express/NestJS/Serverless adapters
│   │       ├── registry.ts      # Namespace registry + ScopedBus
│   │       └── index.ts         # Public API surface
│   │
│   ├── websocket/src/index.ts   # @ga-pubsub/websocket
│   ├── sse/src/index.ts         # @ga-pubsub/sse
│   ├── broadcast-channel/       # @ga-pubsub/broadcast-channel
│   ├── redis/src/index.ts       # @ga-pubsub/redis
│   ├── kafka/src/index.ts       # @ga-pubsub/kafka
│   ├── nats/src/index.ts        # @ga-pubsub/nats
│   └── rabbitmq/src/index.ts    # @ga-pubsub/rabbitmq
│
├── tests/
│   ├── setup.ts                 # Vitest global setup (Web Crypto polyfill)
│   ├── unit/bus.test.ts         # Unit tests
│   ├── integration/             # Integration and E2E tests
│   ├── security/                # Security-specific tests
│   └── stress/                  # Stress and chaos tests
│
├── docs/
│   └── otel-integration.example.ts  # OpenTelemetry example
│
├── vitest.config.ts
├── package.json                 # Monorepo root
├── README.md
├── SECURITY.md
├── CHANGELOG.md
└── CONTRIBUTING.md              # This file
```

---

## Architecture Decisions

Understanding *why* things are the way they are saves a lot of time.

### Why zero runtime dependencies in the core?

The core package must run in all environments: browsers, Node.js, Deno,
Cloudflare Workers, edge runtimes, and environments with strict bundle
size requirements. Every runtime dependency narrows that target.

The core deliberately uses only:
- Web Crypto API (`crypto.subtle`) — available in all modern environments
- Standard `Map`, `Set`, `Promise`, `TextEncoder` — universally available

### Why are publish and subscribe purely async?

Subscribers can perform async work (DB writes, HTTP calls, etc.). Making
the dispatch loop `await` each subscriber sequentially ensures:
1. Priority ordering is respected end-to-end (not just at the start)
2. Subscriber errors are caught per-subscriber
3. Back-pressure naturally applies when subscribers are slow

For fire-and-forget use cases, subscribers can choose not to await their
own async work.

### Why is the SubscriptionIndex separate from the EventBus?

Separation of concerns. The `SubscriptionIndex` is a pure data structure
(trie + hash map) with no knowledge of middleware, replay, or metrics.
This makes it independently testable and replaceable.

### Why does signing exclude metadata?

Metadata is designed for operational enrichment — middleware can add
`region`, `processingStartedAt`, `pipeline-version`, etc. without
breaking the signature. Security-critical fields (event name, payload,
namespace, timestamp) are all signed.

### Why token bucket for rate limiting instead of fixed window?

Token bucket avoids the "thundering herd" problem of fixed-window
counters where traffic can spike at window boundaries. It also handles
burst traffic more gracefully — a brief burst is allowed up to the
configured burst capacity, then smoothed.

### Why separate pub and sub channels in RabbitMQ?

RabbitMQ's AMQP protocol can exhibit head-of-line blocking when a slow
consumer on a channel holds up a fast publisher using the same channel.
Separate channels eliminate this contention entirely.

### Why does getHistory() perform lazy eviction?

Eager eviction on a timer alone would mean a TTL-expired event could be
replayed in the window between sweeps. Lazy eviction on read ensures
expired events are never returned, regardless of sweep timing.

---

## Writing Code

### TypeScript standards

- **Strict mode** — all compiler strict flags are on, including `exactOptionalPropertyTypes`
- **No `any`** — use `unknown` when the type is genuinely unknown
- **No `as` casts** — when necessary, cast through `unknown` and add a comment
- **Prefer `readonly`** on object fields that should not be mutated
- **Document public APIs** with JSDoc comments

### Security-first mindset

Before adding any feature that handles user-provided data:

1. Can this cause a DoS? (unbounded loop, infinite recursion, memory growth)
2. Can this cause information leakage? (error messages, logs)
3. Can this be used for injection? (command, prototype, path traversal)
4. Does this require a new bound? (add it to the LRU/max tracking)

### No silent failures in the security path

The security layer **must** throw on violations — never silently drop
or log-and-continue. Silent failures would allow attackers to probe
behavior without triggering alerts.

### Module boundaries

- `bus.ts` may import from `wildcard.ts`, `security.ts`, `replay.ts`, `metrics.ts`
- `bus.ts` must **not** import from `validators.ts`, `integrations.ts`, or `registry.ts`
- `security.ts` must **not** import from any other internal module
- Transport adapters must **not** import from `bus.ts` — only from `types.ts`

---

## Writing Tests

### Coverage requirements

Every PR must maintain >95% line/function coverage and >90% branch coverage.
Run `npm run test:coverage` to check before submitting.

### Test categories

| Category | Location | What to test |
|----------|----------|--------------|
| Unit | `tests/unit/` | Each module in isolation, mocking dependencies |
| Integration | `tests/integration/` | Multiple modules working together, end-to-end workflows |
| Security | `tests/security/` | All security properties — signing, replay attack, tamper detection, DOS |
| Stress | `tests/stress/` | Scale (100k subs), throughput (10k events), chaos (crash storms) |

### Test naming convention

```typescript
describe('ComponentName — feature category', () => {
  it('specific behaviour being tested', async () => {
    // Arrange
    // Act
    // Assert
  });
});
```

### Security test pattern

For every security mechanism, test both directions:

```typescript
it('correctly accepts valid input', () => { ... });
it('correctly rejects invalid input with the right error type', () => { ... });
it('error carries the correct code and relevant context', () => { ... });
```

### Stress test guidelines

- Stress tests must pass with a generous timeout (30-60s)
- Print throughput stats with `console.log` so CI logs are informative
- Test both registration scale AND routing correctness at scale
- Check memory: `bus.getMetrics().activeSubscriptions === 0` after teardown

---

## Adding a Transport

To add a new transport (e.g. `@ga-pubsub/mqtt`):

### 1. Create the package

```bash
mkdir -p packages/mqtt/src
```

### 2. Implement the `TransportAdapter` interface

```typescript
// packages/mqtt/src/index.ts
import type { TransportAdapter, EventEnvelope } from 'ga-pubsub';

export class MQTTTransport implements TransportAdapter {
  readonly name = 'mqtt';

  async connect(): Promise<void> { ... }
  async disconnect(): Promise<void> { ... }
  async publish(envelope: EventEnvelope): Promise<void> { ... }
  async subscribe(pattern: string, onMessage: (e: EventEnvelope) => void): Promise<() => void> { ... }
  async ping(): Promise<boolean> { ... }
  on(event: string, handler: (...args: unknown[]) => void): void { ... }
  off(event: string, handler: (...args: unknown[]) => void): void { ... }
}
```

### 3. Map wildcards

Document how your transport's pattern syntax maps to GA-PubSub wildcards:

| GA-PubSub | MQTT | AMQP | NATS |
|-----------|------|------|------|
| `*`       | `+`  | `*`  | `*`  |
| `**`      | `#`  | `#`  | `>`  |

### 4. Add `package.json`

Use the existing transport `package.json` files as a template.
Set `ga-pubsub` as a peer dependency.

### 5. Write contract tests

```typescript
import { contractTestTransport } from '../../tests/unit/bus.test.js';
import { MQTTTransport } from '../../packages/mqtt/src/index.js';

contractTestTransport('mqtt', async () => {
  const transport = new MQTTTransport({ url: 'mqtt://localhost:1883' });
  return transport;
});
```

### 6. Add to monorepo

Add the package to `workspaces` in the root `package.json` and
update the transport comparison table in `README.md`.

### 7. Document delivery guarantee

Clearly document what delivery guarantee the transport provides:
- `at-most-once` — fire and forget
- `at-least-once` — acknowledgment required
- `best-effort` — strongest available (idempotent, all-replica ACK, etc.)

---

## Pull Request Process

### Before opening a PR

- [ ] All tests pass: `npm test`
- [ ] Coverage is ≥95%: `npm run test:coverage`
- [ ] TypeScript compiles with zero errors: `npm run typecheck`
- [ ] Lint passes: `npm run lint`
- [ ] `CHANGELOG.md` updated under `[Unreleased]`
- [ ] New public APIs have JSDoc comments
- [ ] Security implications documented if applicable

### PR title convention

```
feat(core): add event batching API
fix(redis): reconnect on stream consumer group not found
security(core): bound sequence tracker to prevent OOM
docs(websocket): add JWT rotation example
test(kafka): add contract test for partition key assignment
chore(deps): update ioredis to 5.3.2
```

### PR size

Keep PRs focused. A PR that adds a feature, fixes a bug, updates docs,
and refactors internals is hard to review. Split it.

Large PRs that cannot be split (e.g. a new transport package) should
include a detailed description of the architecture and key decisions.

### Review criteria

All PRs are reviewed for:
1. **Correctness** — does it do what it says?
2. **Security** — does it introduce any security regressions?
3. **Performance** — does it introduce allocations in hot paths?
4. **API consistency** — does it feel like the rest of GA-PubSub?
5. **Test quality** — does it test the right things at the right level?

---

## Release Process

We use [Changesets](https://github.com/changesets/changesets) for versioning.

```bash
# Create a changeset describing your change
npm run changeset
# → select affected packages
# → select semver bump type (patch / minor / major)
# → write a summary

# This creates a .changeset/*.md file — commit it with your PR

# On merge to main, the release CI job runs:
npm run release
# → bumps versions, updates CHANGELOG.md, publishes to npm
```

### Versioning policy

- **Patch** — bug fixes, security patches, doc updates
- **Minor** — new features, new built-in middleware, new validator adapters
- **Major** — breaking API changes, security model changes

Security patches may be released as patch versions out of the normal
release cycle. They are always announced in GitHub Security Advisories
before the npm release.

---

## Code of Conduct

This project follows the [Contributor Covenant v2.1](https://www.contributor-covenant.org/version/2/1/code_of_conduct/).

In short: be kind, assume good faith, and leave the project better than you found it.
