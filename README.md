<div align="center">

# GA-PubSub Core

**Free, browser-only, in-memory pub/sub for TypeScript applications.**

[![npm](https://img.shields.io/npm/v/ga-pubsub?color=blue)](https://www.npmjs.com/package/ga-pubsub)
[![License: Elastic-2.0](https://img.shields.io/badge/License-Elastic--2.0-blue.svg)](./LICENSE)
[![TypeScript](https://img.shields.io/badge/TypeScript-5.4%2B-blue)](https://www.typescriptlang.org)

[Documentation](https://deslay-ai.web.app/ga-pubsub-docs/reference) · [Live Demo](https://deslayai.github.io/ga-pubsub/) · [GA-PubSub PRO](https://deslay-ai.web.app/ga-pubsub-docs/pricing)

</div>

---

## Product boundary

This repository publishes one product package:

| Package | License | Supported runtime | Delivery model |
|---------|---------|-------------------|----------------|
| [`ga-pubsub`](./packages/core) | Elastic License 2.0 | Browser | In-memory, within the current page |

`ga-pubsub` does not provide backend runtime support, network transports, broker integrations, HMAC signing, authorization, rate limiting, multi-tenancy, or commercial licensing.

Those capabilities belong exclusively to the separately licensed commercial suite:

- `@deslayai/ga-pubsub-pro` — paid frontend/backend runtime built on `ga-pubsub`.
- Nine paid `@deslayai/ga-pubsub-*` adapters — HTTP, WebSocket, SSE, Socket.IO, BroadcastChannel, Redis, Kafka, NATS, and RabbitMQ.

The historical `packages/*` transport directories are not npm workspaces, are not part of the free package build, and must not be documented or published as core features. The maintained adapters live in their separate PRO repositories.

## Installation

```bash
npm install ga-pubsub
```

## Browser usage

```typescript
import { EventBus } from 'ga-pubsub';

const bus = new EventBus({ namespace: 'storefront' });

bus.subscribe('cart.*', envelope => {
  console.log(envelope.event, envelope.payload);
});

await bus.publish('cart.updated', {
  productId: 'prod_42',
  quantity: 2,
});
```

The event is delivered only inside the browser page containing this bus instance. Refreshing or closing the page clears its in-memory state.

## Included free features

- Exact and wildcard publish/subscribe
- Priority subscribers and one-time subscriptions
- Middleware and schema validation
- In-memory replay and TTL enforcement
- Request/response within the same browser runtime
- Metrics, telemetry hooks, and subscription limits
- ESM and CommonJS package outputs with zero runtime dependencies

## Not included in core

- Backend or server runtime support
- Cross-tab, cross-process, or cross-service delivery
- HTTP, WebSocket, SSE, Socket.IO, BroadcastChannel, Redis, Kafka, NATS, or RabbitMQ transports
- HMAC signing, replay-attack prevention, authorization, rate limiting, or tenant registry
- A commercial production license

Use [GA-PubSub PRO](https://deslay-ai.web.app/ga-pubsub-docs/pricing) when any of these capabilities are required.

## Development

```bash
npm install
npm run build
npm test
```

`npm run build` builds only `packages/core`. Adapter packages are developed and released from their individual PRO repositories.

## Documentation

The complete browser guide, API reference, and copy-ready example are available in [`packages/core/README.md`](./packages/core/README.md) and [`packages/core/examples/usage.ts`](./packages/core/examples/usage.ts).

## License

Elastic License 2.0 © Ajithraj G and Gowri KS. See [LICENSE](./LICENSE).
