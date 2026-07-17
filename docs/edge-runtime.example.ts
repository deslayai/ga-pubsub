/**
 * GA-PubSub — Edge Runtime Examples
 *
 * GA-PubSub core works in any environment that supports:
 *   - Web Crypto API (crypto.subtle + crypto.randomUUID)
 *   - ES Modules
 *   - Promise / async-await
 *
 * This covers: Cloudflare Workers, Deno Deploy, Vercel Edge Functions,
 * Fastly Compute@Edge, and any WinterCG-compatible runtime.
 *
 * Key constraints in edge environments:
 *   - No Node.js built-ins (no 'node:crypto', 'node:events', etc.)
 *   - Short request lifetimes — persistent state must use KV or external storage
 *   - No long-running background timers (the TTL sweep timer is non-blocking)
 *   - Transport adapters that require Node.js (ioredis, kafkajs) won't work
 *     → Use @ga-pubsub/websocket or @ga-pubsub/sse for edge transports
 *
 * All three examples below use ONLY ga-pubsub core — zero Node.js APIs.
 */

// ─────────────────────────────────────────────────────────────────────────────
// EXAMPLE 1: Cloudflare Worker — event-driven webhook handler
// ─────────────────────────────────────────────────────────────────────────────

/**
 * Cloudflare Worker that validates incoming webhook payloads,
 * publishes them to an event bus, and fans out to multiple handlers.
 *
 * Deploy: wrangler publish
 * wrangler.toml: compatibility_date = "2024-01-01"
 */

import { getBus, zodValidator, loggingMiddleware } from 'ga-pubsub';

// Zod is edge-compatible (pure JS, no native modules)
// import { z } from 'zod';

// ── Schemas ──────────────────────────────────────────────────────────────────

// const GitHubPushSchema = z.object({
//   ref:        z.string(),
//   repository: z.object({ full_name: z.string() }),
//   commits:    z.array(z.object({ id: z.string(), message: z.string() })),
// });

// ── Bus setup (module-level — reused across requests in the same isolate) ────

const webhookBus = getBus('webhooks', {
  replay:          { limit: 0 },    // No replay in edge — stateless
  maxPayloadBytes: 65_536,
  enableSigning:   true,
  // In production: signingSecret: env.WEBHOOK_HMAC_SECRET
});

// Middleware
webhookBus.use(loggingMiddleware({ level: 'debug' }));
webhookBus.use(async (envelope, next) => {
  // Enrich with request metadata
  envelope.metadata = {
    ...envelope.metadata,
    edgeRuntime: 'cloudflare-workers',
    processedAt:  Date.now(),
  };
  await next();
});

// Schema validation
// webhookBus.registerSchema('github.push', zodValidator('GitHubPush', GitHubPushSchema));

// Handlers
webhookBus.subscribe('github.push', async (envelope) => {
  const payload = envelope.payload as { ref: string; commits: unknown[] };
  console.log(`[CI] Push to ${payload.ref} — ${payload.commits.length} commits`);
  // await triggerCIBuild(payload);
});

webhookBus.subscribe('github.push', async (envelope) => {
  const payload = envelope.payload as { repository: { full_name: string } };
  console.log(`[Slack] Notify team about push to ${payload.repository.full_name}`);
  // await notifySlack(payload);
});

// ── Worker handler ────────────────────────────────────────────────────────────

export interface Env {
  WEBHOOK_HMAC_SECRET: string;
}

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const url = new URL(request.url);

    if (request.method !== 'POST') {
      return new Response('Method Not Allowed', { status: 405 });
    }

    // Parse event type from path: /webhook/github/push → 'github.push'
    const eventType = url.pathname
      .replace(/^\/webhook\//, '')
      .replace(/\//g, '.');

    let payload: unknown;
    try {
      payload = await request.json();
    } catch {
      return new Response('Invalid JSON', { status: 400 });
    }

    try {
      await webhookBus.publish(eventType, payload, {
        // Propagate request correlation headers
        correlationId: request.headers.get('x-correlation-id') ?? undefined,
        source:        request.headers.get('x-webhook-source') ?? 'webhook',
        metadata: {
          userAgent: request.headers.get('user-agent') ?? '',
          cfRay:     request.headers.get('cf-ray') ?? '',
          country:   request.headers.get('cf-ipcountry') ?? '',
        },
      });

      return Response.json({ ok: true, event: eventType });
    } catch (err) {
      console.error('Webhook processing failed:', err);
      return new Response('Internal Server Error', { status: 500 });
    }
  },
};

// ─────────────────────────────────────────────────────────────────────────────
// EXAMPLE 2: Deno Deploy — pub/sub relay with SSE streaming
// ─────────────────────────────────────────────────────────────────────────────

/**
 * Deno Deploy function that accepts published events via POST
 * and fans them out to SSE subscribers.
 *
 * Architecture:
 *   Publisher → POST /publish  → GA-PubSub bus → SSE streams → Subscribers
 *
 * Deploy: deployctl deploy --project=ga-pubsub-relay edge-runtime.example.ts
 */

// In a real Deno Deploy file these would be top-level imports:
// import { getBus } from 'ga-pubsub';
// import { createSSEWriter } from '@ga-pubsub/sse';

/*
const relayBus = getBus('relay', {
  replay:          { limit: 5, ttl: 60_000 },
  maxPayloadBytes: 32_768,
});

// Track active SSE connections
const sseConnections = new Map<string, ReturnType<typeof createSSEWriter>>();

Deno.serve(async (req) => {
  const url = new URL(req.url);

  // POST /publish — receive events from upstream services
  if (req.method === 'POST' && url.pathname === '/publish') {
    const { event, payload, correlationId } = await req.json();
    await relayBus.publish(event, payload, { correlationId });
    return new Response(JSON.stringify({ ok: true }), {
      headers: { 'Content-Type': 'application/json' },
    });
  }

  // GET /stream — SSE endpoint for subscribers
  if (req.method === 'GET' && url.pathname === '/stream') {
    const clientId = crypto.randomUUID();
    const pattern = url.searchParams.get('pattern') ?? '**';

    const { readable, writable } = new TransformStream();
    const writer = writable.getWriter();
    const encoder = new TextEncoder();

    const write = (envelope: EventEnvelope) => {
      const data = `event: ga-pubsub\nid: ${envelope.id}\ndata: ${JSON.stringify(envelope)}\n\n`;
      writer.write(encoder.encode(data)).catch(() => {
        handle.unsubscribe();
        writer.close().catch(() => {});
      });
    };

    // Subscribe and replay history immediately
    const handle = relayBus.subscribe(pattern, write, { replay: true });
    req.signal.addEventListener('abort', () => {
      handle.unsubscribe();
      sseConnections.delete(clientId);
      writer.close().catch(() => {});
    });

    sseConnections.set(clientId, write as never);

    return new Response(readable, {
      headers: {
        'Content-Type':  'text/event-stream',
        'Cache-Control': 'no-cache',
        'Connection':    'keep-alive',
        'X-Client-ID':   clientId,
      },
    });
  }

  return new Response('Not Found', { status: 404 });
});
*/

// ─────────────────────────────────────────────────────────────────────────────
// EXAMPLE 3: Vercel Edge Function — per-request isolated bus
// ─────────────────────────────────────────────────────────────────────────────

/**
 * Vercel Edge Function that uses GA-PubSub for internal request processing.
 * Each request gets its own isolated bus (no shared state between requests).
 *
 * Use case: fan-out enrichment pipeline — enrich a product object with
 * multiple independent data sources in parallel, collected via pub/sub.
 */

/*
// pages/api/product/[id].ts (Edge Runtime)
export const config = { runtime: 'edge' };

export default async function handler(request: Request): Promise<Response> {
  const url = new URL(request.url);
  const productId = url.pathname.split('/').at(-1) ?? '';

  // Per-request bus — isolated, no cross-request leakage
  const { EventBus, destroyAll } = await import('ga-pubsub');
  const bus = new EventBus({ namespace: `request-${crypto.randomUUID()}` });

  const enrichments: Record<string, unknown> = {};

  // Register collectors
  bus.subscribe('product.pricing',     (e) => { enrichments['pricing']     = e.payload; });
  bus.subscribe('product.inventory',   (e) => { enrichments['inventory']   = e.payload; });
  bus.subscribe('product.reviews',     (e) => { enrichments['reviews']     = e.payload; });
  bus.subscribe('product.related',     (e) => { enrichments['related']     = e.payload; });

  // Trigger all enrichments in parallel
  await Promise.allSettled([
    bus.publish('product.pricing',   await fetchPricing(productId)),
    bus.publish('product.inventory', await fetchInventory(productId)),
    bus.publish('product.reviews',   await fetchReviews(productId)),
    bus.publish('product.related',   await fetchRelated(productId)),
  ]);

  await bus.destroy(); // Clean up immediately

  return Response.json({
    id: productId,
    ...enrichments,
    _meta: { enrichedAt: new Date().toISOString() },
  });
}
*/

// ─────────────────────────────────────────────────────────────────────────────
// EDGE COMPATIBILITY NOTES
// ─────────────────────────────────────────────────────────────────────────────

export const EDGE_COMPATIBILITY = {
  'ga-pubsub core': {
    cloudflareWorkers:  '✅ Full support',
    denoDeploy:         '✅ Full support',
    vercelEdge:         '✅ Full support',
    fastlyCompute:      '✅ Full support — no globalThis.crypto.randomUUID, falls back to getRandomValues',
    nodeJs18:           '✅ Full support',
    nodeJs16:           '⚠️  Requires --experimental-global-webcrypto flag',
  },
  '@ga-pubsub/websocket': {
    cloudflareWorkers:  '✅ Client mode (built-in WebSocket)',
    denoDeploy:         '✅ Client mode (built-in WebSocket)',
    vercelEdge:         '✅ Client mode (built-in WebSocket)',
    nodeJs:             '✅ With `ws` package as WebSocket constructor',
  },
  '@ga-pubsub/sse': {
    cloudflareWorkers:  '✅ Both channels (fetch + Response streaming)',
    denoDeploy:         '✅ Both channels (Deno.serve + TransformStream)',
    vercelEdge:         '✅ Both channels (fetch + ReadableStream)',
    nodeJs:             '✅ With EventSource polyfill for subscribe',
  },
  '@ga-pubsub/broadcast-channel': {
    cloudflareWorkers:  '⚠️  Workers do not support BroadcastChannel across isolates',
    browsers:           '✅ Full support (Chrome 54+, Firefox 38+, Safari 15.4+)',
    nodeJs:             '❌ Not available — use Redis or NATS',
  },
  '@ga-pubsub/redis': {
    cloudflareWorkers:  '❌ ioredis requires Node.js — use Hyperdrive with REST',
    denoDeploy:         '❌ ioredis requires Node.js',
    nodeJs:             '✅ Full support',
  },
  '@ga-pubsub/kafka': {
    cloudflareWorkers:  '❌ kafkajs requires Node.js',
    nodeJs:             '✅ Full support',
  },
  '@ga-pubsub/nats': {
    cloudflareWorkers:  '❌ nats.js requires Node.js TCP',
    nodeJs:             '✅ Full support',
  },
  '@ga-pubsub/rabbitmq': {
    cloudflareWorkers:  '❌ amqplib requires Node.js TCP',
    nodeJs:             '✅ Full support',
  },
} as const;
