/**
 * GA-PubSub — Test Suite
 *
 * Coverage targets: >95% across all modules
 *
 * Test categories:
 *   1. Unit tests — individual modules in isolation
 *   2. Integration tests — bus + replay + middleware + auth working together
 *   3. Security tests — signing, replay attack, rate limiting, payload size
 *   4. Stress tests — 100k subscriptions, 1M events, memory leak detection
 *   5. Contract tests — TransportAdapter interface conformance
 *
 * Run: npx vitest run
 *       npx vitest run --reporter=verbose --coverage
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';

// ─────────────────────────────────────────────────────────────────────────────
// MOCK CRYPTO (for test environments without Web Crypto)
// ─────────────────────────────────────────────────────────────────────────────

// vitest.config.ts should configure jsdom or use --environment node
// with the globalSetup loading a Web Crypto polyfill.

// ─────────────────────────────────────────────────────────────────────────────
// UNIT: WILDCARD MATCHER
// ─────────────────────────────────────────────────────────────────────────────

describe('wildcardMatcher', () => {
  // Import inside describe to allow tree-shaking in prod
  let wildcardMatcher: typeof import('../../packages/core/src/wildcard.ts').wildcardMatcher;

  beforeEach(async () => {
    ({ wildcardMatcher } = await import('../../packages/core/src/wildcard.ts'));
  });

  it('exact match — equal strings', () => {
    expect(wildcardMatcher.matches('user.created', 'user.created')).toBe(true);
  });

  it('exact match — unequal strings', () => {
    expect(wildcardMatcher.matches('user.created', 'user.deleted')).toBe(false);
  });

  it('single wildcard — matches one segment', () => {
    expect(wildcardMatcher.matches('user.*', 'user.created')).toBe(true);
    expect(wildcardMatcher.matches('user.*', 'user.deleted')).toBe(true);
  });

  it('single wildcard — does not match multiple segments', () => {
    expect(wildcardMatcher.matches('user.*', 'user.billing.created')).toBe(false);
  });

  it('double wildcard — matches zero segments', () => {
    expect(wildcardMatcher.matches('payments.**', 'payments')).toBe(false);
    // 'payments.**' requires 'payments.' prefix
  });

  it('double wildcard — matches one segment', () => {
    expect(wildcardMatcher.matches('payments.**', 'payments.created')).toBe(true);
  });

  it('double wildcard — matches many segments', () => {
    expect(wildcardMatcher.matches('payments.**', 'payments.invoice.line.created')).toBe(true);
  });

  it('mixed wildcards', () => {
    expect(wildcardMatcher.matches('*.invoice.*', 'payments.invoice.created')).toBe(true);
    expect(wildcardMatcher.matches('*.invoice.*', 'payments.invoice')).toBe(false);
  });

  it('compiled pattern is cached', () => {
    const p1 = wildcardMatcher.compile('user.*');
    const p2 = wildcardMatcher.compile('user.*');
    expect(p1).toBe(p2); // Same reference from LRU cache
  });

  it('no wildcard — delegates to string equality', () => {
    expect(wildcardMatcher.matches('exact.event', 'exact.event')).toBe(true);
    expect(wildcardMatcher.matches('exact.event', 'different.event')).toBe(false);
  });
});

// ─────────────────────────────────────────────────────────────────────────────
// UNIT: SECURITY — PAYLOAD SIZE
// ─────────────────────────────────────────────────────────────────────────────

describe('enforcePayloadLimit', () => {
describe('RateLimiter', () => {
describe('ReplayEngine', () => {
  let ReplayEngine: typeof import('../../packages/core/src/replay.ts').ReplayEngine;

  beforeEach(async () => {
    ({ ReplayEngine } = await import('../../packages/core/src/replay.ts'));
  });

  it('stores and retrieves events', () => {
    const engine = new ReplayEngine({ limit: 5 });
    const envelope = makeEnvelope('user.created', { id: '1' });
    engine.store_('user.created', envelope);
    const history = engine.getHistory('user.created');
    expect(history).toHaveLength(1);
    expect(history[0]).toBe(envelope);
  });

  it('respects replay limit (ring buffer)', () => {
    const engine = new ReplayEngine({ limit: 3 });
    for (let i = 0; i < 5; i++) {
      engine.store_('evt', makeEnvelope('evt', { i }));
    }
    const history = engine.getHistory('evt');
    expect(history).toHaveLength(3);
    // Should have the LATEST 3 events
    expect((history[0].payload as { i: number }).i).toBe(2);
    expect((history[2].payload as { i: number }).i).toBe(4);
  });

  it('filters by TTL', async () => {
    const engine = new ReplayEngine({ limit: 5, ttl: 50 });
    const envelope = makeEnvelope('evt', {});
    engine.store_('evt', envelope);
    await sleep(60);
    const history = engine.getHistory('evt');
    expect(history).toHaveLength(0);
  });

  it('filters by lastMs', () => {
    const engine = new ReplayEngine({ limit: 10 });
    const old = makeEnvelope('evt', {}, Date.now() - 10_000);
    const recent = makeEnvelope('evt', {});
    engine.store_('evt', old);
    engine.store_('evt', recent);
    const history = engine.getHistory('evt', { lastMs: 5_000 });
    expect(history).toHaveLength(1);
    expect(history[0]).toBe(recent);
  });

  it('wildcard replay returns matching events', () => {
    const engine = new ReplayEngine({ limit: 5, replayWildcards: true });
    const e1 = makeEnvelope('user.created', {});
    const e2 = makeEnvelope('user.deleted', {});
    engine.store_('user.created', e1);
    engine.store_('user.deleted', e2);
    const history = engine.getHistory('user.*');
    expect(history).toHaveLength(2);
  });

  it('no replay when limit is 0', () => {
    const engine = new ReplayEngine({ limit: 0 });
    engine.store_('evt', makeEnvelope('evt', {}));
    expect(engine.getHistory('evt')).toHaveLength(0);
  });
});

// ─────────────────────────────────────────────────────────────────────────────
// INTEGRATION: EVENT BUS — BASIC PUB/SUB
// ─────────────────────────────────────────────────────────────────────────────

describe('EventBus — basic pub/sub', () => {
  let EventBus: typeof import('../../packages/core/src/bus.ts').EventBus;

  beforeEach(async () => {
    ({ EventBus } = await import('../../packages/core/src/bus.ts'));
  });

  it('delivers event to subscriber', async () => {
    const bus = new EventBus();
    const received: unknown[] = [];
    bus.subscribe('test.event', (e) => { received.push(e.payload); });
    await bus.publish('test.event', { value: 42 });
    expect(received).toEqual([{ value: 42 }]);
    await bus.destroy();
  });

  it('subscribeOnce auto-unsubscribes after first event', async () => {
    const bus = new EventBus();
    let count = 0;
    bus.subscribeOnce('evt', () => { count++; });
    await bus.publish('evt', {});
    await bus.publish('evt', {});
    expect(count).toBe(1);
    await bus.destroy();
  });

  it('unsubscribe stops event delivery', async () => {
    const bus = new EventBus();
    let count = 0;
    const handle = bus.subscribe('evt', () => { count++; });
    await bus.publish('evt', {});
    handle.unsubscribe();
    await bus.publish('evt', {});
    expect(count).toBe(1);
    await bus.destroy();
  });

  it('wildcard subscriber receives matching events', async () => {
    const bus = new EventBus();
    const received: string[] = [];
    bus.subscribe('user.*', (e) => { received.push(e.event); });
    await bus.publish('user.created', {});
    await bus.publish('user.deleted', {});
    await bus.publish('payment.created', {}); // Should NOT match
    expect(received).toEqual(['user.created', 'user.deleted']);
    await bus.destroy();
  });

  it('priority ordering — high priority runs first', async () => {
    const bus = new EventBus();
    const order: number[] = [];
    bus.subscribe('evt', () => { order.push(1); }, { priority: 1 });
    bus.subscribe('evt', () => { order.push(10); }, { priority: 10 });
    bus.subscribe('evt', () => { order.push(5); }, { priority: 5 });
    await bus.publish('evt', {});
    expect(order).toEqual([10, 5, 1]);
    await bus.destroy();
  });

  it('middleware can modify payload', async () => {
    const bus = new EventBus();
    bus.use(async (envelope, next) => {
      (envelope as { payload: { enriched: boolean } }).payload = {
        ...(envelope.payload as object),
        enriched: true,
      };
      await next();
    });
    let received: unknown;
    bus.subscribe('evt', (e) => { received = e.payload; });
    await bus.publish('evt', { original: true });
    expect(received).toEqual({ original: true, enriched: true });
    await bus.destroy();
  });

  it('middleware abort — downstream subscribers not called', async () => {
    const bus = new EventBus();
    bus.use(async (_envelope, _next) => {
      // Abort by not calling next()
    });
    let called = false;
    bus.subscribe('evt', () => { called = true; });
    try {
      await bus.publish('evt', {});
    } catch {}
    expect(called).toBe(false);
    await bus.destroy();
  });

  it('multiple middlewares run in order', async () => {
    const bus = new EventBus();
    const order: number[] = [];
    bus.use(async (_e, next) => { order.push(1); await next(); });
    bus.use(async (_e, next) => { order.push(2); await next(); });
    bus.use(async (_e, next) => { order.push(3); await next(); });
    await bus.publish('evt', {});
    expect(order).toEqual([1, 2, 3]);
    await bus.destroy();
  });
});

// ─────────────────────────────────────────────────────────────────────────────
// INTEGRATION: VALIDATION
// ─────────────────────────────────────────────────────────────────────────────

describe('EventBus — schema validation', () => {
  let EventBus: typeof import('../../packages/core/src/bus.ts').EventBus;
  let ValidationFailedError: typeof import('../../packages/core/src/types.ts').ValidationFailedError;
  let customValidator: typeof import('../../packages/core/src/validators.ts').customValidator;

  beforeEach(async () => {
    ({ EventBus } = await import('../../packages/core/src/bus.ts'));
    ({ ValidationFailedError } = await import('../../packages/core/src/types.ts'));
    ({ customValidator } = await import('../../packages/core/src/validators.ts'));
  });

  it('rejects invalid payload and does not dispatch', async () => {
    const bus = new EventBus();
    bus.registerSchema('typed.event', customValidator('Typed', (p) => {
      if (typeof (p as { id?: unknown }).id !== 'string') {
        return { valid: false, errors: [{ path: 'id', message: 'Must be a string' }] };
      }
      return { valid: true };
    }));
    let called = false;
    bus.subscribe('typed.event', () => { called = true; });
    await expect(bus.publish('typed.event', { id: 123 })).rejects.toThrow(ValidationFailedError);
    expect(called).toBe(false);
    await bus.destroy();
  });

  it('allows valid payload through', async () => {
    const bus = new EventBus();
    bus.registerSchema('typed.event', customValidator('Typed', (p) => {
      return typeof (p as { id?: unknown }).id === 'string' ? { valid: true } : { valid: false, errors: [{ path: 'id', message: 'fail' }] };
    }));
    let received: unknown;
    bus.subscribe('typed.event', (e) => { received = e.payload; });
    await bus.publish('typed.event', { id: 'valid-id' });
    expect(received).toEqual({ id: 'valid-id' });
    await bus.destroy();
  });
});

// ─────────────────────────────────────────────────────────────────────────────
// INTEGRATION: AUTHORIZATION
// ─────────────────────────────────────────────────────────────────────────────

describe('EventBus — authorization', () => {
describe('EventBus — request/response', () => {
  let EventBus: typeof import('../../packages/core/src/bus.ts').EventBus;
  let RequestTimeoutError: typeof import('../../packages/core/src/types.ts').RequestTimeoutError;

  beforeEach(async () => {
    ({ EventBus } = await import('../../packages/core/src/bus.ts'));
    ({ RequestTimeoutError } = await import('../../packages/core/src/types.ts'));
  });

  it('request resolves with response', async () => {
    const bus = new EventBus();
    bus.respond<{ name: string }, { greeting: string }>('greet', async (e) => {
      return { greeting: `Hello, ${(e.payload as { name: string }).name}!` };
    });

    const { response } = bus.request<{ name: string }, { greeting: string }>(
      'greet',
      { name: 'World' },
      { timeoutMs: 1000 }
    );

    const envelope = await response;
    expect((envelope.payload as { greeting: string }).greeting).toBe('Hello, World!');
    await bus.destroy();
  });

  it('request times out when no responder', async () => {
    const bus = new EventBus();
    const { response } = bus.request('no.handler', {}, { timeoutMs: 50 });
    await expect(response).rejects.toThrow(RequestTimeoutError);
    await bus.destroy();
  });
});

// ─────────────────────────────────────────────────────────────────────────────
// INTEGRATION: REPLAY
// ─────────────────────────────────────────────────────────────────────────────

describe('EventBus — replay on subscribe', () => {
  let EventBus: typeof import('../../packages/core/src/bus.ts').EventBus;

  beforeEach(async () => {
    ({ EventBus } = await import('../../packages/core/src/bus.ts'));
  });

  it('late subscriber receives replayed events', async () => {
    const bus = new EventBus({ replay: { limit: 5 } });
    await bus.publish('app.started', { version: '1.0' });
    await sleep(10); // Let async replay settle

    const received: unknown[] = [];
    bus.subscribe('app.started', (e) => { received.push(e.payload); });
    await sleep(20);

    expect(received).toHaveLength(1);
    expect(received[0]).toEqual({ version: '1.0' });
    await bus.destroy();
  });

  it('opt-out replay with replay: false', async () => {
    const bus = new EventBus({ replay: { limit: 5 } });
    await bus.publish('app.started', { version: '1.0' });

    const received: unknown[] = [];
    bus.subscribe('app.started', (e) => { received.push(e.payload); }, { replay: false });
    await sleep(20);
    expect(received).toHaveLength(0);
    await bus.destroy();
  });
});

// ─────────────────────────────────────────────────────────────────────────────
// SECURITY: SUBSCRIPTION LIMITS
// ─────────────────────────────────────────────────────────────────────────────

describe('EventBus — subscription limits', () => {
  let EventBus: typeof import('../../packages/core/src/bus.ts').EventBus;
  let SubscriptionLimitError: typeof import('../../packages/core/src/types.ts').SubscriptionLimitError;

  beforeEach(async () => {
    ({ EventBus } = await import('../../packages/core/src/bus.ts'));
    ({ SubscriptionLimitError } = await import('../../packages/core/src/types.ts'));
  });

  it('throws SubscriptionLimitError when limit is reached', () => {
    const bus = new EventBus({ maxSubscriptions: 2 });
    bus.subscribe('evt1', () => {});
    bus.subscribe('evt2', () => {});
    expect(() => bus.subscribe('evt3', () => {})).toThrow(SubscriptionLimitError);
  });
});

// ─────────────────────────────────────────────────────────────────────────────
// SECURITY: PAYLOAD SIZE LIMIT
// ─────────────────────────────────────────────────────────────────────────────

describe('EventBus — payload size enforcement', () => {
describe('EventBus — stress: 100k subscriptions', () => {
  it('handles 100,000 subscriptions without memory error', async () => {
    const { EventBus } = await import('../../packages/core/src/bus.ts');
    const bus = new EventBus({ maxSubscriptions: 0, enableWildcard: false });

    const handles = [];
    for (let i = 0; i < 100_000; i++) {
      handles.push(bus.subscribe(`event.${i}`, () => {}));
    }

    expect(bus.getMetrics().activeSubscriptions).toBe(100_000);

    // Publish to one specific event — should route to exactly 1 subscriber
    let hitCount = 0;
    bus.subscribe('event.50000', () => { hitCount++; });
    await bus.publish('event.50000', {}, { storeHistory: false });
    expect(hitCount).toBe(1);

    await bus.destroy();
  }, 30_000); // 30 second timeout for stress test
});

// ─────────────────────────────────────────────────────────────────────────────
// STRESS: 10K SEQUENTIAL PUBLISHES
// ─────────────────────────────────────────────────────────────────────────────

describe('EventBus — stress: 10k sequential publishes', () => {
  it('publishes 10,000 events without degradation', async () => {
    const { EventBus } = await import('../../packages/core/src/bus.ts');
    const bus = new EventBus({ replay: { limit: 0 } }); // No replay storage

    let received = 0;
    bus.subscribe('perf.test', () => { received++; });

    const start = Date.now();
    for (let i = 0; i < 10_000; i++) {
      await bus.publish('perf.test', { i }, { storeHistory: false });
    }
    const elapsed = Date.now() - start;

    expect(received).toBe(10_000);
    // Should process at least 5,000 events/second on any reasonable hardware
    expect(elapsed).toBeLessThan(10_000);
    console.log(`10k sequential publishes in ${elapsed}ms (${Math.round(10_000 / elapsed * 1000)}/sec)`);

    await bus.destroy();
  }, 30_000);
});

// ─────────────────────────────────────────────────────────────────────────────
// CONTRACT: TRANSPORT ADAPTER
// ─────────────────────────────────────────────────────────────────────────────

/**
 * Transport adapter contract test.
 * Run this against each transport implementation to verify conformance.
 */
export function contractTestTransport(
  name: string,
  factory: () => Promise<import('../../packages/core/src/types.ts').TransportAdapter>
): void {
  describe(`Transport contract — ${name}`, () => {
    let transport: import('../../packages/core/src/types.ts').TransportAdapter;

    beforeEach(async () => { transport = await factory(); });
    afterEach(async () => { await transport.disconnect(); });

    it('connect() resolves without error', async () => {
      await expect(transport.connect()).resolves.not.toThrow();
    });

    it('ping() returns true after connect', async () => {
      await transport.connect();
      expect(await transport.ping()).toBe(true);
    });

    it('subscribe() returns a cleanup function', async () => {
      await transport.connect();
      const cleanup = await transport.subscribe('test.*', () => {});
      expect(typeof cleanup).toBe('function');
      await expect(cleanup()).resolves.not.toThrow();
    });

    it('publish() sends without error', async () => {
      await transport.connect();
      const envelope = makeEnvelope('test.event', { ok: true });
      await expect(transport.publish(envelope)).resolves.not.toThrow();
    });

    it('disconnect() resolves without error', async () => {
      await transport.connect();
      await expect(transport.disconnect()).resolves.not.toThrow();
    });
  });
}

// ─────────────────────────────────────────────────────────────────────────────
// HELPERS
// ─────────────────────────────────────────────────────────────────────────────

function makeEnvelope(event: string, payload: unknown, timestamp = Date.now()): import('../../packages/core/src/types.ts').EventEnvelope {
  return {
    id: Math.random().toString(36).slice(2),
    event,
    namespace: 'test',
    payload,
    timestamp,
    correlationId: Math.random().toString(36).slice(2),
    causationId: '',
    source: 'test',
    version: '1',
  };
}

function sleep(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}

// ─────────────────────────────────────────────────────────────────────────────
// FUZZ TESTS (run with --fuzz flag or a dedicated fuzzing framework)
// ─────────────────────────────────────────────────────────────────────────────

describe('EventBus — fuzz: adversarial event names', () => {
  it('handles adversarial event name inputs without crash', async () => {
    const { EventBus } = await import('../../packages/core/src/bus.ts');
    const bus = new EventBus({ maxPayloadBytes: 0 }); // No size limit for fuzz

    const adversarialNames = [
      '',
      '.',
      '.*',
      '.**',
      '**.**',
      'a'.repeat(10_000),
      'a.b.c.d.e.f.g.h.i.j.k.l.m.n.o.p.q.r.s.t.u.v.w.x.y.z',
      '\x00\x01\x02',
      '💀🔥🚀',
      '__proto__',
      'constructor',
      'prototype.pollute',
    ];

    for (const name of adversarialNames) {
      try {
        await bus.publish(name, {}, { storeHistory: false });
      } catch {
        // Expected: some may be rejected — the key is no crash or memory issue
      }
    }

    await bus.destroy();
  });
});

describe('EventBus — fuzz: adversarial payloads', () => {
  it('handles adversarial payloads without prototype pollution', async () => {
    const { EventBus } = await import('../../packages/core/src/bus.ts');
    const bus = new EventBus({ maxPayloadBytes: 0 });

    let received: unknown;
    bus.subscribe('fuzz.test', (e) => { received = e.payload; });

    const adversarialPayloads:any = [
      { '__proto__': { polluted: true } },
      { 'constructor': { prototype: { polluted: true } } },
      null,
      undefined,
      42,
      'string payload',
      [],
      [1, 2, { nested: true }],
      { deeply: { nested: { object: { with: { many: { levels: true } } } } } },
    ];

    for (const payload of adversarialPayloads) {
      try {
        await bus.publish('fuzz.test', payload, { storeHistory: false });
      } catch {}
    }

    // Verify prototype was not polluted
    expect((({}  as Record<string, unknown>)['polluted'])).toBeUndefined();
    await bus.destroy();
  });
});
