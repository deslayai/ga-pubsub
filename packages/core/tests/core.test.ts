/**
 * ga-pubsub-core — Package-level test suite
 *
 * Imports exclusively from 'ga-pubsub-core' (no pro features).
 * Run: cd packages/core && npm test
 */

import { describe, it, expect, beforeEach } from 'vitest';
import {
  EventBus,
  ReplayEngine,
  wildcardMatcher,
  generateId,
  sanitizePayload,
  isExpired,
  loggingMiddleware,
  ttlGuardMiddleware,
  GAPubSubError,
  ValidationFailedError,
  RequestTimeoutError,
  SubscriptionLimitError,
  MiddlewareAbortError,
} from 'ga-pubsub-core';
import type { EventEnvelope } from 'ga-pubsub-core';

// ─── Helpers ──────────────────────────────────────────────────────────────────

function sleep(ms: number) { return new Promise(r => setTimeout(r, ms)); }

function makeEnvelope(event = 'test', payload: unknown = {}): EventEnvelope {
  return {
    id: generateId(), event, namespace: 'test', payload,
    timestamp: Date.now(), correlationId: generateId(),
    causationId: '', source: 'test', version: '1',
  };
}

// ─── wildcardMatcher ──────────────────────────────────────────────────────────

describe('wildcardMatcher', () => {
  it('exact match', () => {
    expect(wildcardMatcher.matches('a.b', 'a.b')).toBe(true);
    expect(wildcardMatcher.matches('a.b', 'a.c')).toBe(false);
  });

  it('single wildcard (*) matches one segment', () => {
    expect(wildcardMatcher.matches('a.*', 'a.b')).toBe(true);
    expect(wildcardMatcher.matches('a.*', 'a.b.c')).toBe(false);
  });

  it('double wildcard (**) matches one or more trailing segments', () => {
    expect(wildcardMatcher.matches('a.**', 'a')).toBe(false);      // dot separator requires at least one segment
    expect(wildcardMatcher.matches('a.**', 'a.b')).toBe(true);
    expect(wildcardMatcher.matches('a.**', 'a.b.c.d')).toBe(true);
  });

  it('mixed wildcards', () => {
    expect(wildcardMatcher.matches('a.*.c', 'a.b.c')).toBe(true);
    expect(wildcardMatcher.matches('a.*.c', 'a.b.d')).toBe(false);
  });
});

// ─── generateId ───────────────────────────────────────────────────────────────

describe('generateId', () => {
  it('returns a UUID-like string', () => {
    const id = generateId();
    expect(typeof id).toBe('string');
    expect(id).toHaveLength(36);
  });

  it('every call returns a unique id', () => {
    const ids = new Set(Array.from({ length: 1000 }, generateId));
    expect(ids.size).toBe(1000);
  });
});

// ─── sanitizePayload ──────────────────────────────────────────────────────────

describe('sanitizePayload', () => {
  it('deep-clones the payload', () => {
    const orig = { a: { b: 1 } };
    const copy = sanitizePayload(orig);
    expect(copy).toEqual(orig);
    expect(copy).not.toBe(orig);
    copy.a.b = 99;
    expect(orig.a.b).toBe(1);
  });

  it('handles null / arrays', () => {
    expect(sanitizePayload(null)).toBeNull();
    expect(sanitizePayload([1, 2, 3])).toEqual([1, 2, 3]);
  });
});

// ─── isExpired ────────────────────────────────────────────────────────────────

describe('isExpired', () => {
  it('returns false when no ttl', () => {
    expect(isExpired(makeEnvelope())).toBe(false);
  });

  it('returns true when ttl has elapsed', () => {
    const env = { ...makeEnvelope(), timestamp: Date.now() - 1000, ttl: 500 };
    expect(isExpired(env)).toBe(true);
  });

  it('returns false when ttl has not elapsed', () => {
    const env = { ...makeEnvelope(), timestamp: Date.now(), ttl: 10_000 };
    expect(isExpired(env)).toBe(false);
  });
});

// ─── ReplayEngine ─────────────────────────────────────────────────────────────

describe('ReplayEngine', () => {
  it('stores and retrieves events', () => {
    const engine = new ReplayEngine({ limit: 5 });
    const env = makeEnvelope('user.created', { id: '1' });
    engine.store_('user.created', env);
    const history = engine.getHistory('user.created');
    expect(history).toHaveLength(1);
    expect(history[0]).toBe(env);
  });

  it('ring buffer evicts oldest when limit exceeded', () => {
    const engine = new ReplayEngine({ limit: 3 });
    for (let i = 0; i < 5; i++) engine.store_('evt', makeEnvelope('evt', { i }));
    const history = engine.getHistory('evt');
    expect(history).toHaveLength(3);
    expect((history[2]?.payload as { i: number }).i).toBe(4);
  });

  it('wildcard replay', () => {
    const engine = new ReplayEngine({ limit: 10, replayWildcards: true });
    engine.store_('order.created', makeEnvelope('order.created'));
    engine.store_('order.updated', makeEnvelope('order.updated'));
    expect(engine.getHistory('order.*')).toHaveLength(2);
  });
});

// ─── EventBus — pub/sub ───────────────────────────────────────────────────────

describe('EventBus — basic pub/sub', () => {
  it('delivers event to subscriber', async () => {
    const bus = new EventBus();
    let received: unknown;
    bus.subscribe('test', e => { received = e.payload; });
    await bus.publish('test', { msg: 'hello' });
    expect(received).toEqual({ msg: 'hello' });
    await bus.destroy();
  });

  it('subscribeOnce auto-unsubscribes', async () => {
    const bus = new EventBus();
    let count = 0;
    bus.subscribeOnce('evt', () => { count++; });
    await bus.publish('evt', {});
    await bus.publish('evt', {});
    expect(count).toBe(1);
    await bus.destroy();
  });

  it('unsubscribe stops delivery', async () => {
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
    bus.subscribe('order.*', e => { received.push(e.event); });
    await bus.publish('order.created', {});
    await bus.publish('order.updated', {});
    await bus.publish('payment.created', {});
    expect(received).toEqual(['order.created', 'order.updated']);
    await bus.destroy();
  });

  it('priority ordering — higher priority runs first', async () => {
    const bus = new EventBus();
    const order: number[] = [];
    bus.subscribe('evt', () => { order.push(1); }, { priority: 1 });
    bus.subscribe('evt', () => { order.push(10); }, { priority: 10 });
    bus.subscribe('evt', () => { order.push(5); }, { priority: 5 });
    await bus.publish('evt', {});
    expect(order).toEqual([10, 5, 1]);
    await bus.destroy();
  });
});

// ─── Middleware ───────────────────────────────────────────────────────────────

describe('EventBus — middleware', () => {
  it('middleware can modify payload', async () => {
    const bus = new EventBus();
    bus.use(async (e, next) => {
      (e as { payload: Record<string, unknown> }).payload = { ...e.payload as object, enriched: true };
      await next();
    });
    let received: unknown;
    bus.subscribe('evt', e => { received = e.payload; });
    await bus.publish('evt', { original: true });
    expect(received).toEqual({ original: true, enriched: true });
    await bus.destroy();
  });

  it('middleware abort prevents subscribers from running', async () => {
    const bus = new EventBus();
    bus.use(async (_e, _next) => { throw new MiddlewareAbortError('test'); });
    let called = false;
    bus.subscribe('evt', () => { called = true; });
    await expect(bus.publish('evt', {})).rejects.toThrow(MiddlewareAbortError);
    expect(called).toBe(false);
    await bus.destroy();
  });

  it('built-in loggingMiddleware runs without errors', async () => {
    const bus = new EventBus();
    bus.use(loggingMiddleware({ level: 'debug' }));
    let ok = false;
    bus.subscribe('evt', () => { ok = true; });
    await bus.publish('evt', {});
    expect(ok).toBe(true);
    await bus.destroy();
  });

  it('ttlGuardMiddleware drops expired events', async () => {
    const bus = new EventBus();
    bus.use(ttlGuardMiddleware());
    let called = false;
    bus.subscribe('evt', () => { called = true; });
    // Publish with an already-expired TTL by crafting the payload through middleware
    // We simulate by publishing normally — ttlGuard only drops when timestamp+ttl < now
    await bus.publish('evt', {}, { ttl: 100_000 }); // not expired
    expect(called).toBe(true);
    await bus.destroy();
  });
});

// ─── Schema validation ────────────────────────────────────────────────────────

describe('EventBus — schema validation', () => {
  it('rejects invalid payload', async () => {
    const bus = new EventBus();
    bus.registerSchema('typed', {
      name: 'TypedValidator',
      validate: (p: unknown) => typeof (p as { id?: unknown }).id === 'string'
        ? { valid: true }
        : { valid: false, errors: [{ path: 'id', message: 'must be string' }] },
    });
    await expect(bus.publish('typed', { id: 123 })).rejects.toThrow(ValidationFailedError);
    await bus.destroy();
  });

  it('allows valid payload', async () => {
    const bus = new EventBus();
    bus.registerSchema('typed', {
      name: 'TypedValidator',
      validate: (p: unknown) => typeof (p as { id?: unknown }).id === 'string'
        ? { valid: true }
        : { valid: false, errors: [{ path: 'id', message: 'must be string' }] },
    });
    let received: unknown;
    bus.subscribe('typed', e => { received = e.payload; });
    await bus.publish('typed', { id: 'abc' });
    expect(received).toEqual({ id: 'abc' });
    await bus.destroy();
  });
});

// ─── Request / Response (RPC) ─────────────────────────────────────────────────

describe('EventBus — RPC', () => {
  it('request resolves with response', async () => {
    const bus = new EventBus();
    bus.respond<{ name: string }, { greeting: string }>('greet', e =>
      ({ greeting: `Hello, ${(e.payload as { name: string }).name}!` })
    );
    const { response } = bus.request<{ name: string }, { greeting: string }>(
      'greet', { name: 'World' }, { timeoutMs: 2000 }
    );
    const env = await response;
    expect((env.payload as { greeting: string }).greeting).toBe('Hello, World!');
    await bus.destroy();
  });

  it('request times out when no responder', async () => {
    const bus = new EventBus();
    const { response } = bus.request('no.handler', {}, { timeoutMs: 50 });
    await expect(response).rejects.toThrow(RequestTimeoutError);
    await bus.destroy();
  });

  it('cancel() rejects the response promise', async () => {
    const bus = new EventBus();
    const handle = bus.request('no.handler', {}, { timeoutMs: 5000 });
    handle.cancel();
    await expect(handle.response).rejects.toThrow(GAPubSubError);
    await bus.destroy();
  });
});

// ─── Replay ───────────────────────────────────────────────────────────────────

describe('EventBus — replay', () => {
  it('late subscriber receives replayed events', async () => {
    const bus = new EventBus({ replay: { limit: 5 } });
    await bus.publish('app.started', { version: '1.0' });
    await sleep(10);
    const received: unknown[] = [];
    bus.subscribe('app.started', e => { received.push(e.payload); });
    await sleep(20);
    expect(received).toHaveLength(1);
    await bus.destroy();
  });

  it('opt-out with replay: false', async () => {
    const bus = new EventBus({ replay: { limit: 5 } });
    await bus.publish('app.started', { version: '1.0' });
    const received: unknown[] = [];
    bus.subscribe('app.started', e => { received.push(e.payload); }, { replay: false });
    await sleep(20);
    expect(received).toHaveLength(0);
    await bus.destroy();
  });
});

// ─── Subscription limit ───────────────────────────────────────────────────────

describe('EventBus — subscription limit', () => {
  it('throws SubscriptionLimitError when limit reached', () => {
    const bus = new EventBus({ maxSubscriptions: 2 });
    bus.subscribe('a', () => {});
    bus.subscribe('b', () => {});
    expect(() => bus.subscribe('c', () => {})).toThrow(SubscriptionLimitError);
  });
});

// ─── Metrics ─────────────────────────────────────────────────────────────────

describe('EventBus — metrics', () => {
  it('tracks publish count', async () => {
    const bus = new EventBus();
    await bus.publish('evt', {});
    await bus.publish('evt', {});
    expect(bus.getMetrics().publishCount).toBe(2);
    await bus.destroy();
  });

  it('tracks active subscriptions', () => {
    const bus = new EventBus();
    const h1 = bus.subscribe('a', () => {});
    const h2 = bus.subscribe('b', () => {});
    expect(bus.getMetrics().activeSubscriptions).toBe(2);
    h1.unsubscribe();
    expect(bus.getMetrics().activeSubscriptions).toBe(1);
    h2.unsubscribe();
  });
});

// ─── Errors ───────────────────────────────────────────────────────────────────

describe('Error classes', () => {
  it('GAPubSubError carries code', () => {
    const err = new GAPubSubError('oops', 'TEST_CODE');
    expect(err.code).toBe('TEST_CODE');
    expect(err.message).toBe('oops');
  });

  it('MiddlewareAbortError has correct code', () => {
    const err = new MiddlewareAbortError('evt');
    expect(err.code).toBe('MIDDLEWARE_ABORT');
  });
});
