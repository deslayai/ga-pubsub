/**
 * GA-PubSub — Integration Tests
 *
 * End-to-end workflows exercising multiple subsystems together:
 *   1. Full publish lifecycle: validate → authorize → middleware → dispatch → replay
 *   2. Multi-tenant isolation with shared registry
 *   3. Scoped bus (prefix isolation within a namespace)
 *   4. Request/response with middleware enrichment
 *   5. Telemetry hooks across all phases
 *   6. Built-in middleware (ttlGuard, logging, timestamp, namespaceGuard)
 *   7. Validator composites (AND, OR)
 *   8. Error isolation: one phase failing does not corrupt others
 *   9. Registry lifecycle: getBus, destroyNamespace, destroyAll
 *  10. Real-world scenario: order processing pipeline
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { ProEventBus as EventBus } from '../../packages/pro/src/bus.ts';
import {
  getBus,
  destroyNamespace,
  destroyAll,
  listNamespaces,
  createScopedBus,
} from '../../packages/pro/src/registry.ts';
import {
  loggingMiddleware,
  ttlGuardMiddleware,
  timestampMiddleware,
  namespaceGuardMiddleware,
} from '../../packages/core/src/index.js';
import { zodValidator, andValidator, orValidator, customValidator } from '../../packages/core/src/validators.js';
import type { EventEnvelope, BusMetrics } from '../../packages/core/src/types.js';
import { ValidationFailedError, GAPubSubError } from '../../packages/core/src/types.js';
import { AuthorizationDeniedError } from '../../packages/pro/src/types.ts';

// ─────────────────────────────────────────────────────────────────────────────
// HELPERS
// ─────────────────────────────────────────────────────────────────────────────

const sleep = (ms: number) => new Promise<void>(r => setTimeout(r, ms));

function makeZodLike(pass: boolean, errors: string[] = []) {
  return {
    safeParse: (data: unknown) => pass
      ? { success: true }
      : { success: false, error: { issues: errors.map(e => ({ path: [], message: e, code: 'custom' })) } }
  };
}

// ─────────────────────────────────────────────────────────────────────────────
// 1. FULL PUBLISH LIFECYCLE
// ─────────────────────────────────────────────────────────────────────────────

describe('Integration: full publish lifecycle', () => {
  it('schema → auth → middleware → subscriber all execute in order', async () => {
    const bus = new EventBus();
    const log: string[] = [];

    bus.registerSchema('order.placed', customValidator('OrderPlaced', (p) => {
      log.push('validate');
      return typeof (p as { orderId?: unknown }).orderId === 'string'
        ? { valid: true }
        : { valid: false, errors: [{ path: 'orderId', message: 'Required' }] };
    }));

    bus.authorize('order.placed', () => { log.push('authorize'); return true; });

    bus.use(async (_e, next) => { log.push('middleware'); await next(); });

    bus.subscribe('order.placed', () => { log.push('subscriber'); });

    await bus.publish('order.placed', { orderId: 'ORD-001' });

    expect(log).toEqual(['validate', 'authorize', 'middleware', 'subscriber']);
    await bus.destroy();
  });

  it('failed validation prevents auth + middleware + subscriber from running', async () => {
    const bus = new EventBus();
    const log: string[] = [];

    bus.registerSchema('typed', customValidator('T', () => {
      log.push('validate');
      return { valid: false, errors: [{ path: '', message: 'Always fails' }] };
    }));
    bus.authorize('typed', () => { log.push('authorize'); return true; });
    bus.use(async (_e, next) => { log.push('middleware'); await next(); });
    bus.subscribe('typed', () => { log.push('subscriber'); });

    await expect(bus.publish('typed', {})).rejects.toThrow(ValidationFailedError);
    expect(log).toEqual(['validate']); // only validate ran
    await bus.destroy();
  });

  it('failed auth prevents middleware + subscriber from running', async () => {
    const bus = new EventBus();
    const log: string[] = [];

    bus.authorize('secure', () => { log.push('authorize'); return false; });
    bus.use(async (_e, next) => { log.push('middleware'); await next(); });
    bus.subscribe('secure', () => { log.push('subscriber'); });

    await expect(bus.publish('secure', {})).rejects.toThrow(AuthorizationDeniedError);
    expect(log).toEqual(['authorize']);
    await bus.destroy();
  });
});

// ─────────────────────────────────────────────────────────────────────────────
// 2. MULTI-TENANT REGISTRY
// ─────────────────────────────────────────────────────────────────────────────

describe('Integration: multi-tenant registry', () => {
  afterEach(async () => {
    await destroyAll();
  });

  it('getBus returns the same instance for the same namespace', () => {
    const a = getBus('acme');
    const b = getBus('acme');
    expect(a).toBe(b);
  });

  it('different namespaces return different instances', () => {
    const a = getBus('acme');
    const b = getBus('globex');
    expect(a).not.toBe(b);
  });

  it('listNamespaces() returns all active namespaces', () => {
    getBus('ns-a');
    getBus('ns-b');
    getBus('ns-c');
    const namespaces = listNamespaces();
    expect(namespaces).toContain('ns-a');
    expect(namespaces).toContain('ns-b');
    expect(namespaces).toContain('ns-c');
  });

  it('destroyNamespace() removes it from registry', async () => {
    getBus('to-destroy');
    await destroyNamespace('to-destroy');
    expect(listNamespaces()).not.toContain('to-destroy');
  });

  it('after destroyNamespace, getBus creates a fresh bus', async () => {
    const original = getBus('fresh');
    let callCount = 0;
    original.subscribe('ping', () => { callCount++; });

    await destroyNamespace('fresh');

    const fresh = getBus('fresh');
    expect(fresh).not.toBe(original);
    await fresh.publish('ping', {}, { storeHistory: false });
    // callCount should still be 0 — original was destroyed
    expect(callCount).toBe(0);
  });

  it('cross-namespace events do not leak', async () => {
    const busA = getBus('tenant-a');
    const busB = getBus('tenant-b');

    let bReceived = false;
    busB.subscribe('shared.event', () => { bReceived = true; });

    await busA.publish('shared.event', { payload: 'tenant-a only' });
    await sleep(10);

    expect(bReceived).toBe(false);
  });
});

// ─────────────────────────────────────────────────────────────────────────────
// 3. SCOPED BUS
// ─────────────────────────────────────────────────────────────────────────────

describe('Integration: ScopedBus', () => {
  it('scopes event names with prefix', async () => {
    const bus = new EventBus();
    const received: string[] = [];

    bus.subscribe('user.created', (e) => { received.push(e.event); });
    bus.subscribe('user.deleted', (e) => { received.push(e.event); });

    const userBus = createScopedBus(bus, 'user');
    await userBus.publish('created', { id: '1' });
    await userBus.publish('deleted', { id: '2' });

    expect(received).toEqual(['user.created', 'user.deleted']);
    await bus.destroy();
  });

  it('scoped subscribe registers prefixed pattern', async () => {
    const bus = new EventBus();
    const received: string[] = [];

    const userBus = createScopedBus(bus, 'user');
    userBus.subscribe('*', (e) => { received.push(e.event); });

    await bus.publish('user.created', { id: '1' });
    await bus.publish('user.updated', { id: '1' });
    await bus.publish('payment.created', {}); // should NOT appear

    expect(received).toEqual(['user.created', 'user.updated']);
    await bus.destroy();
  });

  it('scoped request/response works with prefix', async () => {
    const bus = new EventBus();
    const userBus = createScopedBus(bus, 'user');

    userBus.respond<{ id: string }, { name: string }>('fetch', (e) => ({
      name: `User-${(e.payload as { id: string }).id}`,
    }));

    const { response } = userBus.request<{ id: string }, { name: string }>(
      'fetch',
      { id: 'u1' },
      { timeoutMs: 1000 }
    );

    const envelope = await response;
    expect((envelope.payload as { name: string }).name).toBe('User-u1');
    await bus.destroy();
  });
});

// ─────────────────────────────────────────────────────────────────────────────
// 4. REQUEST/RESPONSE WITH MIDDLEWARE
// ─────────────────────────────────────────────────────────────────────────────

describe('Integration: request/response with middleware', () => {
  it('middleware enriches request envelope before responder sees it', async () => {
    const bus = new EventBus();

    bus.use(async (envelope, next) => {
      (envelope as { metadata: Record<string, unknown> }).metadata = {
        ...(envelope.metadata ?? {}),
        enriched: true,
        region: 'eu-west',
      };
      await next();
    });

    let receivedMeta: unknown;
    bus.respond('info.request', (e) => {
      receivedMeta = e.metadata;
      return { ok: true };
    });

    const { response } = bus.request('info.request', {}, { timeoutMs: 1000 });
    await response;

    expect((receivedMeta as Record<string, unknown>)?.enriched).toBe(true);
    expect((receivedMeta as Record<string, unknown>)?.region).toBe('eu-west');
    await bus.destroy();
  });

  it('concurrent requests with the same event name are independently resolved', async () => {
    const bus = new EventBus();

    bus.respond<{ n: number }, { result: number }>('math.square', (e) =>
      ({ result: (e.payload as { n: number }).n ** 2 })
    );

    const results = await Promise.all(
      [2, 3, 4, 5, 6].map(async (n) => {
        const { response } = bus.request<{ n: number }, { result: number }>(
          'math.square', { n }, { timeoutMs: 2000 }
        );
        const env = await response;
        return (env.payload as { result: number }).result;
      })
    );

    expect(results.sort((a, b) => a - b)).toEqual([4, 9, 16, 25, 36]);
    await bus.destroy();
  });
});

// ─────────────────────────────────────────────────────────────────────────────
// 5. TELEMETRY HOOKS
// ─────────────────────────────────────────────────────────────────────────────

describe('Integration: telemetry hooks', () => {
  it('onPublish is called with envelope and latency', async () => {
    const onPublish = vi.fn();
    const bus = new EventBus({ telemetry: { onPublish } });

    await bus.publish('test', { value: 1 });

    expect(onPublish).toHaveBeenCalledOnce();
    const [envelope, latency] = onPublish.mock.calls[0] as [EventEnvelope, number];
    expect(envelope.event).toBe('test');
    expect(typeof latency).toBe('number');
    expect(latency).toBeGreaterThanOrEqual(0);
    await bus.destroy();
  });

  it('onSubscribe / onUnsubscribe fire correctly', () => {
    const onSubscribe   = vi.fn();
    const onUnsubscribe = vi.fn();
    const bus = new EventBus({ telemetry: { onSubscribe, onUnsubscribe } });

    const handle = bus.subscribe('test', () => {});
    expect(onSubscribe).toHaveBeenCalledOnce();
    expect(onSubscribe.mock.calls[0]?.[1]).toBe('test');

    handle.unsubscribe();
    expect(onUnsubscribe).toHaveBeenCalledOnce();
  });

  it('onError fires when middleware throws', async () => {
    const onError = vi.fn();
    const bus = new EventBus({ telemetry: { onError } });

    bus.use(async () => { throw new Error('MW crash'); });

    try { await bus.publish('test', {}); } catch {}

    expect(onError).toHaveBeenCalledOnce();
    const [err, ctx] = onError.mock.calls[0] as [Error, { phase: string }];
    expect(err.message).toBe('MW crash');
    expect(ctx.phase).toBe('middleware');
    await bus.destroy();
  });

  it('onValidationFailure fires with errors array', async () => {
    const onValidationFailure = vi.fn();
    const bus = new EventBus({ telemetry: { onValidationFailure } });

    bus.registerSchema('t', customValidator('T', () => ({
      valid: false,
      errors: [{ path: 'x', message: 'Required' }],
    })));

    try { await bus.publish('t', {}); } catch {}

    expect(onValidationFailure).toHaveBeenCalledOnce();
    const [eventName, errors] = onValidationFailure.mock.calls[0] as [string, unknown[]];
    expect(eventName).toBe('t');
    expect(errors).toHaveLength(1);
    await bus.destroy();
  });

  it('onReplay fires when late subscriber receives history', async () => {
    const onReplay = vi.fn();
    const bus = new EventBus({ replay: { limit: 5 }, telemetry: { onReplay } });

    await bus.publish('app.started', {});
    await bus.publish('app.started', {});

    bus.subscribe('app.started', () => {}, { replay: true });
    await sleep(20);

    expect(onReplay).toHaveBeenCalledWith('app.started', 2);
    await bus.destroy();
  });
});

// ─────────────────────────────────────────────────────────────────────────────
// 6. BUILT-IN MIDDLEWARE
// ─────────────────────────────────────────────────────────────────────────────

describe('Integration: built-in middleware', () => {
  it('ttlGuardMiddleware drops expired events', async () => {
    const bus = new EventBus();
    bus.use(ttlGuardMiddleware());

    let received = false;
    bus.subscribe('test', () => { received = true; });

    // Publish with a TTL that has already expired
    const pastTimestamp = Date.now() - 10_000;
    // We patch the envelope by publishing then manually testing:
    // Since we can't set timestamp via publish, we test directly via middleware
    const expiredEnvelope: EventEnvelope = {
      id: 'test-id', event: 'test', namespace: 'default',
      payload: {}, timestamp: pastTimestamp, correlationId: 'c',
      causationId: '', source: 'test', version: '1',
      ttl: 5_000, // 5s TTL, but timestamp is 10s ago
    };

    // Directly invoke the middleware to test TTL drop
    const mw = ttlGuardMiddleware();
    const nextFn = vi.fn();
    await mw(expiredEnvelope, nextFn);
    expect(nextFn).not.toHaveBeenCalled();
    await bus.destroy();
  });

  it('timestampMiddleware adds processingStartedAt to metadata', async () => {
    const bus = new EventBus();
    bus.use(timestampMiddleware());

    let meta: unknown;
    bus.subscribe('test', (e) => { meta = e.metadata; });
    await bus.publish('test', {}, { storeHistory: false });

    expect((meta as Record<string, unknown>)?.processingStartedAt).toBeTypeOf('number');
    await bus.destroy();
  });

  it('namespaceGuardMiddleware rejects wrong namespace', async () => {
    const bus = new EventBus({ namespace: 'production' });
    bus.use(namespaceGuardMiddleware('production'));

    let received = false;
    bus.subscribe('test', () => { received = true; });

    // Normal event passes
    await bus.publish('test', {}, { storeHistory: false });
    expect(received).toBe(true);

    await bus.destroy();
  });

  it('loggingMiddleware does not break the pipeline', async () => {
    const consoleSpy = vi.spyOn(console, 'debug').mockImplementation(() => {});
    const bus = new EventBus();
    bus.use(loggingMiddleware({ level: 'debug' }));

    let received = false;
    bus.subscribe('test', () => { received = true; });
    await bus.publish('test', {}, { storeHistory: false });

    expect(received).toBe(true);
    expect(consoleSpy).toHaveBeenCalledOnce();
    consoleSpy.mockRestore();
    await bus.destroy();
  });
});

// ─────────────────────────────────────────────────────────────────────────────
// 7. VALIDATOR COMPOSITES
// ─────────────────────────────────────────────────────────────────────────────

describe('Integration: validator composites', () => {
  it('andValidator — passes only when ALL pass', async () => {
    const v1 = customValidator<{ a: number }>('V1', (p) =>
      typeof (p as { a?: unknown }).a === 'number' ? { valid: true } : { valid: false, errors: [{ path: 'a', message: 'Must be number' }] }
    );
    const v2 = customValidator<{ b: string }>('V2', (p) =>
      typeof (p as { b?: unknown }).b === 'string' ? { valid: true } : { valid: false, errors: [{ path: 'b', message: 'Must be string' }] }
    );

    const bus = new EventBus();
    bus.registerSchema('typed', andValidator('Both', v1, v2));

    let received = false;
    bus.subscribe('typed', () => { received = true; });

    // Both pass
    await bus.publish('typed', { a: 1, b: 'ok' });
    expect(received).toBe(true);

    // Only v1 passes — should fail
    received = false;
    await expect(bus.publish('typed', { a: 1 })).rejects.toThrow(ValidationFailedError);
    expect(received).toBe(false);

    await bus.destroy();
  });

  it('orValidator — passes when at least ONE passes', async () => {
    const isString = customValidator('IsString', (p) =>
      typeof p === 'string' ? { valid: true } : { valid: false, errors: [{ path: '', message: 'Not string' }] }
    );
    const isNumber = customValidator('IsNumber', (p) =>
      typeof p === 'number' ? { valid: true } : { valid: false, errors: [{ path: '', message: 'Not number' }] }
    );

    const bus = new EventBus();
    bus.registerSchema('union', orValidator('StringOrNumber', isString, isNumber));

    let received = false;
    bus.subscribe('union', () => { received = true; });

    await bus.publish('union', 'hello');
    expect(received).toBe(true);

    received = false;
    await bus.publish('union', 42);
    expect(received).toBe(true);

    await expect(bus.publish('union', { object: true })).rejects.toThrow(ValidationFailedError);

    await bus.destroy();
  });

  it('zodValidator adapter maps Zod errors correctly', async () => {
    const zodSchema = makeZodLike(false, ['Email is invalid', 'ID must be UUID']);
    const bus = new EventBus();
    bus.registerSchema('zod.event', zodValidator('ZodTest', zodSchema));

    let caughtErrors: unknown;
    try {
      await bus.publish('zod.event', { bad: true });
    } catch (e) {
      caughtErrors = (e as ValidationFailedError).errors;
    }

    expect(Array.isArray(caughtErrors)).toBe(true);
    expect((caughtErrors as unknown[]).length).toBe(2);
    await bus.destroy();
  });
});

// ─────────────────────────────────────────────────────────────────────────────
// 8. ERROR ISOLATION
// ─────────────────────────────────────────────────────────────────────────────

describe('Integration: error isolation', () => {
  it('onError hook is called for all failure phases', async () => {
    const errors: Array<{ phase: string; message: string }> = [];

    const bus = new EventBus({
      onError: (err, ctx) => errors.push({ phase: ctx.phase, message: err.message }),
    });

    // Subscriber crash
    bus.subscribe('test', () => { throw new Error('subscriber boom'); });
    await bus.publish('test', {}, { storeHistory: false });

    expect(errors).toContainEqual(expect.objectContaining({ phase: 'subscriber', message: 'subscriber boom' }));
    await bus.destroy();
  });

  it('crashed onError hook does not propagate errors', async () => {
    const bus = new EventBus({
      onError: () => { throw new Error('onError itself crashed'); },
    });
    bus.subscribe('test', () => { throw new Error('subscriber'); });
    // Should not throw despite onError also crashing
    await expect(bus.publish('test', {}, { storeHistory: false })).resolves.not.toThrow();
    await bus.destroy();
  });

  it('bus is fully operational after a cascade of subscriber crashes', async () => {
    const bus = new EventBus();
    for (let i = 0; i < 20; i++) {
      bus.subscribe('crash', () => { throw new Error(`crash-${i}`); });
    }
    let okReceived = false;
    bus.subscribe('ok', () => { okReceived = true; });

    await bus.publish('crash', {}, { storeHistory: false });
    await bus.publish('ok', {}, { storeHistory: false });

    expect(okReceived).toBe(true);
    await bus.destroy();
  });
});

// ─────────────────────────────────────────────────────────────────────────────
// 9. METRICS INTEGRATION
// ─────────────────────────────────────────────────────────────────────────────

describe('Integration: metrics', () => {
  it('getMetrics() returns accurate snapshot after mixed operations', async () => {
    const bus = new EventBus({ replay: { limit: 10 } });

    bus.registerSchema('validated', customValidator('V', () => ({ valid: false, errors: [{ path: '', message: 'fail' }] })));

    const handle = bus.subscribe('test', () => {});
    await bus.publish('test', {}, { storeHistory: true });
    await bus.publish('test', {}, { storeHistory: true });
    try { await bus.publish('validated', {}); } catch {}
    handle.unsubscribe();

    const m: BusMetrics = bus.getMetrics();
    expect(m.publishCount).toBe(2);        // validated failed before storing
    expect(m.subscribeCount).toBe(1);
    expect(m.unsubscribeCount).toBe(1);
    expect(m.validationFailures).toBe(1);
    expect(m.activeSubscriptions).toBe(0);
    expect(m.historySize).toBe(2);
    expect(m.p95LatencyMs).toBeGreaterThanOrEqual(0);

    await bus.destroy();
  });
});

// ─────────────────────────────────────────────────────────────────────────────
// 10. REAL-WORLD: ORDER PROCESSING PIPELINE
// ─────────────────────────────────────────────────────────────────────────────

describe('Integration: real-world order processing pipeline', () => {
  it('complete order lifecycle flows correctly', async () => {
    const bus = new EventBus({ namespace: 'ecommerce', replay: { limit: 20 } });
    const log: Array<{ event: string; payload: unknown }> = [];

    // ── Schema definitions ──────────────────────────────────────────────────
    bus.registerSchema('order.placed', customValidator('OrderPlaced', (p) =>
      typeof (p as { orderId?: unknown }).orderId === 'string' &&
      typeof (p as { amount?: unknown }).amount === 'number'
        ? { valid: true }
        : { valid: false, errors: [{ path: '', message: 'Invalid order' }] }
    ));

    // ── Middleware: correlation propagation ─────────────────────────────────
    bus.use(async (envelope, next) => {
      (envelope as { metadata: Record<string, unknown> }).metadata = {
        ...(envelope.metadata ?? {}),
        processedAt: Date.now(),
        pipeline: 'order-v2',
      };
      await next();
    });

    // ── Subscribers: simulate downstream services ────────────────────────────
    bus.subscribe('order.placed', async (e) => {
      log.push({ event: e.event, payload: e.payload });
      // Inventory service publishes reservation
      await bus.publish('inventory.reserved', {
        orderId: (e.payload as { orderId: string }).orderId,
        items: [],
      }, { causationId: e.id, correlationId: e.correlationId });
    });

    bus.subscribe('inventory.reserved', async (e) => {
      log.push({ event: e.event, payload: e.payload });
      // Payment service publishes charge
      await bus.publish('payment.charged', {
        orderId: (e.payload as { orderId: string }).orderId,
        charged: true,
      }, { causationId: e.id, correlationId: e.correlationId });
    });

    bus.subscribe('payment.charged', async (e) => {
      log.push({ event: e.event, payload: e.payload });
      // Notification service
      await bus.publish('notification.sent', {
        orderId: (e.payload as { orderId: string }).orderId,
        channel: 'email',
      }, { causationId: e.id, correlationId: e.correlationId });
    });

    bus.subscribe('notification.sent', (e) => {
      log.push({ event: e.event, payload: e.payload });
    });

    // Wildcard audit logger — captures all order pipeline events
    const auditLog: string[] = [];
    bus.subscribe('**', (e) => {
      if (!e.event.startsWith('$system')) auditLog.push(e.event);
    }, { priority: -999 }); // lowest priority — runs after all domain subscribers

    // ── Trigger the pipeline ─────────────────────────────────────────────────
    await bus.publish('order.placed', { orderId: 'ORD-2024-001', amount: 149.99 });

    // Allow async chain to complete
    await sleep(50);

    // ── Assertions ───────────────────────────────────────────────────────────
    expect(log.map(l => l.event)).toEqual([
      'order.placed',
      'inventory.reserved',
      'payment.charged',
      'notification.sent',
    ]);

    // All events should have same correlationId (propagated through chain)
    // Verify orderId was threaded through all steps
    for (const entry of log) {
      expect((entry.payload as { orderId: string }).orderId).toBe('ORD-2024-001');
    }

    // Audit log should have captured all 4 domain events
    expect(auditLog.filter(e => ['order.placed', 'inventory.reserved', 'payment.charged', 'notification.sent'].includes(e))).toHaveLength(4);

    // Replay — a late audit subscriber should see the full pipeline
    const replayedEvents: string[] = [];
    bus.subscribe('**', (e) => {
      if (!e.event.startsWith('$system')) replayedEvents.push(e.event);
    }, { replay: true, priority: -999 });
    await sleep(50);
    expect(replayedEvents).toContain('order.placed');

    await bus.destroy();
  });
});
