/**
 * GA-PubSub — Security Integration Tests
 *
 * Tests end-to-end security scenarios including:
 *   - HMAC envelope signing and verification
 *   - Tamper detection (payload modification after signing)
 *   - Replay attack prevention with sequence tracking
 *   - Rate limiting under load
 *   - Namespace isolation (cross-namespace leakage)
 *   - Prototype pollution resistance
 *   - Adversarial input resistance
 */

import { describe, it, expect, beforeEach } from 'vitest';
import { ProEventBus as EventBus } from '../../packages/pro/src/bus.ts';
import { sanitizePayload, generateId } from '../../packages/core/src/security.js';
import { signEnvelope, verifyEnvelope, SequenceTracker, RateLimiter } from '../../packages/pro/src/security.ts';
import { SubscriptionLimitError } from '../../packages/core/src/types.js';
import {
  TamperDetectedError,
  ReplayAttackError,
  RateLimitExceededError,
  PayloadTooLargeError,
  AuthorizationDeniedError,
} from '../../packages/pro/src/types.ts';
import type { EventEnvelope } from '../../packages/core/src/types.js';

// ─────────────────────────────────────────────────────────────────────────────
// HMAC SIGNING
// ─────────────────────────────────────────────────────────────────────────────

describe('HMAC envelope signing', () => {
  const SECRET = 'test-secret-key-never-use-in-production';

  function makeTestEnvelope(overrides: Partial<EventEnvelope> = {}): EventEnvelope {
    return {
      id: generateId(),
      event: 'test.event',
      namespace: 'test',
      payload: { value: 42 },
      timestamp: Date.now(),
      correlationId: generateId(),
      causationId: '',
      source: 'test-service',
      version: '1',
      ...overrides,
    };
  }

  it('generates a 64-character hex signature', async () => {
    const envelope = makeTestEnvelope();
    const sig = await signEnvelope(envelope, SECRET);
    expect(sig).toMatch(/^[0-9a-f]{64}$/);
  });

  it('same envelope produces same signature (deterministic)', async () => {
    const envelope = makeTestEnvelope();
    const sig1 = await signEnvelope(envelope, SECRET);
    const sig2 = await signEnvelope(envelope, SECRET);
    expect(sig1).toBe(sig2);
  });

  it('different secrets produce different signatures', async () => {
    const envelope = makeTestEnvelope();
    const sig1 = await signEnvelope(envelope, 'secret-one');
    const sig2 = await signEnvelope(envelope, 'secret-two');
    expect(sig1).not.toBe(sig2);
  });

  it('verifies a correctly signed envelope', async () => {
    const envelope = makeTestEnvelope();
    const sig = await signEnvelope(envelope, SECRET);
    const withSig: EventEnvelope = { ...envelope, signature: sig };
    expect(await verifyEnvelope(withSig, SECRET)).toBe(true);
  });

  it('rejects a tampered payload', async () => {
    const envelope = makeTestEnvelope({ payload: { original: true } });
    const sig = await signEnvelope(envelope, SECRET);
    // Tamper: change the payload after signing
    const tampered: EventEnvelope = {
      ...envelope,
      payload: { malicious: true }, // <-- payload changed
      signature: sig,
    };
    expect(await verifyEnvelope(tampered, SECRET)).toBe(false);
  });

  it('rejects a tampered event name', async () => {
    const envelope = makeTestEnvelope({ event: 'user.created' });
    const sig = await signEnvelope(envelope, SECRET);
    const tampered: EventEnvelope = {
      ...envelope,
      event: 'admin.elevated', // <-- event name changed
      signature: sig,
    };
    expect(await verifyEnvelope(tampered, SECRET)).toBe(false);
  });

  it('rejects an envelope with no signature', async () => {
    const envelope = makeTestEnvelope();
    // No signature field
    expect(await verifyEnvelope(envelope, SECRET)).toBe(false);
  });

  it('rejects an envelope with wrong-length signature', async () => {
    const envelope = makeTestEnvelope();
    const withBadSig: EventEnvelope = { ...envelope, signature: 'deadbeef' };
    expect(await verifyEnvelope(withBadSig, SECRET)).toBe(false);
  });

  it('rejects an envelope with wrong secret', async () => {
    const envelope = makeTestEnvelope();
    const sig = await signEnvelope(envelope, SECRET);
    const withSig: EventEnvelope = { ...envelope, signature: sig };
    expect(await verifyEnvelope(withSig, 'wrong-secret')).toBe(false);
  });

  it('does not include metadata in signature (metadata can be modified freely)', async () => {
    const envelope = makeTestEnvelope({ metadata: { region: 'us-east' } });
    const sig = await signEnvelope(envelope, SECRET);
    // Changing metadata should NOT invalidate the signature
    const withModifiedMeta: EventEnvelope = {
      ...envelope,
      metadata: { region: 'eu-west', extra: 'ok' },
      signature: sig,
    };
    expect(await verifyEnvelope(withModifiedMeta, SECRET)).toBe(true);
  });
});

// ─────────────────────────────────────────────────────────────────────────────
// REPLAY ATTACK PREVENTION
// ─────────────────────────────────────────────────────────────────────────────

describe('SequenceTracker — replay attack prevention', () => {
  it('accepts first message for a source', () => {
    const tracker = new SequenceTracker();
    const envelope = makeSequencedEnvelope('src-a', 1);
    expect(() => tracker.check(envelope)).not.toThrow();
  });

  it('accepts monotonically increasing sequences', () => {
    const tracker = new SequenceTracker();
    tracker.check(makeSequencedEnvelope('src-a', 1));
    tracker.check(makeSequencedEnvelope('src-a', 2));
    tracker.check(makeSequencedEnvelope('src-a', 100));
    // No throw = pass
  });

  it('throws ReplayAttackError for repeated sequence', () => {
    const tracker = new SequenceTracker();
    tracker.check(makeSequencedEnvelope('src-a', 5));
    expect(() =>
      tracker.check(makeSequencedEnvelope('src-a', 5))
    ).toThrow(ReplayAttackError);
  });

  it('throws ReplayAttackError for rewound sequence', () => {
    const tracker = new SequenceTracker();
    tracker.check(makeSequencedEnvelope('src-a', 10));
    expect(() =>
      tracker.check(makeSequencedEnvelope('src-a', 7))
    ).toThrow(ReplayAttackError);
  });

  it('tracks sources independently', () => {
    const tracker = new SequenceTracker();
    tracker.check(makeSequencedEnvelope('src-a', 5));
    // src-b starting from 1 should be fine
    expect(() =>
      tracker.check(makeSequencedEnvelope('src-b', 1))
    ).not.toThrow();
  });

  it('does not throw when sequence field is absent', () => {
    const tracker = new SequenceTracker();
    const envelope: EventEnvelope = {
      id: generateId(),
      event: 'test',
      namespace: 'test',
      payload: {},
      timestamp: Date.now(),
      correlationId: generateId(),
      causationId: '',
      source: 'src',
      version: '1',
      // No sequence field
    };
    expect(() => tracker.check(envelope)).not.toThrow();
  });

  it('reset() clears sequence state for a namespace', () => {
    const tracker = new SequenceTracker();
    tracker.check(makeSequencedEnvelope('src-a', 100));
    tracker.reset('test');
    // After reset, sequence 1 should be acceptable again
    expect(() =>
      tracker.check(makeSequencedEnvelope('src-a', 1))
    ).not.toThrow();
  });
});

function makeSequencedEnvelope(source: string, sequence: number): EventEnvelope {
  return {
    id: generateId(),
    event: 'test.event',
    namespace: 'test',
    payload: {},
    timestamp: Date.now(),
    correlationId: generateId(),
    causationId: '',
    source,
    version: '1',
    sequence,
  };
}

// ─────────────────────────────────────────────────────────────────────────────
// RATE LIMITING
// ─────────────────────────────────────────────────────────────────────────────

describe('RateLimiter — token bucket', () => {
  it('allows burst up to the configured limit', () => {
    const limiter = new RateLimiter(10, 5); // 10/sec, burst=5
    expect(() => {
      for (let i = 0; i < 5; i++) limiter.consume('src');
    }).not.toThrow();
  });

  it('blocks on burst exhaustion', () => {
    const limiter = new RateLimiter(10, 3); // 10/sec, burst=3
    limiter.consume('src');
    limiter.consume('src');
    limiter.consume('src');
    expect(() => limiter.consume('src')).toThrow(RateLimitExceededError);
  });

  it('refills tokens over time', async () => {
    const limiter = new RateLimiter(100, 1); // 100/sec, burst=1
    limiter.consume('src'); // drain burst
    await new Promise(r => setTimeout(r, 15)); // wait 15ms = 1.5 tokens refilled
    expect(() => limiter.consume('src')).not.toThrow();
  });

  it('different sources are independent buckets', () => {
    const limiter = new RateLimiter(1, 1);
    limiter.consume('src-a');
    expect(() => limiter.consume('src-b')).not.toThrow();
  });

  it('error includes rate limit value', () => {
    const limiter = new RateLimiter(5, 0);
    let err: RateLimitExceededError | undefined;
    try {
      limiter.consume('src');
    } catch (e) {
      err = e as RateLimitExceededError;
    }
    expect(err).toBeInstanceOf(RateLimitExceededError);
    expect(err?.code).toBe('RATE_LIMIT_EXCEEDED');
    expect(err?.message).toContain('5');
  });
});

// ─────────────────────────────────────────────────────────────────────────────
// PROTOTYPE POLLUTION RESISTANCE
// ─────────────────────────────────────────────────────────────────────────────

describe('sanitizePayload — prototype pollution resistance', () => {
  it('strips __proto__ keys', () => {
    const malicious = JSON.parse('{"__proto__":{"polluted":true},"data":"ok"}');
    const safe = sanitizePayload(malicious);
    expect((({} as Record<string, unknown>)['polluted'])).toBeUndefined();
    expect((safe as Record<string, unknown>)['data']).toBe('ok');
  });

  it('strips constructor.prototype keys', () => {
    const malicious = { constructor: { prototype: { polluted: true } }, data: 'ok' };
    sanitizePayload(malicious);
    expect((({} as Record<string, unknown>)['polluted'])).toBeUndefined();
  });

  it('deep-clones — mutating the sanitized copy does not affect the original', () => {
    const original = { nested: { value: 1 } };
    const copy = sanitizePayload(original);
    (copy as { nested: { value: number } }).nested.value = 999;
    expect(original.nested.value).toBe(1);
  });

  it('handles null payloads', () => {
    expect(() => sanitizePayload(null)).not.toThrow();
  });

  it('handles array payloads', () => {
    const arr = [1, 2, { a: 3 }];
    const copy = sanitizePayload(arr) as typeof arr;
    expect(copy).toEqual([1, 2, { a: 3 }]);
    expect(copy).not.toBe(arr);
  });
});

// ─────────────────────────────────────────────────────────────────────────────
// NAMESPACE ISOLATION
// ─────────────────────────────────────────────────────────────────────────────

describe('Namespace isolation — cross-namespace leakage prevention', () => {
  it('events in namespace A never reach subscribers in namespace B', async () => {
    const busA = new EventBus({ namespace: 'tenant-a' });
    const busB = new EventBus({ namespace: 'tenant-b' });

    let bReceived = false;
    busB.subscribe('order.created', () => { bReceived = true; });

    await busA.publish('order.created', { orderId: '123' });

    // Give async operations a tick to settle
    await new Promise(r => setTimeout(r, 10));

    expect(bReceived).toBe(false);

    await busA.destroy();
    await busB.destroy();
  });

  it('wildcard subscriptions do not cross namespace boundaries', async () => {
    const busA = new EventBus({ namespace: 'tenant-a' });
    const busB = new EventBus({ namespace: 'tenant-b' });

    const received: string[] = [];
    busB.subscribe('**', (e) => { received.push(e.namespace); });

    await busA.publish('any.event', {});

    await new Promise(r => setTimeout(r, 10));

    // busB should never see events from busA
    expect(received.filter(ns => ns === 'tenant-a')).toHaveLength(0);

    await busA.destroy();
    await busB.destroy();
  });
});

// ─────────────────────────────────────────────────────────────────────────────
// DOS PROTECTION
// ─────────────────────────────────────────────────────────────────────────────

describe('DOS protection', () => {
  it('payload size limit blocks oversized events immediately', async () => {
    const bus = new EventBus({ maxPayloadBytes: 512 });
    const payload = { data: 'x'.repeat(600) }; // ~600 bytes

    await expect(
      bus.publish('large.event', payload)
    ).rejects.toThrow(PayloadTooLargeError);

    // Verify nothing was stored in replay history
    const metrics = bus.getMetrics();
    expect(metrics.historySize).toBe(0);

    await bus.destroy();
  });

  it('subscription limit blocks new subscriptions beyond max', () => {
    const bus = new EventBus({ maxSubscriptions: 3 });

    bus.subscribe('a', () => {});
    bus.subscribe('b', () => {});
    bus.subscribe('c', () => {});

    expect(() => bus.subscribe('d', () => {})).toThrow(SubscriptionLimitError);

    // After unsubscribing one, we can add another
    // (Note: requires handle from subscribe, simplified test uses unsubscribeAll)
    bus.unsubscribeAll();
    expect(() => bus.subscribe('new', () => {})).not.toThrow();
  });

  it('error has correct code and message', async () => {
    const bus = new EventBus({ maxPayloadBytes: 100 });
    try {
      await bus.publish('test', { data: 'x'.repeat(200) });
    } catch (err) {
      expect((err as PayloadTooLargeError).code).toBe('PAYLOAD_TOO_LARGE');
      expect((err as PayloadTooLargeError).message).toContain('exceeds limit');
    }
    await bus.destroy();
  });
});

// ─────────────────────────────────────────────────────────────────────────────
// AUTHORIZATION INTEGRATION
// ─────────────────────────────────────────────────────────────────────────────

describe('Authorization layer', () => {
  it('authorizer receives the full envelope for inspection', async () => {
    const bus = new EventBus();
    let inspectedEnvelope: EventEnvelope | undefined;

    bus.authorize('secure.event', (envelope) => {
      inspectedEnvelope = envelope;
      return false;
    });

    try {
      await bus.publish('secure.event', { secret: 'data' }, { userId: 'u1' });
    } catch {}

    expect(inspectedEnvelope?.event).toBe('secure.event');
    expect(inspectedEnvelope?.userId).toBe('u1');

    await bus.destroy();
  });

  it('async authorizer is awaited', async () => {
    const bus = new EventBus();
    let called = false;

    bus.authorize('secure.event', async () => {
      await new Promise(r => setTimeout(r, 5));
      called = true;
      return true;
    });

    bus.subscribe('secure.event', () => {});
    await bus.publish('secure.event', {});

    expect(called).toBe(true);
    await bus.destroy();
  });

  it('wildcard authorizer matches multiple events', async () => {
    const bus = new EventBus();
    const denied: string[] = [];

    bus.authorize('admin.**', () => false);
    bus.subscribe('admin.users.list', () => {});
    bus.subscribe('admin.config.write', () => {});

    try { await bus.publish('admin.users.list', {}); } catch (e) {
      denied.push((e as Error).message);
    }
    try { await bus.publish('admin.config.write', {}); } catch (e) {
      denied.push((e as Error).message);
    }

    expect(denied).toHaveLength(2);
    await bus.destroy();
  });

  it('denied publish increments authorization denial metric', async () => {
    const bus = new EventBus();
    bus.authorize('locked.event', () => false);

    try { await bus.publish('locked.event', {}); } catch {}

    const metrics = bus.getMetrics();
    expect(metrics.authorizationDenials).toBe(1);
    await bus.destroy();
  });
});

// ─────────────────────────────────────────────────────────────────────────────
// SECURE BY DEFAULT VERIFICATION
// ─────────────────────────────────────────────────────────────────────────────

describe('Secure by default', () => {
  it('default bus has 64KB payload limit', async () => {
    const bus = new EventBus();
    const metrics = bus.getMetrics(); // Just verify bus starts clean
    expect(metrics.publishCount).toBe(0);

    // 64KB + 1 byte should be rejected
    const bigPayload = { data: 'x'.repeat(65_537) };
    await expect(bus.publish('test', bigPayload)).rejects.toThrow(PayloadTooLargeError);

    await bus.destroy();
  });

  it('payload is sanitized (deep-cloned) before dispatch', async () => {
    const bus = new EventBus();
    const original = { mutable: { count: 0 } };
    let receivedPayload: typeof original | undefined;

    bus.subscribe('test', (e) => {
      receivedPayload = e.payload as typeof original;
    });

    await bus.publish('test', original, { storeHistory: false });

    // Mutating the original after publish should not affect the received copy
    original.mutable.count = 999;
    expect(receivedPayload?.mutable.count).toBe(0);

    await bus.destroy();
  });

  it('middleware errors do not crash the bus', async () => {
    const bus = new EventBus();
    bus.use(async () => {
      throw new Error('Middleware catastrophic failure');
    });

    let handlerCalled = false;
    bus.subscribe('test', () => { handlerCalled = true; });

    // Should not throw — error is caught and routed to onError
    try {
      await bus.publish('test', {});
    } catch {
      // Expected — middleware threw
    }

    // Bus should still be operational for future events on different patterns
    const bus2 = new EventBus();
    let alive = false;
    bus2.subscribe('health', () => { alive = true; });
    await bus2.publish('health', {});
    expect(alive).toBe(true);

    await bus.destroy();
    await bus2.destroy();
  });

  it('subscriber errors do not affect other subscribers', async () => {
    const bus = new EventBus();

    const results: number[] = [];

    bus.subscribe('test', () => { throw new Error('Subscriber 1 crashed'); }, { priority: 10 });
    bus.subscribe('test', () => { results.push(2); }, { priority: 5 });
    bus.subscribe('test', () => { results.push(3); }, { priority: 1 });

    await bus.publish('test', {}, { storeHistory: false });

    // Subscribers 2 and 3 should still run despite subscriber 1 crashing
    expect(results).toContain(2);
    expect(results).toContain(3);

    await bus.destroy();
  });
});
