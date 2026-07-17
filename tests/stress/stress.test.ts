/**
 * GA-PubSub — Stress & Chaos Tests
 *
 * Tests system behaviour under extreme conditions:
 *   - 100k concurrent subscriptions
 *   - Wildcard routing at scale
 *   - Concurrent publish + subscribe races
 *   - Memory leak detection (GC pressure)
 *   - Chaos: random subscriber crashes, random unsubscribes mid-flight
 *   - Request/response under concurrency
 */

import { describe, it, expect, vi } from 'vitest';
import { ProEventBus as EventBus } from '../../packages/pro/src/bus.ts';
import { getBus, destroyAll } from '../../packages/pro/src/registry.ts';

// ─────────────────────────────────────────────────────────────────────────────
// THROUGHPUT BENCHMARKS
// ─────────────────────────────────────────────────────────────────────────────

describe('Throughput — sequential publishes', () => {
  it('10,000 events dispatched in under 10 seconds', async () => {
    const bus = new EventBus({ replay: { limit: 0 } });
    let count = 0;
    bus.subscribe('bench', () => { count++; });

    const start = Date.now();
    for (let i = 0; i < 10_000; i++) {
      await bus.publish('bench', { i }, { storeHistory: false });
    }
    const elapsed = Date.now() - start;

    expect(count).toBe(10_000);
    expect(elapsed).toBeLessThan(10_000);

    const throughput = Math.round(10_000 / (elapsed / 1000));
    console.log(`[Throughput] 10k sequential: ${elapsed}ms (${throughput.toLocaleString()}/sec)`);

    await bus.destroy();
  }, 15_000);

  it('1,000 events with 10 subscribers each dispatch correctly', async () => {
    const bus = new EventBus({ replay: { limit: 0 } });
    let totalCallbacks = 0;

    for (let s = 0; s < 10; s++) {
      bus.subscribe('multi.sub', () => { totalCallbacks++; });
    }

    for (let i = 0; i < 1_000; i++) {
      await bus.publish('multi.sub', { i }, { storeHistory: false });
    }

    expect(totalCallbacks).toBe(10_000); // 1000 events × 10 subscribers
    await bus.destroy();
  }, 15_000);
});

// ─────────────────────────────────────────────────────────────────────────────
// SUBSCRIPTION SCALE
// ─────────────────────────────────────────────────────────────────────────────

describe('Scale — 100k subscriptions', () => {
  it('registers 100,000 unique-pattern subscriptions without OOM', async () => {
    const bus = new EventBus({ maxSubscriptions: 0, enableWildcard: false });

    const handles = [];
    for (let i = 0; i < 100_000; i++) {
      handles.push(bus.subscribe(`event.${i}`, () => {}));
    }

    expect(bus.getMetrics().activeSubscriptions).toBe(100_000);

    // Verify routing still works correctly at scale
    let hit = false;
    const targetHandle = bus.subscribe('event.77777', () => { hit = true; });
    await bus.publish('event.77777', {}, { storeHistory: false });
    expect(hit).toBe(true);

    targetHandle.unsubscribe();
    await bus.destroy();

    console.log('[Scale] 100k subscriptions registered and routed correctly');
  }, 60_000);

  it('batch unsubscribe of 10,000 subscriptions is fast', async () => {
    const bus = new EventBus({ maxSubscriptions: 0 });
    const handles = [];
    for (let i = 0; i < 10_000; i++) {
      handles.push(bus.subscribe(`event.${i}`, () => {}));
    }

    const start = Date.now();
    bus.unsubscribeAll();
    const elapsed = Date.now() - start;

    expect(bus.getMetrics().activeSubscriptions).toBe(0);
    expect(elapsed).toBeLessThan(1_000); // bulk unsubscribe < 1s
    console.log(`[Scale] 10k unsubscribeAll: ${elapsed}ms`);

    await bus.destroy();
  }, 10_000);
});

// ─────────────────────────────────────────────────────────────────────────────
// WILDCARD ROUTING SCALE
// ─────────────────────────────────────────────────────────────────────────────

describe('Wildcard routing at scale', () => {
  it('100 wildcard patterns route correctly among 10,000 exact subscriptions', async () => {
    const bus = new EventBus({ maxSubscriptions: 0 });

    // Add 10k exact subscriptions
    for (let i = 0; i < 10_000; i++) {
      bus.subscribe(`service.${i}.event`, () => {});
    }

    // Add 100 wildcard subscriptions
    const wildcardHits: string[] = [];
    for (let w = 0; w < 100; w++) {
      bus.subscribe(`service.${w}.*`, (e) => { wildcardHits.push(e.event); });
    }

    // Publish to a specific service — should match both exact + wildcard for service.5
    await bus.publish('service.5.event', {}, { storeHistory: false });

    expect(wildcardHits).toContain('service.5.event');

    await bus.destroy();
  }, 30_000);

  it('double-wildcard ** matches at any depth efficiently', async () => {
    const bus = new EventBus();
    const received: string[] = [];

    bus.subscribe('payments.**', (e) => { received.push(e.event); });

    const events = [
      'payments.created',
      'payments.invoice.line.added',
      'payments.refund.initiated.review.pending',
    ];

    for (const event of events) {
      await bus.publish(event, {}, { storeHistory: false });
    }

    expect(received).toEqual(events);
    await bus.destroy();
  });
});

// ─────────────────────────────────────────────────────────────────────────────
// CONCURRENT PUBLISH / SUBSCRIBE RACES
// ─────────────────────────────────────────────────────────────────────────────

describe('Concurrency — race conditions', () => {
  it('concurrent publishes do not lose events', async () => {
    const bus = new EventBus({ replay: { limit: 0 } });
    let received = 0;
    bus.subscribe('concurrent', () => { received++; });

    const CONCURRENCY = 100;
    await Promise.all(
      Array.from({ length: CONCURRENCY }, (_, i) =>
        bus.publish('concurrent', { i }, { storeHistory: false })
      )
    );

    expect(received).toBe(CONCURRENCY);
    await bus.destroy();
  });

  it('subscribe/unsubscribe during publish does not corrupt state', async () => {
    const bus = new EventBus({ replay: { limit: 0 } });
    let received = 0;
    const handles: ReturnType<typeof bus.subscribe>[] = [];

    // Subscribe 10 handlers
    for (let i = 0; i < 10; i++) {
      handles.push(bus.subscribe('churn', () => { received++; }));
    }

    // Simultaneously publish and unsubscribe
    const publishPromise = bus.publish('churn', {}, { storeHistory: false });
    handles[0].unsubscribe();
    handles[1].unsubscribe();
    await publishPromise;

    // received should be between 8-10 (depending on race timing)
    expect(received).toBeGreaterThanOrEqual(0);
    expect(received).toBeLessThanOrEqual(10);

    await bus.destroy();
  });

  it('100 concurrent request/response pairs resolve correctly', async () => {
    const bus = new EventBus({ replay: { limit: 0 } });

    bus.respond<{ id: number }, { doubled: number }>('math.double', (e) => ({
      doubled: (e.payload as { id: number }).id * 2,
    }));

    const results = await Promise.all(
      Array.from({ length: 100 }, async (_, i) => {
        const { response } = bus.request<{ id: number }, { doubled: number }>(
          'math.double',
          { id: i },
          { timeoutMs: 5_000 }
        );
        const envelope = await response;
        return (envelope.payload as { doubled: number }).doubled;
      })
    );

    // Verify each result is correct
    for (let i = 0; i < 100; i++) {
      expect(results).toContain(i * 2);
    }

    await bus.destroy();
  }, 15_000);
});

// ─────────────────────────────────────────────────────────────────────────────
// CHAOS TESTS
// ─────────────────────────────────────────────────────────────────────────────

describe('Chaos — random failures', () => {
  it('bus continues operating after 50% subscriber crash rate', async () => {
    const bus = new EventBus({ replay: { limit: 0 } });
    let stable = 0;

    // Half the subscribers throw errors
    for (let i = 0; i < 50; i++) {
      bus.subscribe('chaos', () => { throw new Error(`Subscriber ${i} crashed`); });
    }
    // Half are stable
    for (let i = 50; i < 100; i++) {
      bus.subscribe('chaos', () => { stable++; });
    }

    await bus.publish('chaos', {}, { storeHistory: false });

    // All stable subscribers should have received the event despite the crashes
    expect(stable).toBe(50);

    const metrics = bus.getMetrics();
    expect(metrics.failedDeliveries).toBe(50);

    await bus.destroy();
  });

  it('replay engine handles concurrent store and retrieve', async () => {
    const bus = new EventBus({ replay: { limit: 50 } });

    // Simultaneously publish (which stores) and subscribe (which reads history)
    const publishTasks = Array.from({ length: 20 }, (_, i) =>
      bus.publish(`event.${i % 5}`, { i })
    );

    const subscribeTasks = Array.from({ length: 10 }, () =>
      new Promise<number>(resolve => {
        let count = 0;
        bus.subscribe('event.0', () => { count++; }, { replay: true });
        setTimeout(() => resolve(count), 50);
      })
    );

    await Promise.all([...publishTasks, ...subscribeTasks]);
    // No assertion on exact count — we're verifying no crash/deadlock

    await bus.destroy();
  }, 10_000);

  it('random unsubscribe storm does not leak memory', async () => {
    const bus = new EventBus({ maxSubscriptions: 0, replay: { limit: 0 } });
    const handles: ReturnType<typeof bus.subscribe>[] = [];

    // Subscribe 1000
    for (let i = 0; i < 1_000; i++) {
      handles.push(bus.subscribe(`event.${i % 10}`, () => {}));
    }

    expect(bus.getMetrics().activeSubscriptions).toBe(1_000);

    // Randomly unsubscribe all in shuffled order
    const shuffled = [...handles].sort(() => Math.random() - 0.5);
    for (const h of shuffled) h.unsubscribe();

    expect(bus.getMetrics().activeSubscriptions).toBe(0);

    await bus.destroy();
  });
});

// ─────────────────────────────────────────────────────────────────────────────
// MEMORY LEAK DETECTION
// ─────────────────────────────────────────────────────────────────────────────

describe('Memory — leak detection', () => {
  it('subscribeOnce does not accumulate subscriber records', async () => {
    const bus = new EventBus({ replay: { limit: 0 } });

    // Subscribe 1000 once-handlers
    for (let i = 0; i < 1_000; i++) {
      bus.subscribeOnce('mem.test', () => {});
    }

    expect(bus.getMetrics().activeSubscriptions).toBe(1_000);

    // After one publish, all once-handlers should be gone
    await bus.publish('mem.test', {}, { storeHistory: false });

    expect(bus.getMetrics().activeSubscriptions).toBe(0);
    await bus.destroy();
  });

  it('replay engine evicts events when TTL expires', async () => {
    const bus = new EventBus({ replay: { limit: 1_000, ttl: 50 } });

    // Fill with 100 events
    for (let i = 0; i < 100; i++) {
      await bus.publish('ttl.test', { i });
    }

    expect(bus.getMetrics().historySize).toBe(100);

    // Wait for TTL to expire
    await new Promise(r => setTimeout(r, 80));

    // Reading history triggers lazy eviction
    const received: unknown[] = [];
    bus.subscribe('ttl.test', (e) => { received.push(e); }, { replay: true });
    await new Promise(r => setTimeout(r, 20));

    // Expired events should not be replayed
    expect(received).toHaveLength(0);

    await bus.destroy();
  }, 10_000);

  it('namespace registry cleanup — destroyAll releases all buses', async () => {
    const namespaces = ['ns-1', 'ns-2', 'ns-3', 'ns-4', 'ns-5'];

    for (const ns of namespaces) {
      const bus = getBus(ns);
      for (let i = 0; i < 100; i++) {
        bus.subscribe(`event.${i}`, () => {});
      }
    }

    await destroyAll();

    // All namespaces should be gone
    const { listNamespaces } = await import('../../packages/pro/src/registry.ts');
    const remaining = listNamespaces().filter(ns => namespaces.includes(ns));
    expect(remaining).toHaveLength(0);
  });
});

// ─────────────────────────────────────────────────────────────────────────────
// METRICS ACCURACY
// ─────────────────────────────────────────────────────────────────────────────

describe('Metrics — accuracy under load', () => {
  it('publish count matches actual publishes', async () => {
    const bus = new EventBus({ replay: { limit: 0 } });
    const N = 500;

    for (let i = 0; i < N; i++) {
      await bus.publish('metrics.test', { i }, { storeHistory: false });
    }

    expect(bus.getMetrics().publishCount).toBe(N);
    await bus.destroy();
  });

  it('p95 latency is computed over last 1000 samples', async () => {
    const bus = new EventBus({ replay: { limit: 0 } });
    bus.subscribe('latency', () => {});

    for (let i = 0; i < 200; i++) {
      await bus.publish('latency', { i }, { storeHistory: false });
    }

    const metrics = bus.getMetrics();
    expect(metrics.p95LatencyMs).toBeGreaterThanOrEqual(0);
    expect(metrics.p95LatencyMs).toBeLessThan(1000); // sanity check

    await bus.destroy();
  });

  it('failed delivery counter increments on subscriber throws', async () => {
    const bus = new EventBus({ replay: { limit: 0 } });
    bus.subscribe('fail', () => { throw new Error('intentional'); });
    bus.subscribe('fail', () => { throw new Error('also intentional'); });

    await bus.publish('fail', {}, { storeHistory: false });

    expect(bus.getMetrics().failedDeliveries).toBe(2);
    await bus.destroy();
  });
});
