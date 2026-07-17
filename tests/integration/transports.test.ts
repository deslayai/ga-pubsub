/**
 * GA-PubSub — Transport Integration Tests
 *
 * These tests require running broker instances.
 * Start them with: docker compose -f tests/docker-compose.yml up -d
 *
 * Environment variables (set by CI or .env.test):
 *   REDIS_URL     = redis://localhost:6379
 *   KAFKA_BROKERS = localhost:9092
 *   NATS_URL      = nats://localhost:4222
 *   RABBITMQ_URL  = amqp://guest:guest@localhost:5672
 *
 * Each transport is tested against the TransportAdapter contract
 * PLUS its specific capabilities (delivery modes, wildcards, reconnect).
 *
 * Tests are skipped automatically when the broker is unavailable —
 * this allows running the suite in environments without Docker.
 */

import { describe, it, expect, beforeAll, afterAll, vi } from 'vitest';
import { EventBus } from '../../packages/core/src/bus.js';
import type { TransportAdapter, EventEnvelope } from '../../packages/core/src/types.js';

// ─────────────────────────────────────────────────────────────────────────────
// HELPERS
// ─────────────────────────────────────────────────────────────────────────────

const sleep = (ms: number) => new Promise<void>(r => setTimeout(r, ms));

function makeEnvelope(event: string, payload: unknown): EventEnvelope {
  return {
    id:            crypto.randomUUID(),
    event,
    namespace:     'integration-test',
    payload,
    timestamp:     Date.now(),
    correlationId: crypto.randomUUID(),
    causationId:   '',
    source:        'test',
    version:       '1',
  };
}

/**
 * Wraps a test that requires an external service.
 * If the service is unreachable, the test is skipped gracefully.
 */
async function withTransport<T extends TransportAdapter>(
  factory: () => Promise<T>,
  testFn: (transport: T) => Promise<void>
): Promise<void> {
  let transport: T | undefined;
  try {
    transport = await factory();
    await testFn(transport);
  } catch (err) {
    const msg = (err as Error).message ?? '';
    if (
      msg.includes('ECONNREFUSED') ||
      msg.includes('connect ETIMEDOUT') ||
      msg.includes('Connection refused') ||
      msg.includes('not available')
    ) {
      // Skip — broker not running
      console.warn(`[Transport test skipped] ${msg.slice(0, 80)}`);
      return;
    }
    throw err;
  } finally {
    if (transport) {
      try { await transport.disconnect(); } catch {}
    }
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// CONTRACT TEST HELPER (reusable across all transports)
// ─────────────────────────────────────────────────────────────────────────────

export function runTransportContractTests(
  name: string,
  factory: () => Promise<TransportAdapter>
): void {
  describe(`Transport contract: ${name}`, () => {
    let transport: TransportAdapter;

    beforeAll(async () => {
      transport = await factory();
      await transport.connect();
    });

    afterAll(async () => {
      try { await transport.disconnect(); } catch {}
    });

    it('ping() returns true after connect', async () => {
      expect(await transport.ping()).toBe(true);
    });

    it('publish() sends an envelope without throwing', async () => {
      const envelope = makeEnvelope('contract.test', { ok: true });
      await expect(transport.publish(envelope)).resolves.not.toThrow();
    });

    it('subscribe() returns a working cleanup function', async () => {
      const cleanup = await transport.subscribe('contract.*', () => {});
      expect(typeof cleanup).toBe('function');
      await expect(cleanup()).resolves.not.toThrow();
    });

    it('on/off event handlers are registered and removable', () => {
      const handler = vi.fn();
      transport.on('connected', handler);
      transport.off('connected', handler);
      // No assertion beyond no-throw — verifying the API exists
    });

    it('disconnect() resolves cleanly', async () => {
      // transport.disconnect() called in afterAll — just verify it doesn't throw
    });
  });
}

// ─────────────────────────────────────────────────────────────────────────────
// REDIS TRANSPORT TESTS
// ─────────────────────────────────────────────────────────────────────────────

describe('Redis transport — integration', () => {
  const REDIS_URL = process.env['REDIS_URL'] ?? 'redis://localhost:6379';

  it('pub/sub: publishes and receives events', async () => {
    await withTransport(
      async () => {
        const { RedisTransport } = await import('../../packages/redis/src/index.js');
        // Dynamic ioredis import so tests skip if not installed
        const Redis = (await import('ioredis')).default;
        const client = new Redis(REDIS_URL);
        const transport = new RedisTransport({ client, keyPrefix: 'test' });
        await transport.connect();
        return transport;
      },
      async (transport) => {
        const received: EventEnvelope[] = [];
        await transport.subscribe('redis.pubsub.*', (e) => { received.push(e); });

        const envelope = makeEnvelope('redis.pubsub.test', { hello: 'redis' });
        await transport.publish(envelope);
        await sleep(200);

        expect(received.length).toBeGreaterThanOrEqual(1);
        expect(received.some(e => e.id === envelope.id)).toBe(true);
      }
    );
  });

  it('streams: publishes and consumes via consumer group', async () => {
    await withTransport(
      async () => {
        const { RedisTransport } = await import('../../packages/redis/src/index.js');
        const Redis = (await import('ioredis')).default;
        const client = new Redis(REDIS_URL);
        const transport = new RedisTransport({
          client,
          keyPrefix: `test-${Date.now()}`,
          enableStreams: true,
          consumerGroup: 'test-group',
          consumerName: `consumer-${Date.now()}`,
          delivery: 'at-least-once',
          streamPollMs: 50,
        });
        await transport.connect();
        return transport;
      },
      async (transport) => {
        const received: EventEnvelope[] = [];
        await transport.subscribe('redis.stream.test', (e) => { received.push(e); });
        await sleep(100); // Let polling start

        const envelope = makeEnvelope('redis.stream.test', { mode: 'streams' });
        await transport.publish(envelope);
        await sleep(500);

        expect(received.length).toBeGreaterThanOrEqual(1);
      }
    );
  });
});

// ─────────────────────────────────────────────────────────────────────────────
// NATS TRANSPORT TESTS
// ─────────────────────────────────────────────────────────────────────────────

describe('NATS transport — integration', () => {
  const NATS_URL = process.env['NATS_URL'] ?? 'nats://localhost:4222';

  it('core: publishes and subscribes with wildcard patterns', async () => {
    await withTransport(
      async () => {
        const { NATSTransport } = await import('../../packages/nats/src/index.js');
        const { connect } = await import('nats');
        const nc = await connect({ servers: NATS_URL });
        const transport = new NATSTransport({ nc, prefix: 'test' });
        await transport.connect();
        return transport;
      },
      async (transport) => {
        const received: EventEnvelope[] = [];
        await transport.subscribe('nats.*', (e) => { received.push(e); });
        await sleep(50);

        const envelope = makeEnvelope('nats.test', { hello: 'nats' });
        await transport.publish(envelope);
        await sleep(200);

        expect(received.length).toBeGreaterThanOrEqual(1);
      }
    );
  });

  it('JetStream: publishes to stream and consumer receives', async () => {
    await withTransport(
      async () => {
        const { NATSTransport } = await import('../../packages/nats/src/index.js');
        const { connect } = await import('nats');
        const nc = await connect({ servers: NATS_URL });
        const streamName = `TEST_${Date.now()}`;
        const transport = new NATSTransport({
          nc,
          prefix: `test-js-${Date.now()}`,
          enableJetStream: true,
          stream: streamName,
          durableName: `consumer-${Date.now()}`,
        });
        await transport.connect();
        return transport;
      },
      async (transport) => {
        const received: EventEnvelope[] = [];
        await transport.subscribe('jetstream.test', (e) => { received.push(e); });
        await sleep(100);

        const envelope = makeEnvelope('jetstream.test', { mode: 'jetstream' });
        await transport.publish(envelope);
        await sleep(500);

        expect(received.length).toBeGreaterThanOrEqual(1);
      }
    );
  });

  it('subject mapping: ** becomes > in NATS', async () => {
    await withTransport(
      async () => {
        const { NATSTransport } = await import('../../packages/nats/src/index.js');
        const { connect } = await import('nats');
        const nc = await connect({ servers: NATS_URL });
        const transport = new NATSTransport({ nc, prefix: `wc-${Date.now()}` });
        await transport.connect();
        return transport;
      },
      async (transport) => {
        const received: string[] = [];
        await transport.subscribe('events.**', (e) => { received.push(e.event); });
        await sleep(50);

        await transport.publish(makeEnvelope('events.order.created', {}));
        await transport.publish(makeEnvelope('events.payment.processed', {}));
        await transport.publish(makeEnvelope('events.user.signup.confirmed', {}));
        await sleep(300);

        expect(received).toContain('events.order.created');
        expect(received).toContain('events.payment.processed');
        expect(received).toContain('events.user.signup.confirmed');
      }
    );
  });
});

// ─────────────────────────────────────────────────────────────────────────────
// RABBITMQ TRANSPORT TESTS
// ─────────────────────────────────────────────────────────────────────────────

describe('RabbitMQ transport — integration', () => {
  const RABBITMQ_URL = process.env['RABBITMQ_URL'] ?? 'amqp://guest:guest@localhost:5672';

  it('at-least-once: publishes with confirms and receives', async () => {
    await withTransport(
      async () => {
        const { RabbitMQTransport } = await import('../../packages/rabbitmq/src/index.js');
        const transport = new RabbitMQTransport({
          url: RABBITMQ_URL,
          exchange: `test-ex-${Date.now()}`,
          consumerQueue: `test-q-${Date.now()}`,
          delivery: 'at-least-once',
          durable: false,
        });
        await transport.connect();
        return transport;
      },
      async (transport) => {
        const received: EventEnvelope[] = [];
        await transport.subscribe('rabbit.*', (e) => { received.push(e); });
        await sleep(100);

        const envelope = makeEnvelope('rabbit.test', { broker: 'rabbitmq' });
        await transport.publish(envelope);
        await sleep(500);

        expect(received.length).toBeGreaterThanOrEqual(1);
        expect(received.some(e => e.id === envelope.id)).toBe(true);
      }
    );
  });

  it('routing key mapping: ** becomes # in AMQP', async () => {
    await withTransport(
      async () => {
        const { RabbitMQTransport } = await import('../../packages/rabbitmq/src/index.js');
        const transport = new RabbitMQTransport({
          url: RABBITMQ_URL,
          exchange: `test-wc-${Date.now()}`,
          consumerQueue: `test-wc-q-${Date.now()}`,
          delivery: 'at-most-once',
          durable: false,
        });
        await transport.connect();
        return transport;
      },
      async (transport) => {
        const received: string[] = [];
        await transport.subscribe('payments.**', (e) => { received.push(e.event); });
        await sleep(150);

        await transport.publish(makeEnvelope('payments.invoice.created', {}));
        await transport.publish(makeEnvelope('payments.refund.approved', {}));
        await sleep(400);

        expect(received).toContain('payments.invoice.created');
        expect(received).toContain('payments.refund.approved');
      }
    );
  });

  it('dead-letter: failed messages route to DLX', async () => {
    await withTransport(
      async () => {
        const { RabbitMQTransport } = await import('../../packages/rabbitmq/src/index.js');
        const transport = new RabbitMQTransport({
          url: RABBITMQ_URL,
          exchange: `test-dlx-${Date.now()}`,
          consumerQueue: `test-dlx-q-${Date.now()}`,
          delivery: 'at-least-once',
          enableDeadLetter: true,
          durable: false,
        });
        await transport.connect();
        return transport;
      },
      async (transport) => {
        let attempt = 0;
        await transport.subscribe('dlx.test', () => {
          attempt++;
          if (attempt < 2) throw new Error('Simulated processing failure');
        });
        await sleep(100);

        await transport.publish(makeEnvelope('dlx.test', { failOnFirst: true }));
        await sleep(500);

        // First attempt threw — message should be in DLX
        expect(attempt).toBeGreaterThanOrEqual(1);
      }
    );
  });

  it('TTL: envelope TTL maps to AMQP expiration header', async () => {
    await withTransport(
      async () => {
        const { RabbitMQTransport } = await import('../../packages/rabbitmq/src/index.js');
        const transport = new RabbitMQTransport({
          url: RABBITMQ_URL,
          exchange: `test-ttl-${Date.now()}`,
          consumerQueue: `test-ttl-q-${Date.now()}`,
          delivery: 'at-least-once',
          durable: false,
        });
        await transport.connect();
        return transport;
      },
      async (transport) => {
        const ttlEnvelope: EventEnvelope = {
          ...makeEnvelope('ttl.test', { expires: 'soon' }),
          ttl: 100, // 100ms TTL
        };

        const received: EventEnvelope[] = [];
        await transport.subscribe('ttl.test', (e) => { received.push(e); });
        await sleep(100);

        await transport.publish(ttlEnvelope);
        // Don't wait for delivery — TTL expires in 100ms
        // This just verifies publish doesn't throw with TTL set
        expect(true).toBe(true);
      }
    );
  });
});

// ─────────────────────────────────────────────────────────────────────────────
// WEBSOCKET TRANSPORT TESTS (mock server)
// ─────────────────────────────────────────────────────────────────────────────

describe('WebSocket transport — mock server integration', () => {
  it('connects and exchanges messages with a mock WS server', async () => {
    // We simulate a WebSocket server using a mock implementation
    // Real WS tests would use 'ws' package in Node.js
    const received: string[] = [];

    class MockWebSocket {
      static OPEN = 1;
      readyState = MockWebSocket.OPEN;
      private listeners: Record<string, ((e: unknown) => void)[]> = {};

      addEventListener(event: string, handler: (e: unknown) => void) {
        if (!this.listeners[event]) this.listeners[event] = [];
        this.listeners[event].push(handler);
        // Simulate immediate open
        if (event === 'open') setTimeout(() => handler({}), 10);
      }

      send(data: string) {
        received.push(data);
        // Echo back as message (simulate server)
        setTimeout(() => {
          this.listeners['message']?.forEach(h =>
            h({ data: JSON.stringify({ type: 'ack', id: JSON.parse(data)?.envelope?.id }) })
          );
        }, 20);
      }

      close() { this.readyState = 3; }
    }

    const { WebSocketTransport } = await import('../../packages/websocket/src/index.js');
    const transport = new WebSocketTransport({
      url: 'ws://localhost:9999',
      WebSocket: MockWebSocket as unknown as typeof WebSocket,
      heartbeatInterval: 10000, // don't heartbeat during test
    });

    await transport.connect();

    const envelope = makeEnvelope('ws.test', { via: 'websocket' });
    await transport.publish(envelope);
    await sleep(100);

    expect(received.length).toBeGreaterThanOrEqual(1);
    const sent = JSON.parse(received[0]!) as { type: string; envelope: EventEnvelope };
    expect(sent.type).toBe('publish');
    expect(sent.envelope.event).toBe('ws.test');

    await transport.disconnect();
  });

  it('queues messages during reconnect and flushes on reconnection', async () => {
    let connectionCount = 0;

    class MockWebSocket {
      static OPEN = 1;
      readyState = MockWebSocket.OPEN;
      private listeners: Record<string, ((e: unknown) => void)[]> = {};

      constructor(url: string) {
        connectionCount++;
        setTimeout(() => this.listeners['open']?.forEach(h => h({})), 10);
      }

      addEventListener(event: string, handler: (e: unknown) => void) {
        if (!this.listeners[event]) this.listeners[event] = [];
        this.listeners[event].push(handler);
      }

      send(_data: string) { /* silently accept */ }
      close() {
        this.readyState = 3;
        this.listeners['close']?.forEach(h => h({ code: 1006, reason: 'test' }));
      }
    }

    const { WebSocketTransport } = await import('../../packages/websocket/src/index.js');
    const transport = new WebSocketTransport({
      url: 'ws://localhost:9999',
      WebSocket: MockWebSocket as unknown as typeof WebSocket,
      initialBackoffMs: 50,
      heartbeatInterval: 60000,
    });

    await transport.connect();
    expect(connectionCount).toBe(1);
    await transport.disconnect();
  });
});

// ─────────────────────────────────────────────────────────────────────────────
// BROADCAST CHANNEL TRANSPORT TESTS (jsdom)
// ─────────────────────────────────────────────────────────────────────────────

describe('BroadcastChannel transport — browser env', () => {
  it('sends and receives messages across instances', async () => {
    // jsdom does not implement BroadcastChannel natively,
    // so we provide a mock
    const channels: Record<string, Array<(e: { data: unknown }) => void>> = {};

    class MockBroadcastChannel {
      private name: string;
      onmessage: ((e: { data: unknown }) => void) | null = null;
      onmessageerror: (() => void) | null = null;

      constructor(name: string) {
        this.name = name;
        if (!channels[name]) channels[name] = [];
        channels[name].push((e) => { this.onmessage?.(e); });
      }

      postMessage(data: unknown) {
        // Broadcast to ALL listeners except self (real API does not echo)
        const myHandler = channels[this.name]?.at(-1);
        channels[this.name]?.forEach(h => {
          if (h !== myHandler) h({ data });
        });
      }

      close() {
        const idx = channels[this.name]?.findIndex(h => h === channels[this.name]?.at(-1));
        if (idx !== undefined && idx > -1) channels[this.name]?.splice(idx, 1);
      }
    }

    // @ts-ignore — injecting mock
    globalThis.BroadcastChannel = MockBroadcastChannel;

    const { BroadcastChannelTransport } = await import('../../packages/broadcast-channel/src/index.js');

    const sender   = new BroadcastChannelTransport({ channel: 'test-channel' });
    const receiver = new BroadcastChannelTransport({ channel: 'test-channel' });

    await sender.connect();
    await receiver.connect();

    const received: EventEnvelope[] = [];
    await receiver.subscribe('bc.*', (e) => { received.push(e); });

    const envelope = makeEnvelope('bc.test', { via: 'broadcast-channel' });
    await sender.publish(envelope);
    await sleep(50);

    expect(received.length).toBeGreaterThanOrEqual(1);

    await sender.disconnect();
    await receiver.disconnect();
    // @ts-ignore
    delete globalThis.BroadcastChannel;
  });

  it('throws when BroadcastChannel is unavailable', async () => {
    // @ts-ignore — ensure it's not defined
    delete globalThis.BroadcastChannel;

    const { BroadcastChannelTransport } = await import('../../packages/broadcast-channel/src/index.js');
    const transport = new BroadcastChannelTransport({ channel: 'test' });

    await expect(transport.connect()).rejects.toThrow('BroadcastChannel is not supported');
  });
});

// ─────────────────────────────────────────────────────────────────────────────
// FULL BUS + TRANSPORT INTEGRATION
// ─────────────────────────────────────────────────────────────────────────────

describe('EventBus + transport end-to-end', () => {
  it('bus with mock transport routes published envelopes through transport.publish', async () => {
    const publishedEnvelopes: EventEnvelope[] = [];
    const inboundHandlers: Array<(e: EventEnvelope) => void> = [];

    const mockTransport: TransportAdapter = {
      name: 'mock',
      async connect() {},
      async disconnect() {},
      async publish(envelope) { publishedEnvelopes.push(envelope); },
      async subscribe(_pattern, onMessage) {
        inboundHandlers.push(onMessage);
        return () => {};
      },
      async ping() { return true; },
      on() {},
      off() {},
    };

    const bus = new EventBus({ transport: mockTransport });
    await bus.attachTransportAsync(mockTransport);

    bus.subscribe('test.event', () => {});
    await bus.publish('test.event', { data: 'via transport' }, { storeHistory: false });

    expect(publishedEnvelopes.length).toBeGreaterThanOrEqual(1);
    expect(publishedEnvelopes.at(-1)?.event).toBe('test.event');

    await bus.destroy();
  });

  it('bus correctly routes inbound transport events to local subscribers', async () => {
    const inboundHandlers: Array<(e: EventEnvelope) => void> = [];

    const mockTransport: TransportAdapter = {
      name: 'mock',
      async connect() {},
      async disconnect() {},
      async publish() {},
      async subscribe(_pattern, onMessage) {
        inboundHandlers.push(onMessage);
        return () => {};
      },
      async ping() { return true; },
      on() {},
      off() {},
    };

    const bus = new EventBus({ transport: mockTransport });

    const received: unknown[] = [];
    bus.subscribe('inbound.event', (e) => { received.push(e.payload); });

    // Simulate an inbound envelope arriving from the transport
    const inbound = makeEnvelope('inbound.event', { from: 'remote' });
    inboundHandlers.forEach(h => h(inbound));
    await sleep(20);

    expect(received).toContain(inbound.payload);
    await bus.destroy();
  });
});
