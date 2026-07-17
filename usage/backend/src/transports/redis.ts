/**
 * Redis Transport Adapter for GA-PubSub
 *
 * Publishes events to a Redis channel and subscribes to receive them,
 * enabling multi-process / multi-server event distribution.
 *
 * Falls back to in-process mock automatically when Redis is unreachable,
 * so the demo runs without a local Redis installation.
 */

import type { TransportAdapter, EventEnvelope } from 'ga-pubsub';
import { redisConfig } from '../config.js';

type EventHandler = (...args: unknown[]) => void;

// ─── Mock Redis (used when real Redis is unavailable) ─────────────────────────

class MockRedisTransport implements TransportAdapter {
  readonly name = 'Redis (Simulated)';
  private handlers: Array<(env: EventEnvelope) => void> = [];
  private listeners = new Map<string, EventHandler[]>();

  async connect(): Promise<void> {
    console.log('[Redis Transport] Running in simulation mode (no Redis server)');
    this.emit('connected');
  }

  async disconnect(): Promise<void> { this.emit('disconnected'); }

  async publish(envelope: EventEnvelope): Promise<void> {
    // In simulation mode, loop back to self (cross-process simulation)
    setImmediate(() => {
      for (const h of this.handlers) h(envelope);
    });
  }

  async subscribe(_pattern: string, onMessage: (envelope: EventEnvelope) => void): Promise<() => void> {
    this.handlers.push(onMessage);
    return () => {
      const idx = this.handlers.indexOf(onMessage);
      if (idx !== -1) this.handlers.splice(idx, 1);
    };
  }

  async ping(): Promise<boolean> { return true; }

  on(event: string, handler: EventHandler): void {
    if (!this.listeners.has(event)) this.listeners.set(event, []);
    this.listeners.get(event)!.push(handler);
  }

  off(event: string, handler: EventHandler): void {
    const arr = this.listeners.get(event);
    if (arr) {
      const idx = arr.indexOf(handler);
      if (idx !== -1) arr.splice(idx, 1);
    }
  }

  private emit(event: string, ...args: unknown[]): void {
    for (const h of this.listeners.get(event) ?? []) h(...args);
  }
}

// ─── Real Redis Transport ─────────────────────────────────────────────────────

class RedisTransport implements TransportAdapter {
  readonly name = 'Redis';
  private pub!: import('ioredis').Redis;
  private sub!: import('ioredis').Redis;
  private readonly channel = 'ga-pubsub:events';
  private listeners = new Map<string, EventHandler[]>();

  async connect(): Promise<void> {
    const { default: Redis } = await import('ioredis');
    this.pub = new Redis(redisConfig as import('ioredis').RedisOptions);
    this.sub = new Redis(redisConfig as import('ioredis').RedisOptions);

    await this.pub.ping();
    await this.sub.ping();

    this.pub.on('error', (e) => this.emit('error', e));
    this.sub.on('error', (e) => this.emit('error', e));

    await this.sub.subscribe(this.channel);
    this.emit('connected');
  }

  async disconnect(): Promise<void> {
    await this.sub.unsubscribe(this.channel);
    this.pub.disconnect();
    this.sub.disconnect();
    this.emit('disconnected');
  }

  async publish(envelope: EventEnvelope): Promise<void> {
    await this.pub.publish(this.channel, JSON.stringify(envelope));
  }

  async subscribe(_pattern: string, onMessage: (envelope: EventEnvelope) => void): Promise<() => void> {
    const handler = (_ch: string, msg: string) => {
      try { onMessage(JSON.parse(msg) as EventEnvelope); } catch {}
    };
    this.sub.on('message', handler);
    return () => this.sub.off('message', handler);
  }

  async ping(): Promise<boolean> {
    try { return (await this.pub.ping()) === 'PONG'; } catch { return false; }
  }

  on(event: string, handler: EventHandler): void {
    if (!this.listeners.has(event)) this.listeners.set(event, []);
    this.listeners.get(event)!.push(handler);
  }

  off(event: string, handler: EventHandler): void {
    const arr = this.listeners.get(event);
    if (arr) { const i = arr.indexOf(handler); if (i !== -1) arr.splice(i, 1); }
  }

  private emit(event: string, ...args: unknown[]): void {
    for (const h of this.listeners.get(event) ?? []) h(...args);
  }
}

// ─── Factory: try real, fall back to mock ────────────────────────────────────

export async function createRedisTransport(): Promise<{ adapter: TransportAdapter; simulated: boolean }> {
  const real = new RedisTransport();
  try {
    await real.connect();
    console.log('[Redis Transport] Connected to Redis at', redisConfig.host, ':', redisConfig.port);
    return { adapter: real, simulated: false };
  } catch {
    console.warn('[Redis Transport] Redis unavailable — using simulation mode');
    const mock = new MockRedisTransport();
    await mock.connect();
    return { adapter: mock, simulated: true };
  }
}
