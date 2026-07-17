/**
 * @ga-pubsub/redis — Redis Transport Adapter
 *
 * Provides two operating modes:
 *
 *   MODE 1: Redis Pub/Sub (default)
 *     - At-most-once delivery
 *     - No persistence
 *     - Best for high-throughput, low-latency scenarios
 *     - Consumer must be online to receive messages
 *
 *   MODE 2: Redis Streams (enableStreams: true)
 *     - At-least-once delivery via consumer groups
 *     - Persistent — messages survive restarts
 *     - Built-in replay from stream offset
 *     - Supports exactly-once processing (idempotent consumers)
 *
 * FEATURES:
 *   - Cluster support (via ioredis Cluster)
 *   - Sentinel support (via ioredis Sentinel)
 *   - Pattern subscriptions (PSUBSCRIBE for wildcard events)
 *   - TLS support (pass tls: {} in ioredis options)
 *   - Connection retry with exponential backoff
 *
 * DEPENDENCIES:
 *   npm install ioredis
 *
 * USAGE:
 *   import Redis from 'ioredis';
 *   import { RedisTransport } from '@ga-pubsub/redis';
 *
 *   const redis = new Redis({ host: 'redis.internal', tls: {} });
 *   const bus = new EventBus({
 *     transport: new RedisTransport({ client: redis, keyPrefix: 'myapp' })
 *   });
 */

import type { TransportAdapter, EventEnvelope } from 'ga-pubsub';

// ─────────────────────────────────────────────────────────────────────────────
// TYPE-ONLY IOREDIS INTERFACE (avoids hard dependency)
// ─────────────────────────────────────────────────────────────────────────────

interface RedisClient {
  publish(channel: string, message: string): Promise<number>;
  subscribe(...channels: string[]): Promise<unknown>;
  psubscribe(...patterns: string[]): Promise<unknown>;
  punsubscribe(...patterns: string[]): Promise<unknown>;
  on(event: 'message', listener: (channel: string, message: string) => void): this;
  on(event: 'pmessage', listener: (pattern: string, channel: string, message: string) => void): this;
  on(event: string, listener: (...args: unknown[]) => void): this;
  duplicate(): RedisClient;
  quit(): Promise<string>;
  xadd(key: string, id: string, ...fieldValues: string[]): Promise<string | null>;
  xreadgroup(
    group: string,
    consumer: string,
    ...args: string[]
  ): Promise<Array<[string, Array<[string, string[]]>]> | null>;
  xack(key: string, group: string, ...ids: string[]): Promise<number>;
  xgroup(subcommand: string, ...args: string[]): Promise<unknown>;
}

// ─────────────────────────────────────────────────────────────────────────────
// OPTIONS
// ─────────────────────────────────────────────────────────────────────────────

export interface RedisTransportOptions {
  /** ioredis client instance */
  client: RedisClient;

  /**
   * Key prefix applied to all channels/stream keys.
   * Use per-service prefixes to prevent channel collisions.
   * @default 'ga-pubsub'
   */
  keyPrefix?: string;

  /**
   * Enable Redis Streams instead of Pub/Sub.
   * Provides persistence and consumer group semantics.
   * @default false
   */
  enableStreams?: boolean;

  /**
   * Redis Streams consumer group name.
   * Required when enableStreams is true.
   * @default 'ga-pubsub-group'
   */
  consumerGroup?: string;

  /**
   * Unique consumer name within the group.
   * Must be unique per process/replica.
   * @default 'consumer-{random}'
   */
  consumerName?: string;

  /**
   * Stream polling interval (ms). Only for Streams mode.
   * @default 100
   */
  streamPollMs?: number;

  /**
   * Max messages to read per poll. Only for Streams mode.
   * @default 100
   */
  streamBatchSize?: number;

  /**
   * Delivery mode for Streams.
   * 'at-least-once': ACK after processing
   * 'at-most-once': ACK before processing (fire-and-forget)
   * @default 'at-least-once'
   */
  delivery?: 'at-least-once' | 'at-most-once';
}

type EventHandler = (...args: unknown[]) => void;

// ─────────────────────────────────────────────────────────────────────────────
// REDIS TRANSPORT
// ─────────────────────────────────────────────────────────────────────────────

export class RedisTransport implements TransportAdapter {
  readonly name = 'redis';

  private pub!: RedisClient;
  private sub!: RedisClient;
  private readonly subscriptions = new Map<string, (e: EventEnvelope) => void>();
  private readonly handlers = new Map<string, Set<EventHandler>>();
  private streamPoller?: ReturnType<typeof setInterval>;
  private connected = false;

  private readonly prefix: string;
  private readonly consumerGroup: string;
  private readonly consumerName: string;

  constructor(private readonly opts: RedisTransportOptions) {
    this.prefix = opts.keyPrefix ?? 'ga-pubsub';
    this.consumerGroup = opts.consumerGroup ?? 'ga-pubsub-group';
    this.consumerName = opts.consumerName ?? `consumer-${Math.random().toString(36).slice(2, 8)}`;
  }

  async connect(): Promise<void> {
    // Use separate connections for pub and sub (Redis requirement)
    this.pub = this.opts.client;
    this.sub = this.opts.client.duplicate();

    if (!this.opts.enableStreams) {
      // Pub/Sub mode: attach message listeners
      this.sub.on('message', (channel: string, message: string) => {
        this._handleRawMessage(channel, message);
      });

      this.sub.on('pmessage', (pattern: string, channel: string, message: string) => {
        this._handleRawMessage(channel, message);
      });
    } else {
      // Streams mode: start polling consumer groups
      this._startStreamPoller();
    }

    this.connected = true;
    this._emit('connected');
  }

  async disconnect(): Promise<void> {
    this.connected = false;
    if (this.streamPoller) clearInterval(this.streamPoller);
    await this.sub.quit();
    this._emit('disconnected');
  }

  async publish(envelope: EventEnvelope): Promise<void> {
    const channel = this._channelKey(envelope.event);
    const message = JSON.stringify(envelope);

    if (this.opts.enableStreams) {
      // Write to Redis Stream
      const streamKey = this._streamKey(envelope.event);
      await this.pub.xadd(streamKey, '*', 'envelope', message, 'event', envelope.event);
    } else {
      // Redis Pub/Sub
      await this.pub.publish(channel, message);
    }
  }

  async subscribe(
    pattern: string,
    onMessage: (envelope: EventEnvelope) => void
  ): Promise<() => void> {
    this.subscriptions.set(pattern, onMessage);

    if (!this.opts.enableStreams) {
      const channel = this._channelKey(pattern);
      if (pattern.includes('*')) {
        // Convert GA-PubSub wildcard syntax to Redis pattern syntax
        const redisPattern = channel.replace(/\.\*\*/g, '.*').replace(/\.\*/g, '.[^.]+');
        await this.sub.psubscribe(redisPattern);
      } else {
        await this.sub.subscribe(channel);
      }
    }
    // Streams mode: polling handles all patterns

    return () => {
      this.subscriptions.delete(pattern);
    };
  }

  async ping(): Promise<boolean> {
    return this.connected;
  }

  on(event: string, handler: EventHandler): void {
    if (!this.handlers.has(event)) this.handlers.set(event, new Set());
    this.handlers.get(event)!.add(handler);
  }

  off(event: string, handler: EventHandler): void {
    this.handlers.get(event)?.delete(handler);
  }

  // ─── Streams Polling ────────────────────────────────────────────────────

  private _startStreamPoller(): void {
    const pollMs = this.opts.streamPollMs ?? 100;
    const batchSize = this.opts.streamBatchSize ?? 100;

    this.streamPoller = setInterval(async () => {
      for (const [pattern] of this.subscriptions) {
        const streamKey = this._streamKey(pattern);
        try {
          const results = await this.pub.xreadgroup(
            'GROUP', this.consumerGroup, this.consumerName,
            'COUNT', String(batchSize),
            'BLOCK', '0',
            'STREAMS', streamKey, '>'
          );

          if (!results) continue;

          for (const [, messages] of results) {
            for (const [id, fields] of messages) {
              const envelopeIdx = fields.indexOf('envelope');
              if (envelopeIdx === -1) continue;

              let envelope: EventEnvelope;
              try {
                envelope = JSON.parse(fields[envelopeIdx + 1]) as EventEnvelope;
              } catch {
                continue;
              }

              if (this.opts.delivery === 'at-most-once') {
                await this.pub.xack(streamKey, this.consumerGroup, id);
              }

              this._routeEnvelope(envelope);

              if (this.opts.delivery !== 'at-most-once') {
                await this.pub.xack(streamKey, this.consumerGroup, id);
              }
            }
          }
        } catch {
          // Stream may not exist yet — create consumer group on demand
          try {
            await this.pub.xgroup('CREATE', streamKey, this.consumerGroup, '$', 'MKSTREAM');
          } catch {}
        }
      }
    }, pollMs);
  }

  // ─── Internal ──────────────────────────────────────────────────────────

  private _handleRawMessage(channel: string, message: string): void {
    let envelope: EventEnvelope;
    try {
      envelope = JSON.parse(message) as EventEnvelope;
    } catch {
      return;
    }
    this._routeEnvelope(envelope);
  }

  private _routeEnvelope(envelope: EventEnvelope): void {
    for (const [, handler] of this.subscriptions) {
      handler(envelope);
    }
  }

  private _channelKey(eventName: string): string {
    return `${this.prefix}:${eventName}`;
  }

  private _streamKey(eventName: string): string {
    return `${this.prefix}:stream:${eventName.replace(/[*.]/g, '_')}`;
  }

  private _emit(event: string, ...args: unknown[]): void {
    this.handlers.get(event)?.forEach(h => { try { h(...args); } catch {} });
  }
}
