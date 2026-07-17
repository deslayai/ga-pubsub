/**
 * @ga-pubsub/nats — NATS Transport Adapter
 *
 * Supports both NATS Core (pub/sub) and NATS JetStream (persistent streams).
 *
 *   NATS CORE (default):
 *     - Ultra-low latency (sub-millisecond)
 *     - At-most-once delivery
 *     - Subject wildcards: * (one token), > (multi-token — maps to GA-PubSub **)
 *     - Perfect GA-PubSub semantic match for subject hierarchies
 *
 *   NATS JETSTREAM (enableJetStream: true):
 *     - Persistent streams with configurable retention
 *     - At-least-once delivery with consumer ACKs
 *     - Replay from sequence or time
 *     - Push or pull consumers
 *
 *   SUBJECT MAPPING:
 *     GA-PubSub "payments.invoice.created" → NATS subject "ga-pubsub.payments.invoice.created"
 *     GA-PubSub "payments.*"               → NATS subject "ga-pubsub.payments.*"
 *     GA-PubSub "payments.**"              → NATS subject "ga-pubsub.payments.>"
 *
 * DEPENDENCIES:
 *   npm install nats
 *
 * USAGE:
 *   import { connect } from 'nats';
 *   import { NATSTransport } from '@ga-pubsub/nats';
 *
 *   const nc = await connect({ servers: 'nats://localhost:4222' });
 *   const bus = new EventBus({
 *     transport: new NATSTransport({ nc, enableJetStream: true, stream: 'GA_PUBSUB' })
 *   });
 */

import type { TransportAdapter, EventEnvelope } from 'ga-pubsub';

// ─────────────────────────────────────────────────────────────────────────────
// NATS TYPE-ONLY INTERFACES (nats.js v2)
// ─────────────────────────────────────────────────────────────────────────────

interface NATSMsg {
  subject: string;
  data: Uint8Array;
  headers?: Map<string, string[]>;
  ack?(): void;
  nak?(): void;
}

interface NATSSubscription {
  unsubscribe(): void;
  [Symbol.asyncIterator](): AsyncIterator<NATSMsg>;
}

interface JetStreamManager {
  streams: {
    add(config: {
      name: string;
      subjects: string[];
      retention?: string;
      max_age?: number;
      storage?: string;
      num_replicas?: number;
    }): Promise<unknown>;
    info(name: string): Promise<unknown>;
  };
  consumers: {
    add(stream: string, config: {
      durable_name?: string;
      deliver_subject?: string;
      deliver_policy?: string;
      ack_policy?: string;
      filter_subject?: string;
      max_deliver?: number;
    }): Promise<unknown>;
  };
}

interface JetStreamClient {
  publish(subject: string, data: Uint8Array, opts?: { msgID?: string }): Promise<{ seq: number }>;
  subscribe(subject: string, opts?: Record<string, unknown>): Promise<NATSSubscription>;
}

interface NATSConnection {
  publish(subject: string, data: Uint8Array): void;
  subscribe(subject: string, opts?: { queue?: string }): NATSSubscription;
  jetstreamManager(): Promise<JetStreamManager>;
  jetstream(): JetStreamClient;
  drain(): Promise<void>;
  close(): Promise<void>;
  closed(): Promise<Error | void>;
  isClosed(): boolean;
  status(): AsyncIterable<{ type: string; data?: unknown }>;
}

// ─────────────────────────────────────────────────────────────────────────────
// OPTIONS
// ─────────────────────────────────────────────────────────────────────────────

export interface NATSTransportOptions {
  /** Connected NATS connection */
  nc: NATSConnection;

  /**
   * Subject prefix for all events.
   * @default 'ga-pubsub'
   */
  prefix?: string;

  /**
   * Enable NATS JetStream for persistence and at-least-once delivery.
   * @default false
   */
  enableJetStream?: boolean;

  /**
   * JetStream stream name.
   * Required when enableJetStream is true.
   * @default 'GA_PUBSUB'
   */
  stream?: string;

  /**
   * Durable consumer name for JetStream push consumers.
   * @default 'ga-pubsub-consumer'
   */
  durableName?: string;

  /**
   * Queue group name for load-balanced subscription.
   * Multiple instances sharing the same group receive round-robin delivery.
   */
  queueGroup?: string;

  /**
   * Max message age for JetStream stream (nanoseconds).
   * 0 = unlimited.
   * @default 0
   */
  maxAgeNanos?: number;

  /**
   * Number of stream replicas for HA.
   * @default 1
   */
  numReplicas?: number;
}

type EventHandler = (...args: unknown[]) => void;

// ─────────────────────────────────────────────────────────────────────────────
// NATS TRANSPORT
// ─────────────────────────────────────────────────────────────────────────────

const te = new TextEncoder();
const td = new TextDecoder();

export class NATSTransport implements TransportAdapter {
  readonly name = 'nats';

  private js?: JetStreamClient;
  private jsm?: JetStreamManager;
  private readonly natsSubscriptions: NATSSubscription[] = [];
  private readonly subscriptions = new Map<string, (e: EventEnvelope) => void>();
  private readonly handlers = new Map<string, Set<EventHandler>>();
  private connected = false;
  private statusWatcher?: Promise<void>;

  private readonly prefix: string;

  constructor(private readonly opts: NATSTransportOptions) {
    this.prefix = opts.prefix ?? 'ga-pubsub';
  }

  async connect(): Promise<void> {
    if (this.opts.enableJetStream) {
      this.jsm = await this.opts.nc.jetstreamManager();
      this.js  = this.opts.nc.jetstream();

      await this._ensureStream();
    }

    this.connected = true;
    this._watchStatus();
    this._emit('connected');
  }

  async disconnect(): Promise<void> {
    this.connected = false;
    for (const sub of this.natsSubscriptions) {
      try { sub.unsubscribe(); } catch {}
    }
    await this.opts.nc.drain();
    this._emit('disconnected');
  }

  async publish(envelope: EventEnvelope): Promise<void> {
    const subject = this._toSubject(envelope.event);
    const data = te.encode(JSON.stringify(envelope));

    if (this.opts.enableJetStream && this.js) {
      await this.js.publish(subject, data, { msgID: envelope.id });
    } else {
      this.opts.nc.publish(subject, data);
    }
  }

  async subscribe(
    pattern: string,
    onMessage: (envelope: EventEnvelope) => void
  ): Promise<() => void> {
    this.subscriptions.set(pattern, onMessage);

    const natsSubject = this._toNATSPattern(pattern);

    let natsSub: NATSSubscription;

    if (this.opts.enableJetStream && this.js) {
      natsSub = await this.js.subscribe(natsSubject, {
        durable: this.opts.durableName ?? 'ga-pubsub-consumer',
        queue:   this.opts.queueGroup,
      });
    } else {
      natsSub = this.opts.nc.subscribe(natsSubject, {
        queue: this.opts.queueGroup,
      });
    }

    this.natsSubscriptions.push(natsSub);
    this._consumeSubscription(natsSub);

    return () => {
      this.subscriptions.delete(pattern);
      try { natsSub.unsubscribe(); } catch {}
    };
  }

  async ping(): Promise<boolean> {
    return this.connected && !this.opts.nc.isClosed();
  }

  on(event: string, handler: EventHandler): void {
    if (!this.handlers.has(event)) this.handlers.set(event, new Set());
    this.handlers.get(event)!.add(handler);
  }

  off(event: string, handler: EventHandler): void {
    this.handlers.get(event)?.delete(handler);
  }

  // ─── Internal ──────────────────────────────────────────────────────────

  private _consumeSubscription(sub: NATSSubscription): void {
    void (async () => {
      for await (const msg of sub) {
        try {
          const envelope = JSON.parse(td.decode(msg.data)) as EventEnvelope;
          for (const [, handler] of this.subscriptions) {
            handler(envelope);
          }
          msg.ack?.();
        } catch {
          msg.nak?.();
        }
      }
    })();
  }

  private async _ensureStream(): Promise<void> {
    const streamName = this.opts.stream ?? 'GA_PUBSUB';
    try {
      await this.jsm!.streams.info(streamName);
    } catch {
      // Stream doesn't exist — create it
      await this.jsm!.streams.add({
        name: streamName,
        subjects: [`${this.prefix}.>`],
        retention: 'limits',
        max_age: this.opts.maxAgeNanos ?? 0,
        storage: 'file',
        num_replicas: this.opts.numReplicas ?? 1,
      });
    }
  }

  private _watchStatus(): void {
    this.statusWatcher = (async () => {
      for await (const s of this.opts.nc.status()) {
        if (s.type === 'disconnect') {
          this.connected = false;
          this._emit('disconnected');
          this._emit('reconnecting');
        } else if (s.type === 'reconnect') {
          this.connected = true;
          this._emit('reconnected');
        } else if (s.type === 'error') {
          this._emit('error', s.data);
        }
      }
    })();
  }

  /** GA-PubSub event name → NATS subject */
  private _toSubject(eventName: string): string {
    return `${this.prefix}.${eventName}`;
  }

  /** GA-PubSub pattern → NATS subject pattern */
  private _toNATSPattern(pattern: string): string {
    // GA "user.**"  → NATS "ga-pubsub.user.>"
    // GA "user.*"   → NATS "ga-pubsub.user.*"
    // GA "user.created" → NATS "ga-pubsub.user.created"
    return `${this.prefix}.${pattern.replace(/\.\*\*/g, '.>').replace(/\.\*/g, '.*')}`;
  }

  private _emit(event: string, ...args: unknown[]): void {
    this.handlers.get(event)?.forEach(h => { try { h(...args); } catch {} });
  }
}
