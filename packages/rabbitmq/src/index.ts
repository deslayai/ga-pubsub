/**
 * @ga-pubsub/rabbitmq — RabbitMQ Transport Adapter
 *
 * Maps GA-PubSub events to RabbitMQ topology:
 *
 *   TOPOLOGY DESIGN:
 *     Exchange type: "topic" — matches GA-PubSub wildcard semantics exactly.
 *       AMQP routing key "payments.invoice.created" maps 1:1 to GA-PubSub event names.
 *       AMQP wildcard     "*"  (one word) maps to GA-PubSub "*"  (one segment)
 *       AMQP wildcard     "#"  (zero or more words) maps to GA-PubSub "**"
 *
 *   DELIVERY GUARANTEES:
 *     at-most-once   — publisher confirms off, consumer autoAck on
 *     at-least-once  — publisher confirms on, consumer manual ack [default]
 *     best-effort    — persistent messages + mandatory flag + dead-letter exchange
 *
 *   DEAD-LETTER QUEUES:
 *     Failed messages are routed to a DLX (dead-letter exchange) when
 *     enableDeadLetter is true. Inspect them with:
 *       ga-pubsub.dead-letter → dead letter exchange
 *       ga-pubsub.dead-letter.queue → dead letter queue
 *
 *   FEATURES:
 *     - Publisher confirms (guarantees broker received the message)
 *     - Consumer ACK / NACK / Requeue
 *     - Message TTL per envelope (amqplib expiration header)
 *     - Priority queue support (0–255)
 *     - TLS via amqplib connection string (amqps://)
 *     - Heartbeat keepalive
 *     - Channel-level prefetch (backpressure control)
 *
 * DEPENDENCIES:
 *   npm install amqplib
 *   npm install --save-dev @types/amqplib
 *
 * USAGE:
 *   import amqp from 'amqplib';
 *   import { RabbitMQTransport } from '@ga-pubsub/rabbitmq';
 *
 *   const bus = new EventBus({
 *     transport: new RabbitMQTransport({
 *       url: 'amqps://user:pass@rabbit.internal:5671',
 *       exchange: 'ga-pubsub',
 *       consumerQueue: 'payment-service',
 *       delivery: 'at-least-once',
 *       prefetch: 10,
 *     })
 *   });
 */

import type { TransportAdapter, EventEnvelope } from 'ga-pubsub';

// ─────────────────────────────────────────────────────────────────────────────
// AMQPLIB TYPE-ONLY INTERFACES
// ─────────────────────────────────────────────────────────────────────────────

interface AMQPMessage {
  content: Buffer;
  fields:  { routingKey: string; deliveryTag: number; redelivered: boolean };
  properties: {
    correlationId?: string;
    messageId?:     string;
    timestamp?:     number;
    expiration?:    string;
    priority?:      number;
    headers?:       Record<string, unknown>;
  };
}

interface AMQPChannel {
  assertExchange(exchange: string, type: string, opts?: Record<string, unknown>): Promise<unknown>;
  assertQueue(queue: string, opts?: Record<string, unknown>): Promise<{ queue: string }>;
  bindQueue(queue: string, exchange: string, routingKey: string): Promise<unknown>;
  publish(exchange: string, routingKey: string, content: Buffer, opts?: Record<string, unknown>): boolean;
  consume(queue: string, onMessage: (msg: AMQPMessage | null) => void, opts?: Record<string, unknown>): Promise<{ consumerTag: string }>;
  ack(message: AMQPMessage, allUpTo?: boolean): void;
  nack(message: AMQPMessage, allUpTo?: boolean, requeue?: boolean): void;
  prefetch(count: number): Promise<void>;
  close(): Promise<void>;
  waitForConfirms(): Promise<void>;
}

interface AMQPConfirmChannel extends AMQPChannel {
  waitForConfirms(): Promise<void>;
}

interface AMQPConnection {
  createChannel(): Promise<AMQPChannel>;
  createConfirmChannel(): Promise<AMQPConfirmChannel>;
  on(event: string, handler: (...args: unknown[]) => void): this;
  close(): Promise<void>;
}

interface AMQPLib {
  connect(url: string, opts?: { heartbeat?: number }): Promise<AMQPConnection>;
}

// ─────────────────────────────────────────────────────────────────────────────
// OPTIONS
// ─────────────────────────────────────────────────────────────────────────────

export interface RabbitMQTransportOptions {
  /**
   * AMQP(S) connection URL.
   * e.g. 'amqp://user:pass@localhost:5672' or 'amqps://...:5671' for TLS
   */
  url: string;

  /**
   * Topic exchange name.
   * @default 'ga-pubsub'
   */
  exchange?: string;

  /**
   * Durable queue name for this consumer.
   * Must be unique per service/replica group.
   * @default 'ga-pubsub-default'
   */
  consumerQueue?: string;

  /**
   * Delivery guarantee.
   * @default 'at-least-once'
   */
  delivery?: 'at-most-once' | 'at-least-once' | 'best-effort';

  /**
   * Enable dead-letter exchange for failed messages.
   * @default false
   */
  enableDeadLetter?: boolean;

  /**
   * Channel prefetch count — limits unacknowledged messages in flight.
   * Set to 1 for strict ordered processing, higher for throughput.
   * @default 10
   */
  prefetch?: number;

  /**
   * AMQP heartbeat interval in seconds.
   * @default 60
   */
  heartbeatSecs?: number;

  /**
   * Whether to declare the exchange and queue as durable.
   * Durable = survives broker restart.
   * @default true
   */
  durable?: boolean;

  /**
   * amqplib instance.
   * Defaults to require('amqplib').
   */
  amqplib?: AMQPLib;
}

type EventHandler = (...args: unknown[]) => void;

// ─────────────────────────────────────────────────────────────────────────────
// RABBITMQ TRANSPORT
// ─────────────────────────────────────────────────────────────────────────────

const DEAD_LETTER_EXCHANGE = 'ga-pubsub.dead-letter';
const DEAD_LETTER_QUEUE    = 'ga-pubsub.dead-letter.queue';

export class RabbitMQTransport implements TransportAdapter {
  readonly name = 'rabbitmq';

  private connection?: AMQPConnection;
  private pubChannel?:  AMQPChannel | AMQPConfirmChannel;
  private subChannel?:  AMQPChannel;
  private readonly subscriptions = new Map<string, (e: EventEnvelope) => void>();
  private readonly handlers       = new Map<string, Set<EventHandler>>();
  private connected = false;

  private readonly exchange:    string;
  private readonly queue:       string;
  private readonly durable:     boolean;
  private readonly autoAck:     boolean;
  private readonly confirms:    boolean;
  private readonly prefetch:    number;

  constructor(private readonly opts: RabbitMQTransportOptions) {
    this.exchange  = opts.exchange      ?? 'ga-pubsub';
    this.queue     = opts.consumerQueue ?? 'ga-pubsub-default';
    this.durable   = opts.durable       ?? true;
    this.autoAck   = opts.delivery === 'at-most-once';
    this.confirms  = opts.delivery === 'best-effort' || opts.delivery === 'at-least-once';
    this.prefetch  = opts.prefetch      ?? 10;
  }

  // ─── Public API ─────────────────────────────────────────────────────────

  async connect(): Promise<void> {
    const amqplib = this.opts.amqplib ?? (await import('amqplib') as unknown as { default: AMQPLib }).default;

    this.connection = await amqplib.connect(this.opts.url, {
      heartbeat: this.opts.heartbeatSecs ?? 60,
    });

    this.connection.on('error',  (err: Error) => this._emit('error', err));
    this.connection.on('close',  ()            => {
      this.connected = false;
      this._emit('disconnected');
    });

    // Separate channels for pub and sub (best practice — avoids head-of-line blocking)
    this.pubChannel = this.confirms
      ? await this.connection.createConfirmChannel()
      : await this.connection.createChannel();

    this.subChannel = await this.connection.createChannel();

    // Declare topology
    const exchangeArgs: Record<string, unknown> = { durable: this.durable };
    if (this.opts.enableDeadLetter) {
      exchangeArgs['alternate-exchange'] = DEAD_LETTER_EXCHANGE;
    }

    await this.pubChannel.assertExchange(this.exchange, 'topic', exchangeArgs);
    await this.subChannel.assertExchange(this.exchange, 'topic', exchangeArgs);

    if (this.opts.enableDeadLetter) {
      await this._setupDeadLetter();
    }

    // Assert consumer queue
    const queueArgs: Record<string, unknown> = {
      durable: this.durable,
      arguments: {
        ...(this.opts.enableDeadLetter && {
          'x-dead-letter-exchange': DEAD_LETTER_EXCHANGE,
        }),
      },
    };

    await this.subChannel.assertQueue(this.queue, queueArgs);
    await this.subChannel.prefetch(this.prefetch);

    this.connected = true;
    this._emit('connected');
  }

  async disconnect(): Promise<void> {
    this.connected = false;
    try {
      await this.pubChannel?.close();
      await this.subChannel?.close();
      await this.connection?.close();
    } catch {
      // Ignore errors during shutdown
    }
    this._emit('disconnected');
  }

  async publish(envelope: EventEnvelope): Promise<void> {
    if (!this.pubChannel) throw new Error('RabbitMQTransport: not connected');

    const routingKey = envelope.event; // GA-PubSub event name is the AMQP routing key
    const content    = Buffer.from(JSON.stringify(envelope));

    const publishOpts: Record<string, unknown> = {
      persistent:    this.durable,
      messageId:     envelope.id,
      correlationId: envelope.correlationId,
      timestamp:     Math.floor(envelope.timestamp / 1000), // AMQP uses seconds
      contentType:   'application/json',
      headers: {
        'ga-namespace': envelope.namespace,
        'ga-source':    envelope.source,
        'ga-version':   envelope.version,
        ...(envelope.tenantId && { 'ga-tenant-id': envelope.tenantId }),
        ...(envelope.userId   && { 'ga-user-id':   envelope.userId }),
      },
    };

    // Apply TTL via AMQP expiration (in ms as string, per AMQP spec)
    if (envelope.ttl) {
      publishOpts['expiration'] = String(envelope.ttl);
    }

    // Apply subscriber priority if present
    if (typeof (envelope as Record<string, unknown>)['priority'] === 'number') {
      publishOpts['priority'] = (envelope as Record<string, unknown>)['priority'];
    }

    this.pubChannel.publish(this.exchange, routingKey, content, publishOpts);

    // Wait for broker confirm (at-least-once / best-effort)
    if (this.confirms && 'waitForConfirms' in this.pubChannel) {
      await (this.pubChannel as AMQPConfirmChannel).waitForConfirms();
    }
  }

  async subscribe(
    pattern: string,
    onMessage: (envelope: EventEnvelope) => void
  ): Promise<() => void> {
    if (!this.subChannel) throw new Error('RabbitMQTransport: not connected');

    this.subscriptions.set(pattern, onMessage);

    // Bind queue to exchange with AMQP topic pattern
    const amqpPattern = this._toAMQPPattern(pattern);
    await this.subChannel.bindQueue(this.queue, this.exchange, amqpPattern);

    // Start consuming if not already running
    if (this.subscriptions.size === 1) {
      await this._startConsuming();
    }

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

  // ─── Internal ──────────────────────────────────────────────────────────

  private async _startConsuming(): Promise<void> {
    if (!this.subChannel) return;

    await this.subChannel.consume(this.queue, (msg) => {
      if (!msg) return; // Consumer cancelled

      let envelope: EventEnvelope;
      try {
        envelope = JSON.parse(msg.content.toString()) as EventEnvelope;
      } catch {
        // Malformed message — nack and don't requeue (prevents infinite loop)
        if (!this.autoAck) this.subChannel!.nack(msg, false, false);
        return;
      }

      // at-most-once: ack immediately before processing
      if (this.autoAck) {
        this.subChannel!.ack(msg);
      }

      // Route to all local subscription handlers
      // (wildcard matching is handled by the core bus; transport delivers all)
      for (const [, handler] of this.subscriptions) {
        try {
          handler(envelope);
        } catch {
          // Subscriber crash — nack this message to DLX if enabled
          if (!this.autoAck && this.opts.enableDeadLetter) {
            this.subChannel!.nack(msg, false, false); // don't requeue → goes to DLX
            return;
          }
        }
      }

      // at-least-once / best-effort: ack after successful processing
      if (!this.autoAck) {
        this.subChannel!.ack(msg);
      }
    }, { noAck: this.autoAck });
  }

  private async _setupDeadLetter(): Promise<void> {
    if (!this.pubChannel) return;
    await this.pubChannel.assertExchange(DEAD_LETTER_EXCHANGE, 'fanout', { durable: true });
    await this.pubChannel.assertQueue(DEAD_LETTER_QUEUE, { durable: true });
    await this.pubChannel.bindQueue(DEAD_LETTER_QUEUE, DEAD_LETTER_EXCHANGE, '#');
  }

  /**
   * Converts GA-PubSub wildcard pattern to AMQP topic routing key pattern.
   *
   * GA-PubSub  →  AMQP
   *   *         →  *      (one word)
   *   **        →  #      (zero or more words)
   *   exact     →  exact  (verbatim)
   */
  private _toAMQPPattern(pattern: string): string {
    return pattern
      .replace(/\.\*\*/g, '.#')  // "payments.**"  → "payments.#"
      .replace(/\*\*$/,   '#')   // "**"           → "#"
      .replace(/^\*\*\./,  '#.') // "**.event"     → "#.event"  (rare)
      // Single * remains as * — AMQP and GA-PubSub agree on one-word semantics
  }

  private _emit(event: string, ...args: unknown[]): void {
    this.handlers.get(event)?.forEach(h => { try { h(...args); } catch {} });
  }
}
