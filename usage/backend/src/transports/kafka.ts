/**
 * Kafka Transport Adapter for GA-PubSub
 *
 * Publishes events to a Kafka topic and subscribes as a consumer group,
 * enabling durable, high-throughput event streaming at scale.
 *
 * Falls back to in-process simulation when Kafka is unreachable.
 */

import type { TransportAdapter, EventEnvelope } from 'ga-pubsub';
import { kafkaConfig } from '../config.js';

type EventHandler = (...args: unknown[]) => void;

// ─── Mock Kafka Transport ─────────────────────────────────────────────────────

class MockKafkaTransport implements TransportAdapter {
  readonly name = 'Kafka (Simulated)';
  private handlers: Array<(env: EventEnvelope) => void> = [];
  private listeners = new Map<string, EventHandler[]>();
  private msgQueue: EventEnvelope[] = [];   // simulates topic retention

  async connect(): Promise<void> {
    console.log('[Kafka Transport] Running in simulation mode (no Kafka broker)');
    this.emit('connected');
  }

  async disconnect(): Promise<void> { this.emit('disconnected'); }

  async publish(envelope: EventEnvelope): Promise<void> {
    this.msgQueue.push(envelope);
    if (this.msgQueue.length > 500) this.msgQueue.shift(); // bounded retention
    setImmediate(() => {
      for (const h of this.handlers) h(envelope);
    });
  }

  async subscribe(_pattern: string, onMessage: (envelope: EventEnvelope) => void): Promise<() => void> {
    this.handlers.push(onMessage);
    // Replay last 10 messages to simulate consumer-group offset reset
    const recent = this.msgQueue.slice(-10);
    setImmediate(() => { for (const e of recent) onMessage(e); });
    return () => {
      const i = this.handlers.indexOf(onMessage);
      if (i !== -1) this.handlers.splice(i, 1);
    };
  }

  async ping(): Promise<boolean> { return true; }

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

  /** Expose simulated topic message count */
  get topicSize(): number { return this.msgQueue.length; }
}

// ─── Real Kafka Transport ─────────────────────────────────────────────────────

class KafkaTransport implements TransportAdapter {
  readonly name = 'Kafka';
  private producer!: import('kafkajs').Producer;
  private consumer!: import('kafkajs').Consumer;
  private listeners = new Map<string, EventHandler[]>();
  private handlers: Array<(env: EventEnvelope) => void> = [];

  async connect(): Promise<void> {
    const { Kafka } = await import('kafkajs');
    const kafka = new Kafka({
      clientId: kafkaConfig.clientId,
      brokers: kafkaConfig.brokers,
      connectionTimeout: 3000,
      requestTimeout: 5000,
      retry: { retries: 0 },
    });

    this.producer = kafka.producer();
    this.consumer = kafka.consumer({ groupId: kafkaConfig.groupId });

    await this.producer.connect();
    await this.consumer.connect();
    await this.consumer.subscribe({ topic: kafkaConfig.topic, fromBeginning: false });

    await this.consumer.run({
      eachMessage: async ({ message }) => {
        if (!message.value) return;
        try {
          const envelope = JSON.parse(message.value.toString()) as EventEnvelope;
          for (const h of this.handlers) h(envelope);
        } catch {}
      },
    });

    this.emit('connected');
  }

  async disconnect(): Promise<void> {
    await this.consumer.disconnect();
    await this.producer.disconnect();
    this.emit('disconnected');
  }

  async publish(envelope: EventEnvelope): Promise<void> {
    await this.producer.send({
      topic: kafkaConfig.topic,
      messages: [{ key: envelope.id, value: JSON.stringify(envelope) }],
    });
  }

  async subscribe(_pattern: string, onMessage: (envelope: EventEnvelope) => void): Promise<() => void> {
    this.handlers.push(onMessage);
    return () => {
      const i = this.handlers.indexOf(onMessage);
      if (i !== -1) this.handlers.splice(i, 1);
    };
  }

  async ping(): Promise<boolean> {
    try { await this.producer.send({ topic: kafkaConfig.topic, messages: [] }); return true; }
    catch { return false; }
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

// ─── Factory ──────────────────────────────────────────────────────────────────

export async function createKafkaTransport(): Promise<{
  adapter: TransportAdapter;
  simulated: boolean;
  mockRef?: MockKafkaTransport;
}> {
  const real = new KafkaTransport();
  try {
    await real.connect();
    console.log('[Kafka Transport] Connected to broker', kafkaConfig.brokers.join(','));
    return { adapter: real, simulated: false };
  } catch {
    console.warn('[Kafka Transport] Kafka unavailable — using simulation mode');
    const mock = new MockKafkaTransport();
    await mock.connect();
    return { adapter: mock, simulated: true, mockRef: mock };
  }
}
