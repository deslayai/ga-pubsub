/**
 * @ga-pubsub/broadcast-channel — BroadcastChannel Transport Adapter
 *
 * Enables cross-tab and cross-worker event sharing in browser environments
 * using the Web BroadcastChannel API — zero network round-trips.
 *
 * SUPPORTED ENVIRONMENTS:
 *   - All modern browsers (Chrome 54+, Firefox 38+, Safari 15.4+)
 *   - Web Workers and Service Workers
 *   - NOT supported in Node.js (use Redis or NATS for multi-process)
 *
 * SECURITY:
 *   - Same-origin policy is enforced by the browser
 *   - Messages are confined to the same origin and channel name
 *   - Use a unique channel name per application to prevent cross-app leakage
 *
 * USAGE:
 *   import { BroadcastChannelTransport } from '@ga-pubsub/broadcast-channel';
 *
 *   const bus = new EventBus({
 *     transport: new BroadcastChannelTransport({ channel: 'myapp-events' })
 *   });
 *
 *   // Tab 1
 *   bus.publish('cart.updated', { items: 3 });
 *
 *   // Tab 2 — receives event automatically
 *   bus.subscribe('cart.*', envelope => updateCartBadge(envelope.payload));
 */

import type { TransportAdapter, EventEnvelope } from 'ga-pubsub';

export interface BroadcastChannelTransportOptions {
  /**
   * Channel name. Use a unique name per application.
   * All tabs/workers sharing this name form one broadcast group.
   * @default 'ga-pubsub'
   */
  channel?: string;

  /**
   * Whether to also receive messages published by THIS tab.
   * BroadcastChannel does NOT echo to sender by default.
   * Enable this only if you need strict local parity.
   * @default false
   */
  echoLocal?: boolean;
}

type EventHandler = (...args: unknown[]) => void;

export class BroadcastChannelTransport implements TransportAdapter {
  readonly name = 'broadcast-channel';

  private channel: BroadcastChannel | null = null;
  private readonly subscriptions = new Map<string, (e: EventEnvelope) => void>();
  private readonly handlers = new Map<string, Set<EventHandler>>();
  private connected = false;

  private readonly channelName: string;
  private readonly echoLocal: boolean;

  constructor(opts: BroadcastChannelTransportOptions = {}) {
    this.channelName = opts.channel ?? 'ga-pubsub';
    this.echoLocal = opts.echoLocal ?? false;
  }

  async connect(): Promise<void> {
    if (!globalThis.BroadcastChannel) {
      throw new Error(
        'BroadcastChannel is not supported in this environment. ' +
        'Use @ga-pubsub/websocket or @ga-pubsub/redis for Node.js multi-process scenarios.'
      );
    }

    this.channel = new BroadcastChannel(this.channelName);
    this.channel.onmessage = (ev: MessageEvent) => {
      this._handleMessage(ev.data as EventEnvelope);
    };
    this.channel.onmessageerror = () => {
      this._emit('error', new Error('BroadcastChannel message error'));
    };

    this.connected = true;
    this._emit('connected');
  }

  async disconnect(): Promise<void> {
    this.channel?.close();
    this.channel = null;
    this.connected = false;
    this._emit('disconnected');
  }

  async publish(envelope: EventEnvelope): Promise<void> {
    if (!this.channel) {
      throw new Error('BroadcastChannelTransport: not connected');
    }
    this.channel.postMessage(envelope);

    // BroadcastChannel does not echo to sender — optionally dispatch locally
    if (this.echoLocal) {
      this._handleMessage(envelope);
    }
  }

  async subscribe(
    pattern: string,
    onMessage: (envelope: EventEnvelope) => void
  ): Promise<() => void> {
    this.subscriptions.set(pattern, onMessage);
    return () => { this.subscriptions.delete(pattern); };
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

  private _handleMessage(envelope: EventEnvelope): void {
    for (const [, handler] of this.subscriptions) {
      handler(envelope);
    }
  }

  private _emit(event: string, ...args: unknown[]): void {
    this.handlers.get(event)?.forEach(h => { try { h(...args); } catch {} });
  }
}
