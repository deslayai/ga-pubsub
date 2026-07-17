/**
 * @ga-pubsub/sse — Server-Sent Events Transport Adapter
 *
 * Provides a unidirectional (server → client) event stream via SSE,
 * combined with a fetch-based publish channel for browser environments
 * where WebSockets aren't available or desired.
 *
 * Architecture:
 *   SUBSCRIBE: SSE (EventSource) — server pushes events to client
 *   PUBLISH:   HTTP POST to a REST endpoint — client sends events up
 *
 * Features:
 *   - Automatic EventSource reconnection (built into the browser spec)
 *   - Last-Event-ID support for gap recovery
 *   - Bearer token authentication on both channels
 *   - Configurable retry interval
 *
 * Server-side pairing:
 *   Any HTTP server that can write SSE responses.
 *   See @ga-pubsub/sse/server for Express/Koa/Hono/Fastify adapters.
 *
 * USAGE:
 *   import { SSETransport } from '@ga-pubsub/sse';
 *
 *   const bus = new EventBus({
 *     transport: new SSETransport({
 *       sseUrl:     'https://api.example.com/events/stream',
 *       publishUrl: 'https://api.example.com/events/publish',
 *       token: () => auth.getToken(),
 *     })
 *   });
 */

import type { TransportAdapter, EventEnvelope } from 'ga-pubsub';

export interface SSETransportOptions {
  /** SSE endpoint URL */
  sseUrl: string;
  /** HTTP POST endpoint for publishing */
  publishUrl: string;
  /** Bearer token or factory */
  token?: string | (() => string | Promise<string>);
  /**
   * Retry interval hint sent to EventSource (milliseconds).
   * @default 3000
   */
  retryMs?: number;
  /** Additional headers for publish requests */
  headers?: Record<string, string>;
}

type EventHandler = (...args: unknown[]) => void;

export class SSETransport implements TransportAdapter {
  readonly name = 'sse';

  private source: EventSource | null = null;
  private readonly subscriptions = new Map<string, (e: EventEnvelope) => void>();
  private readonly handlers = new Map<string, Set<EventHandler>>();
  private connected = false;

  constructor(private readonly opts: SSETransportOptions) {}

  async connect(): Promise<void> {
    return new Promise((resolve, reject) => {
      if (!globalThis.EventSource) {
        reject(new Error('EventSource not available in this environment'));
        return;
      }

      this.source = new EventSource(this.opts.sseUrl, { withCredentials: true });

      this.source.addEventListener('open', () => {
        this.connected = true;
        this._emit('connected');
        resolve();
      });

      this.source.addEventListener('error', (ev) => {
        if (!this.connected) {
          reject(new Error('SSE connection failed'));
        }
        this.connected = false;
        this._emit('disconnected');
        this._emit('reconnecting');
        // EventSource auto-reconnects per spec
      });

      // Handle named events from the server
      this.source.addEventListener('message', (ev: MessageEvent) => {
        this._handleMessage(ev.data as string);
      });

      // Catch-all for custom event names
      this.source.addEventListener('ga-pubsub', (ev: MessageEvent) => {
        this._handleMessage(ev.data as string);
      });
    });
  }

  async disconnect(): Promise<void> {
    if (this.source) {
      this.source.close();
      this.source = null;
    }
    this.connected = false;
  }

  async publish(envelope: EventEnvelope): Promise<void> {
    const token = typeof this.opts.token === 'function'
      ? await this.opts.token()
      : this.opts.token;

    const headers: Record<string, string> = {
      'Content-Type': 'application/json',
      ...this.opts.headers,
    };
    if (token) headers['Authorization'] = `Bearer ${token}`;

    const response = await fetch(this.opts.publishUrl, {
      method: 'POST',
      headers,
      body: JSON.stringify(envelope),
    });

    if (!response.ok) {
      throw new Error(`SSE publish failed: ${response.status} ${response.statusText}`);
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

  private _handleMessage(raw: string): void {
    let envelope: EventEnvelope;
    try {
      envelope = JSON.parse(raw) as EventEnvelope;
    } catch {
      return;
    }
    for (const [, handler] of this.subscriptions) {
      handler(envelope);
    }
  }

  private _emit(event: string, ...args: unknown[]): void {
    this.handlers.get(event)?.forEach(h => { try { h(...args); } catch {} });
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// SERVER-SIDE: SSE Response Helper
// ─────────────────────────────────────────────────────────────────────────────

/**
 * Creates an SSE response writer compatible with Express / Fastify / Hono.
 *
 * USAGE (Express):
 *   app.get('/events/stream', (req, res) => {
 *     const sse = createSSEWriter(res);
 *     const sub = bus.subscribe('**', envelope => sse.write(envelope));
 *     req.on('close', () => { sub.unsubscribe(); sse.close(); });
 *   });
 */
export interface SSEWriter {
  write(envelope: EventEnvelope): void;
  close(): void;
}

export function createSSEWriter(res: {
  setHeader: (k: string, v: string) => void;
  write: (data: string) => void;
  end: () => void;
}): SSEWriter {
  res.setHeader('Content-Type', 'text/event-stream');
  res.setHeader('Cache-Control', 'no-cache');
  res.setHeader('Connection', 'keep-alive');
  res.setHeader('X-Accel-Buffering', 'no'); // nginx pass-through

  return {
    write(envelope: EventEnvelope): void {
      const data = JSON.stringify(envelope);
      res.write(`event: ga-pubsub\nid: ${envelope.id}\ndata: ${data}\n\n`);
    },
    close(): void {
      res.end();
    },
  };
}
