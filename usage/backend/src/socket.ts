/**
 * Socket.io bridge — connects the GA-PubSub bus to WebSocket clients.
 *
 * Every event published on the bus is forwarded to all connected clients,
 * giving the frontend a real-time stream of pub/sub activity.
 */

import type { Server as HttpServer } from 'http';
import { Server as SocketIO } from 'socket.io';
import { getBus, setBusErrorHandler } from './bus.js';
import { serverConfig } from './config.js';

let io: SocketIO | null = null;

export function getIO(): SocketIO {
  if (!io) throw new Error('Socket.io not initialized');
  return io;
}

export function initSocket(httpServer: HttpServer): SocketIO {
  io = new SocketIO(httpServer, {
    cors: {
      origin: serverConfig.corsOrigin,
      methods: ['GET', 'POST'],
    },
  });

  const bus = getBus();

  // ─── Bridge: every bus event → all WS clients ──────────────────────────
  bus.subscribe('**', (envelope) => {
    io?.emit('bus:event', {
      id:        envelope.id,
      event:     envelope.event,
      payload:   envelope.payload,
      timestamp: envelope.timestamp,
      userId:    envelope.userId,
      source:    envelope.source,
      signed:    !!envelope.signature,
      ttl:       envelope.ttl,
      sequence:  envelope.sequence,
      correlationId: envelope.correlationId,
    });
  });

  // ─── Bus error → WS clients ──────────────────────────────────────────
  setBusErrorHandler((code, message, event, phase) => {
    io?.emit('bus:error', { code, message, event, phase, timestamp: Date.now() });
  });

  // ─── Metrics broadcast (every 2 s) ──────────────────────────────────
  setInterval(() => {
    try {
      const m = bus.getMetrics();
      io?.emit('bus:metrics', m);
    } catch {}
  }, 2000);

  // ─── Socket connection handlers ──────────────────────────────────────
  io.on('connection', (socket) => {
    console.log(`[Socket.io] Client connected: ${socket.id}`);

    // Send current metrics immediately
    try { socket.emit('bus:metrics', bus.getMetrics()); } catch {}

    socket.on('disconnect', () => {
      console.log(`[Socket.io] Client disconnected: ${socket.id}`);
    });

    // Allow clients to request a metrics snapshot
    socket.on('metrics:request', () => {
      try { socket.emit('bus:metrics', bus.getMetrics()); } catch {}
    });
  });

  return io;
}

/** Broadcast any payload to all connected clients */
export function broadcast(event: string, data: unknown): void {
  io?.emit(event, data);
}
