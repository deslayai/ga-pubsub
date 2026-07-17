import axios from 'axios';
import { io, type Socket } from 'socket.io-client';
import type { BusEvent, BusError, BusMetrics, DemoResult, TransportStatus } from '../types.ts';

const API = axios.create({ baseURL: '/api' });

// ─── REST helpers ──────────────────────────────────────────────────────────────
export const api = {
  health:   ()            => API.get('/health').then(r => r.data),
  transports: ()          => API.get('/transport/status').then(r => r.data as TransportStatus),
  kafkaStats: ()          => API.get('/transport/kafka/stats').then(r => r.data),
  createPayment: (data: Record<string, unknown>) => API.post('/payment/create', data).then(r => r.data),
  createSubscription: (data: Record<string, unknown>) => API.post('/payment/subscription', data).then(r => r.data),
  runDemo: (key: string)  => API.post(`/demo/${key}`).then(r => r.data as DemoResult),
};

// ─── Socket.io singleton ───────────────────────────────────────────────────────
let _socket: Socket | null = null;

export function getSocket(): Socket {
  if (!_socket) {
    const url = (import.meta.env['VITE_API_URL'] as string | undefined) ?? '';
    _socket = io(url || window.location.origin, { transports: ['websocket', 'polling'] });
  }
  return _socket;
}

export type BusEventListener   = (e: BusEvent) => void;
export type BusErrorListener   = (e: BusError) => void;
export type BusMetricsListener = (m: BusMetrics) => void;
export type DemoResultListener = (r: DemoResult) => void;
export type TransportListener  = (t: TransportStatus) => void;
