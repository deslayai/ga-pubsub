/**
 * GA-PubSub Core — Security Utilities  (FREE tier)
 *
 * Only the utilities needed by the free EventBus are here:
 *   - generateId      — UUID v4 generation
 *   - sanitizePayload — deep-clone via structuredClone / JSON round-trip
 *   - isExpired       — TTL envelope check for replay engine
 *
 * HMAC signing, SequenceTracker, RateLimiter, enforcePayloadLimit
 * have moved to ga-pubsub-pro/src/security.ts
 */

import type { EventEnvelope } from './types.js';

// ─────────────────────────────────────────────────────────────────────────────
// ID GENERATION
// ─────────────────────────────────────────────────────────────────────────────

export function generateId(): string {
  const cryptoApi = globalThis.crypto;
  if (cryptoApi && typeof cryptoApi.randomUUID === 'function') {
    return cryptoApi.randomUUID();
  }
  if (!cryptoApi || typeof cryptoApi.getRandomValues !== 'function') {
    throw new Error('GA-PubSub requires a Web Crypto compatible runtime.');
  }
  const bytes = new Uint8Array(16);
  cryptoApi.getRandomValues(bytes);
  bytes[6] = (bytes[6]! & 0x0f) | 0x40;
  bytes[8] = (bytes[8]! & 0x3f) | 0x80;
  const hex = Array.from(bytes).map(b => b.toString(16).padStart(2, '0')).join('');
  return [hex.slice(0, 8), hex.slice(8, 12), hex.slice(12, 16), hex.slice(16, 20), hex.slice(20)].join('-');
}

// ─────────────────────────────────────────────────────────────────────────────
// PAYLOAD SANITIZATION
// ─────────────────────────────────────────────────────────────────────────────

export function sanitizePayload<T>(payload: T): T {
  if (typeof globalThis.structuredClone === 'function') {
    try {
      return globalThis.structuredClone(payload);
    } catch (error) {
      throw new TypeError(`Payload cannot be safely cloned: ${(error as Error).message}`);
    }
  }
  throw new Error('GA-PubSub requires structuredClone support to preserve payload types safely.');
}

// ─────────────────────────────────────────────────────────────────────────────
// TTL / EVENT EXPIRATION
// ─────────────────────────────────────────────────────────────────────────────

export function isExpired(envelope: EventEnvelope): boolean {
  if (envelope.ttl === undefined) return false;
  return Date.now() >= envelope.timestamp + envelope.ttl;
}
