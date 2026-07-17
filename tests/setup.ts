/**
 * GA-PubSub — Test Setup
 *
 * Bootstraps:
 *   1. Web Crypto API (Node.js 18+ has it globally; this ensures polyfill for older)
 *   2. structuredClone polyfill fallback
 *   3. TextEncoder / TextDecoder globals
 *   4. Vitest lifecycle helpers
 */

import { afterEach, vi } from 'vitest';

// ─── Web Crypto ───────────────────────────────────────────────────────────────
// Node.js 18+ exposes crypto globally as a subset of Web Crypto.
// Expose it explicitly so our security module can find it.
if (typeof globalThis.crypto === 'undefined') {
  const { webcrypto } = await import('node:crypto');
  Object.defineProperty(globalThis, 'crypto', {
    value: webcrypto,
    writable: false,
    configurable: true,
  });
}

// ─── structuredClone ─────────────────────────────────────────────────────────
// Node.js 17+ ships structuredClone; below that we patch.
if (typeof globalThis.structuredClone === 'undefined') {
  globalThis.structuredClone = <T>(val: T): T =>
    JSON.parse(JSON.stringify(val)) as T;
}

// ─── Fake timers reset guard ──────────────────────────────────────────────────
// Any test that enables fake timers must restore them; this catches leaks.
afterEach(() => {
  vi.useRealTimers();
});
