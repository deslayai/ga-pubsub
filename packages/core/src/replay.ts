/**
 * GA-PubSub — Replay Engine
 *
 * Provides bounded, TTL-aware event history for late-joining subscribers.
 *
 * Design decisions:
 *   - Each event name has its own FIFO ring buffer (max `limit` entries).
 *   - TTL eviction is lazy: happens on read and on a periodic sweep timer.
 *   - The store is a Map<eventName, HistoryEntry[]> with O(1) lookup.
 *   - Memory is bounded by (total_event_types * limit) envelopes.
 *   - Wildcard replay: if replayWildcards is true, a new subscriber to
 *     "user.*" will receive all stored events matching that pattern.
 */

import type { EventEnvelope, ReplayOptions } from './types.js';
import { wildcardMatcher } from './wildcard.js';

interface HistoryEntry {
  envelope: EventEnvelope;
  storedAt: number;
}

const DEFAULT_REPLAY_LIMIT = 10;
const DEFAULT_TTL = 0; // no TTL by default

export class ReplayEngine {
  private readonly store = new Map<string, HistoryEntry[]>();
  private readonly limit: number;
  private readonly ttl: number;
  private readonly replayWildcards: boolean;
  private sweepTimer?: ReturnType<typeof setInterval>;

  constructor(options: ReplayOptions = {}) {
    this.limit = options.limit ?? DEFAULT_REPLAY_LIMIT;
    this.ttl = options.ttl ?? DEFAULT_TTL;
    this.replayWildcards = options.replayWildcards ?? true;

    // Periodic sweep every 60s to prevent TTL staleness
    if (this.ttl > 0 && typeof setInterval !== 'undefined') {
      this.sweepTimer = setInterval(() => this.sweep(), 60_000);
      // Don't block process exit in Node.js
      if (typeof this.sweepTimer === 'object' && 'unref' in this.sweepTimer) {
        (this.sweepTimer as NodeJS.Timeout).unref?.();
      }
    }
  }

  /**
   * Stores an envelope in the history for the given event name.
   * Evicts the oldest entry if the ring buffer is full.
   */
  store_(eventName: string, envelope: EventEnvelope): void {
    if (this.limit === 0) return; // Replay disabled

    if (!this.store.has(eventName)) {
      this.store.set(eventName, []);
    }

    const entries = this.store.get(eventName)!;
    entries.push({ envelope, storedAt: Date.now() });

    // Evict oldest if over limit (ring buffer behavior)
    while (entries.length > this.limit) {
      entries.shift();
    }
  }

  /**
   * Retrieves replay history for an event name or pattern.
   *
   * @param pattern Exact event name or wildcard pattern
   * @param options.lastMs Only return events within the last N milliseconds
   * @param options.filter Custom predicate applied after TTL check
   */
  getHistory(
    pattern: string,
    options: {
      lastMs?: number;
      filter?: (envelope: EventEnvelope) => boolean;
    } = {}
  ): EventEnvelope[] {
    const now = Date.now();
    const cutoff = options.lastMs ? now - options.lastMs : 0;

    const result: EventEnvelope[] = [];

    if (!pattern.includes('*')) {
      // Exact lookup
      const entries = this.store.get(pattern);
      if (entries) {
        const valid = this.filterEntries(entries, now, cutoff, options.filter);
        result.push(...valid);
        // Prune expired entries in-place
        this.store.set(pattern, this.pruneExpired(entries, now));
      }
    } else if (this.replayWildcards) {
      // Scan all stored event names matching the pattern
      for (const [eventName, entries] of this.store.entries()) {
        if (wildcardMatcher.matches(pattern, eventName)) {
          const valid = this.filterEntries(entries, now, cutoff, options.filter);
          result.push(...valid);
          this.store.set(eventName, this.pruneExpired(entries, now));
        }
      }

      // Sort by timestamp so replayed events arrive in chronological order
      result.sort((a, b) => a.timestamp - b.timestamp);
    }

    return result;
  }

  private filterEntries(
    entries: HistoryEntry[],
    now: number,
    cutoff: number,
    filter?: (envelope: EventEnvelope) => boolean
  ): EventEnvelope[] {
    return entries
      .filter(e => {
        if (this.ttl > 0 && now - e.storedAt > this.ttl) return false;
        if (cutoff > 0 && e.envelope.timestamp < cutoff) return false;
        if (filter && !filter(e.envelope)) return false;
        return true;
      })
      .map(e => e.envelope);
  }

  private pruneExpired(entries: HistoryEntry[], now: number): HistoryEntry[] {
    if (this.ttl === 0) return entries;
    return entries.filter(e => now - e.storedAt <= this.ttl);
  }

  /**
   * Periodic sweep to evict all expired entries.
   */
  private sweep(): void {
    if (this.ttl === 0) return;
    const now = Date.now();
    for (const [key, entries] of this.store.entries()) {
      const pruned = this.pruneExpired(entries, now);
      if (pruned.length === 0) {
        this.store.delete(key);
      } else {
        this.store.set(key, pruned);
      }
    }
  }

  /**
   * Returns total number of stored envelopes across all event names.
   */
  get size(): number {
    let n = 0;
    for (const entries of this.store.values()) n += entries.length;
    return n;
  }

  clearEvent(eventName: string): void {
    this.store.delete(eventName);
  }

  clear(): void {
    this.store.clear();
  }

  destroy(): void {
    if (this.sweepTimer !== undefined) {
      clearInterval(this.sweepTimer);
    }
    this.clear();
  }
}
