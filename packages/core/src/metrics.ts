/**
 * GA-PubSub — Metrics & Observability
 *
 * Provides:
 *   - Atomic counters for all lifecycle events
 *   - Sliding-window p95 latency (1000-sample ring buffer)
 *   - OpenTelemetry-compatible hook surface (via TelemetryHooks)
 *   - Zero-dependency: all stats kept in memory
 *
 * OpenTelemetry integration example (external):
 *   const meter = otel.getMeter('ga-pubsub');
 *   const publishCounter = meter.createCounter('ga_pubsub_publish_total');
 *   bus.options.telemetry = {
 *     onPublish: (e, ms) => publishCounter.add(1, { event: e.event }),
 *   };
 */

import type { BusMetrics, TelemetryHooks, ErrorContext, ValidationError } from './types.js';

// ─────────────────────────────────────────────────────────────────────────────
// P95 LATENCY TRACKER
// ─────────────────────────────────────────────────────────────────────────────

const WINDOW_SIZE = 1000;

class LatencyWindow {
  private readonly samples: number[] = [];
  private cursor = 0;
  private full = false;

  record(ms: number): void {
    this.samples[this.cursor] = ms;
    this.cursor = (this.cursor + 1) % WINDOW_SIZE;
    if (this.cursor === 0) this.full = true;
  }

  p95(): number {
    const data = this.full ? [...this.samples] : this.samples.slice(0, this.cursor);
    if (data.length === 0) return 0;
    const sorted = data.sort((a, b) => a - b);
    const idx = Math.floor(sorted.length * 0.95);
    return sorted[Math.min(idx, sorted.length - 1)] ?? 0;
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// METRICS STORE
// ─────────────────────────────────────────────────────────────────────────────

export class MetricsCollector {
  private readonly latency = new LatencyWindow();
  private readonly counters = {
    publishCount: 0,
    subscribeCount: 0,
    unsubscribeCount: 0,
    failedDeliveries: 0,
    middlewareRejections: 0,
    authorizationDenials: 0,
    validationFailures: 0,
    replayCount: 0,
    requestCount: 0,
    requestTimeouts: 0,
  };

  // Live values from the bus
  activeSubscriptions = 0;
  historySize = 0;

  recordPublish(latencyMs: number): void {
    this.counters.publishCount++;
    this.latency.record(latencyMs);
  }

  recordSubscribe(): void { this.counters.subscribeCount++; }
  recordUnsubscribe(): void { this.counters.unsubscribeCount++; }
  recordFailedDelivery(): void { this.counters.failedDeliveries++; }
  recordMiddlewareRejection(): void { this.counters.middlewareRejections++; }
  recordAuthDenial(): void { this.counters.authorizationDenials++; }
  recordValidationFailure(): void { this.counters.validationFailures++; }
  recordReplay(count: number): void { this.counters.replayCount += count; }
  recordRequest(): void { this.counters.requestCount++; }
  recordRequestTimeout(): void { this.counters.requestTimeouts++; }

  snapshot(): BusMetrics {
    return {
      ...this.counters,
      p95LatencyMs: Math.round(this.latency.p95() * 100) / 100,
      activeSubscriptions: this.activeSubscriptions,
      historySize: this.historySize,
    };
  }

  reset(): void {
    for (const key of Object.keys(this.counters) as (keyof typeof this.counters)[]) {
      this.counters[key] = 0;
    }
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// TELEMETRY DISPATCHER
// ─────────────────────────────────────────────────────────────────────────────

/**
 * Dispatches lifecycle events to the user-provided TelemetryHooks.
 * All hooks are called in a try-catch to prevent telemetry bugs
 * from crashing the event bus.
 */
export class TelemetryDispatcher {
  constructor(private readonly hooks: TelemetryHooks = {}) {}

  onPublish(envelope: import('./types.js').EventEnvelope, latencyMs: number): void {
    try { this.hooks.onPublish?.(envelope, latencyMs); } catch {}
  }

  onSubscribe(id: string, eventName: string): void {
    try { this.hooks.onSubscribe?.(id, eventName); } catch {}
  }

  onUnsubscribe(id: string, eventName: string): void {
    try { this.hooks.onUnsubscribe?.(id, eventName); } catch {}
  }

  onError(error: Error, context: ErrorContext): void {
    try { this.hooks.onError?.(error, context); } catch {}
  }

  onMiddlewareRejection(eventName: string, reason: string): void {
    try { this.hooks.onMiddlewareRejection?.(eventName, reason); } catch {}
  }

  onAuthDenial(eventName: string, context: Record<string, unknown>): void {
    try { this.hooks.onAuthDenial?.(eventName, context); } catch {}
  }

  onValidationFailure(eventName: string, errors: ValidationError[]): void {
    try { this.hooks.onValidationFailure?.(eventName, errors); } catch {}
  }

  onReplay(eventName: string, count: number): void {
    try { this.hooks.onReplay?.(eventName, count); } catch {}
  }
}
