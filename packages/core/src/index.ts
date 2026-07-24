/**
 * GA-PubSub Core  (FREE tier — Elastic-2.0)
 *
 * Published as: ga-pubsub
 *
 * Included:
 *   ✅ EventBus — pub/sub with wildcards, middleware, schema validation, priority,
 *                 subscribeOnce, replay, RPC request/response, TTL, metrics
 *
 * NOT included (@deslayai/ga-pubsub-pro):
 *   🔒 Registry / ScopedBus / multi-tenant
 *   🔒 HMAC signing, tamper detection, replay-attack prevention
 *   🔒 Rate limiting, payload size limits
 *   🔒 Authorization (bus.authorize())
 *   🔒 Transport adapters (Redis, Kafka, WebSocket, NATS, RabbitMQ)
 *
 * @module ga-pubsub
 * @version 1.0.0
 */

// ─── Primary Classes ──────────────────────────────────────────────────────────
export { EventBus } from './bus.js';
export { ReplayEngine } from './replay.js';

// ─── Security utilities (free subset) ────────────────────────────────────────
export {
  sanitizePayload,
  isExpired,
  generateId,
} from './security.js';

// ─── Wildcard Engine ──────────────────────────────────────────────────────────
export { wildcardMatcher, SubscriptionIndex } from './wildcard.js';

// ─── Metrics ──────────────────────────────────────────────────────────────────
export { MetricsCollector, TelemetryDispatcher } from './metrics.js';

// ─── Types ────────────────────────────────────────────────────────────────────
export type {
  EventEnvelope,
  SubscriberCallback,
  SubscriberOptions,
  SubscriptionHandle,
  PublishOptions,
  RequestOptions,
  RequestHandle,
  ResponderFn,
  MiddlewareFn,
  NextFn,
  Validator,
  ValidationResult,
  ValidationError,
  ReplayOptions,
  BusOptions,
  BusMetrics,
  TelemetryHooks,
  ErrorContext,
  WildcardMatcher,
  CompiledPattern,
} from './types.js';

// ─── Error Types (free tier) ──────────────────────────────────────────────────
export {
  GAPubSubError,
  ValidationFailedError,
  RequestTimeoutError,
  SubscriptionLimitError,
  MiddlewareAbortError,
} from './types.js';

// ─── Built-in Middleware Factories ────────────────────────────────────────────

/**
 * Logging middleware — logs every event to console.
 */
export function loggingMiddleware(opts: { level?: 'debug' | 'info' | 'warn' } = {}) {
  const level = opts.level ?? 'debug';
  return async (envelope: import('./types.js').EventEnvelope, next: import('./types.js').NextFn) => {
    const fn = console[level] ?? console.log;
    fn('[GA-PubSub]', envelope.event, {
      id: envelope.id,
      namespace: envelope.namespace,
      correlationId: envelope.correlationId,
      source: envelope.source,
      timestamp: new Date(envelope.timestamp).toISOString(),
    });
    await next();
  };
}

/**
 * Timestamp middleware — records processing start time in metadata.
 */
export function timestampMiddleware() {
  return async (envelope: import('./types.js').EventEnvelope, next: import('./types.js').NextFn) => {
    (envelope as { metadata: Record<string, unknown> }).metadata = {
      ...envelope.metadata,
      processingStartedAt: Date.now(),
    };
    await next();
  };
}

/**
 * TTL guard middleware — drops expired events early in the pipeline.
 * Place FIRST in the middleware chain.
 */
export function ttlGuardMiddleware() {
  return async (envelope: import('./types.js').EventEnvelope, next: import('./types.js').NextFn) => {
    if (envelope.ttl && Date.now() > envelope.timestamp + envelope.ttl) {
      return; // silently drop
    }
    await next();
  };
}

/**
 * Namespace guard middleware — rejects envelopes from a different namespace.
 */
export function namespaceGuardMiddleware(expectedNamespace: string) {
  return async (envelope: import('./types.js').EventEnvelope, next: import('./types.js').NextFn) => {
    if (envelope.namespace !== expectedNamespace) {
      throw new (await import('./types.js')).GAPubSubError(
        `Namespace mismatch: expected "${expectedNamespace}", got "${envelope.namespace}"`,
        'NAMESPACE_MISMATCH'
      );
    }
    await next();
  };
}

/**
 * Correlation propagation middleware.
 */
export function correlationMiddleware(getCorrelationId: () => string | undefined) {
  return async (envelope: import('./types.js').EventEnvelope, next: import('./types.js').NextFn) => {
    const correlationId = getCorrelationId();
    if (correlationId && !envelope.correlationId) {
      (envelope as { correlationId: string }).correlationId = correlationId;
    }
    await next();
  };
}
