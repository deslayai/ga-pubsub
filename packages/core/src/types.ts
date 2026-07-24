/**
 * GA-PubSub Core — Type Definitions  (FREE tier)
 *
 * Contains all types for the free ga-pubsub package.
 * PRO types (TransportAdapter, AuthorizerFn, AuthContext, security errors)
 * live in ga-pubsub-pro.
 *
 * @version 1.0.0
 * @license Elastic-2.0
 */

// ─────────────────────────────────────────────────────────────────────────────
// EVENT ENVELOPE
// ─────────────────────────────────────────────────────────────────────────────

export interface EventEnvelope<T = unknown> {
  readonly id: string;
  readonly event: string;
  readonly namespace: string;
  payload: T;
  readonly timestamp: number;
  readonly correlationId: string;
  readonly causationId: string;
  readonly source: string;
  readonly version: string;
  readonly tenantId?: string;
  readonly userId?: string;
  readonly ttl?: number;
  /** HMAC signature — populated by ga-pubsub-pro when signing is enabled */
  readonly signature?: string;
  /** Monotonic sequence — populated by ga-pubsub-pro for replay-attack prevention */
  readonly sequence?: number;
  readonly metadata?: Record<string, unknown>;
}

// ─────────────────────────────────────────────────────────────────────────────
// SUBSCRIBER
// ─────────────────────────────────────────────────────────────────────────────

export type SubscriberCallback<T = unknown> = (
  envelope: EventEnvelope<T>
) => void | Promise<void>;

export interface SubscriberOptions {
  /** Auto-unsubscribe after first delivery. @default false */
  once?: boolean;

  /** Higher priority subscribers run first. @default 0 */
  priority?: number;

  /** Replay historical events on subscribe. @default true */
  replay?: boolean;

  /** Only replay events within the last N milliseconds. */
  replayLastMs?: number;

  /** Only replay events matching this predicate. */
  replayFilter?: (envelope: EventEnvelope) => boolean;

  /**
   * Authorization context passed to ga-pubsub-pro's authorizer.
   * Ignored by ga-pubsub (no enforcement in free tier).
   */
  authContext?: Record<string, unknown>;
}

export interface SubscriptionHandle {
  readonly id: string;
  readonly eventName: string;
  unsubscribe(): void;
}

// ─────────────────────────────────────────────────────────────────────────────
// PUBLISH OPTIONS
// ─────────────────────────────────────────────────────────────────────────────

export interface PublishOptions {
  /** Persist in replay store. @default true */
  storeHistory?: boolean;
  correlationId?: string;
  causationId?: string;
  source?: string;
  version?: string;
  tenantId?: string;
  userId?: string;
  /** Event TTL in milliseconds. Expired events are silently dropped. */
  ttl?: number;
  metadata?: Record<string, unknown>;
  delivery?: 'at-most-once' | 'at-least-once' | 'best-effort';
}

// ─────────────────────────────────────────────────────────────────────────────
// MIDDLEWARE
// ─────────────────────────────────────────────────────────────────────────────

export type NextFn = () => Promise<void>;

export type MiddlewareFn<T = unknown> = (
  envelope: EventEnvelope<T>,
  next: NextFn
) => Promise<void> | void;

// ─────────────────────────────────────────────────────────────────────────────
// SCHEMA VALIDATION
// ─────────────────────────────────────────────────────────────────────────────

export type ValidationResult =
  | { valid: true }
  | { valid: false; errors: ValidationError[] };

export interface ValidationError {
  path: string;
  message: string;
  value?: unknown;
}

export interface Validator<T = unknown> {
  validate(payload: unknown): ValidationResult;
  readonly name: string;
}

// ─────────────────────────────────────────────────────────────────────────────
// REQUEST / RESPONSE (RPC)
// ─────────────────────────────────────────────────────────────────────────────

export interface RequestOptions extends PublishOptions {
  timeoutMs?: number;
}

export interface RequestHandle<T = unknown> {
  readonly response: Promise<EventEnvelope<T>>;
  cancel(): void;
}

export type ResponderFn<TReq = unknown, TRes = unknown> = (
  envelope: EventEnvelope<TReq>
) => TRes | Promise<TRes>;

// ─────────────────────────────────────────────────────────────────────────────
// REPLAY ENGINE
// ─────────────────────────────────────────────────────────────────────────────

export interface ReplayOptions {
  limit?: number;
  ttl?: number;
  replayWildcards?: boolean;
}

// ─────────────────────────────────────────────────────────────────────────────
// OBSERVABILITY / METRICS
// ─────────────────────────────────────────────────────────────────────────────

export interface BusMetrics {
  publishCount: number;
  subscribeCount: number;
  unsubscribeCount: number;
  failedDeliveries: number;
  middlewareRejections: number;
  authorizationDenials: number;
  validationFailures: number;
  replayCount: number;
  requestCount: number;
  requestTimeouts: number;
  p95LatencyMs: number;
  activeSubscriptions: number;
  historySize: number;
}

export interface TelemetryHooks {
  onPublish?: (envelope: EventEnvelope, latencyMs: number) => void;
  onSubscribe?: (id: string, eventName: string) => void;
  onUnsubscribe?: (id: string, eventName: string) => void;
  onError?: (error: Error, context: ErrorContext) => void;
  onMiddlewareRejection?: (eventName: string, reason: string) => void;
  /** Called by ga-pubsub-pro when an authorization is denied. */
  onAuthDenial?: (eventName: string, context: Record<string, unknown>) => void;
  onValidationFailure?: (eventName: string, errors: ValidationError[]) => void;
  onReplay?: (eventName: string, count: number) => void;
}

export interface ErrorContext {
  phase: 'middleware' | 'subscriber' | 'replay' | 'validation' | 'auth' | 'transport';
  eventName: string;
  subscriberId?: string;
  envelope?: EventEnvelope;
}

// ─────────────────────────────────────────────────────────────────────────────
// BUS CONFIGURATION  (FREE tier options)
// ─────────────────────────────────────────────────────────────────────────────

export interface BusOptions {
  /** Logical namespace. @default 'default' */
  namespace?: string;
  tenantId?: string;
  /** Source label on outbound envelopes. @default 'ga-pubsub' */
  source?: string;
  replay?: ReplayOptions;
  /** Enable wildcard matching. @default true */
  enableWildcard?: boolean;
  /** Maximum active subscriptions (0 = unlimited). @default 0 */
  maxSubscriptions?: number;
  telemetry?: TelemetryHooks;
  onError?: (error: Error, context: ErrorContext) => void;
}

// ─────────────────────────────────────────────────────────────────────────────
// WILDCARD ENGINE
// ─────────────────────────────────────────────────────────────────────────────

export interface WildcardMatcher {
  matches(pattern: string, eventName: string): boolean;
  compile(pattern: string): CompiledPattern;
}

export interface CompiledPattern {
  readonly source: string;
  test(eventName: string): boolean;
}

// ─────────────────────────────────────────────────────────────────────────────
// ERROR TYPES  (FREE tier)
// ─────────────────────────────────────────────────────────────────────────────

export class GAPubSubError extends Error {
  constructor(
    message: string,
    public readonly code: string,
    public readonly context?: Record<string, unknown>
  ) {
    super(message);
    this.name = 'GAPubSubError';
  }
}

export class ValidationFailedError extends GAPubSubError {
  constructor(eventName: string, public readonly errors: ValidationError[]) {
    super(
      `Validation failed for event "${eventName}": ${errors.map(e => e.message).join(', ')}`,
      'VALIDATION_FAILED',
      { eventName, errors }
    );
    this.name = 'ValidationFailedError';
  }
}

export class RequestTimeoutError extends GAPubSubError {
  constructor(eventName: string, timeoutMs: number) {
    super(
      `Request "${eventName}" timed out after ${timeoutMs}ms`,
      'REQUEST_TIMEOUT',
      { eventName, timeoutMs }
    );
    this.name = 'RequestTimeoutError';
  }
}

export class SubscriptionLimitError extends GAPubSubError {
  constructor(max: number) {
    super(
      `Subscription limit reached (max ${max} active subscriptions)`,
      'SUBSCRIPTION_LIMIT',
      { max }
    );
    this.name = 'SubscriptionLimitError';
  }
}

export class MiddlewareAbortError extends GAPubSubError {
  constructor(eventName: string, middlewareName?: string) {
    super(
      `Event "${eventName}" aborted by middleware${middlewareName ? ` "${middlewareName}"` : ''}`,
      'MIDDLEWARE_ABORT',
      { eventName, middlewareName }
    );
    this.name = 'MiddlewareAbortError';
  }
}
