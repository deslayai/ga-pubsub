/**
 * GA-PubSub Core — EventBus  (FREE tier)
 *
 * Included in ga-pubsub-core (MIT):
 *   ✅ Publish / Subscribe with priority ordering
 *   ✅ Wildcard patterns (* and **)
 *   ✅ Middleware pipeline
 *   ✅ Schema validation
 *   ✅ Replay engine (late-subscriber history)
 *   ✅ Request / Response (RPC)
 *   ✅ Metrics
 *   ✅ TTL-aware event expiry
 *
 * NOT included (ga-pubsub-pro only):
 *   🔒 HMAC signing & tamper detection
 *   🔒 Rate limiting & payload size limits
 *   🔒 Authorization (bus.authorize())
 *   🔒 Replay attack prevention
 *   🔒 Transport adapters (Redis, Kafka, WebSocket…)
 *   🔒 Multi-tenant ScopedBus / namespace registry
 *
 * Extension point for ga-pubsub-pro:
 *   ProEventBus extends EventBus and overrides the two protected hooks:
 *     onBeforePublish(envelope)           — adds signing, rate limit, auth, size check
 *     onBeforeDispatch(envelope, local)   — adds signature verify, replay attack check
 */

import type {
  BusOptions,
  EventEnvelope,
  SubscriberCallback,
  SubscriberOptions,
  SubscriptionHandle,
  PublishOptions,
  MiddlewareFn,
  Validator,
  RequestOptions,
  RequestHandle,
  ResponderFn,
  BusMetrics,
} from './types.js';
import {
  GAPubSubError,
  ValidationFailedError,
  RequestTimeoutError,
  SubscriptionLimitError,
  MiddlewareAbortError,
} from './types.js';
import { SubscriptionIndex, wildcardMatcher } from './wildcard.js';
import { sanitizePayload, isExpired, generateId } from './security.js';
import { ReplayEngine } from './replay.js';
import { MetricsCollector, TelemetryDispatcher } from './metrics.js';

// ─────────────────────────────────────────────────────────────────────────────
// INTERNAL SUBSCRIBER RECORD
// ─────────────────────────────────────────────────────────────────────────────

interface SubscriberRecord<T = unknown> {
  id: string;
  eventPattern: string;
  callback: SubscriberCallback<T>;
  once: boolean;
  priority: number;
  /** Passed through to ga-pubsub-pro's authorizer — ignored in core */
  authContext?: Record<string, unknown>;
}

// ─────────────────────────────────────────────────────────────────────────────
// EVENT BUS
// ─────────────────────────────────────────────────────────────────────────────

export class EventBus {
  protected readonly namespace: string;
  protected readonly opts: BusOptions & {
    enableWildcard: boolean;
    maxSubscriptions: number;
    source: string;
  };

  // Subscriber storage
  protected readonly subscribers = new Map<string, SubscriberRecord>();
  protected readonly index = new SubscriptionIndex();

  // Middleware chain
  protected readonly middlewares: MiddlewareFn[] = [];

  // Schema validators
  protected readonly validators = new Map<string, Validator>();

  // Internal engines
  protected readonly replay: ReplayEngine;
  protected readonly metrics: MetricsCollector;
  protected readonly telemetry: TelemetryDispatcher;

  // Request/Response pending map
  protected readonly pendingRequests = new Map<string, {
    resolve: (envelope: EventEnvelope) => void;
    reject: (error: Error) => void;
    timer?: ReturnType<typeof setTimeout>;
  }>();

  constructor(options: BusOptions = {}) {
    this.namespace = options.namespace ?? 'default';
    this.opts = {
      enableWildcard: true,
      maxSubscriptions: 0,
      source: 'ga-pubsub',
      ...options,
      namespace: this.namespace,
    };

    this.replay   = new ReplayEngine(options.replay);
    this.metrics  = new MetricsCollector();
    this.telemetry = new TelemetryDispatcher(options.telemetry ?? {});
  }

  // ─── Protected hooks for ga-pubsub-pro to override ────────────────────────

  /**
   * Called AFTER the envelope is built, BEFORE schema validation.
   * ga-pubsub-pro overrides this to add:
   *   rate limiting, payload size enforcement, HMAC signing, authorization.
   * Throw any error to abort the publish.
   */
  protected async onBeforePublish(_envelope: EventEnvelope): Promise<void> {
    // no-op in free tier
  }

  /**
   * Called BEFORE delivering to subscribers.
   * ga-pubsub-pro overrides this to add:
   *   signature verification, replay attack detection.
   * Throw any error to abort delivery.
   * @param local  true = published locally, false = inbound from transport
   */
  protected async onBeforeDispatch(_envelope: EventEnvelope, _local: boolean): Promise<void> {
    // no-op in free tier
  }

  // ─── Middleware ────────────────────────────────────────────────────────────

  use<T = unknown>(fn: MiddlewareFn<T>): this {
    if (typeof fn !== 'function') throw new TypeError('Middleware must be a function');
    this.middlewares.push(fn as MiddlewareFn);
    return this;
  }

  // ─── Schema Validation ────────────────────────────────────────────────────

  registerSchema<T = unknown>(eventName: string, validator: Validator<T>): this {
    this.validators.set(eventName, validator as Validator);
    return this;
  }

  // ─── Subscribe ────────────────────────────────────────────────────────────

  subscribe<T = unknown>(
    eventName: string,
    callback: SubscriberCallback<T>,
    options: SubscriberOptions = {}
  ): SubscriptionHandle {
    if (typeof callback !== 'function') throw new TypeError('Callback must be a function');

    if (this.opts.maxSubscriptions > 0 && this.subscribers.size >= this.opts.maxSubscriptions) {
      throw new SubscriptionLimitError(this.opts.maxSubscriptions);
    }

    const id = generateId();
    const record: SubscriberRecord<T> = {
      id,
      eventPattern: eventName,
      callback,
      once: options.once ?? false,
      priority: options.priority ?? 0,
      ...(options.authContext !== undefined && { authContext: options.authContext }),
    };

    this.subscribers.set(id, record as SubscriberRecord);
    this.index.add(eventName, id);
    this.metrics.recordSubscribe();
    this.metrics.activeSubscriptions = this.subscribers.size;
    this.telemetry.onSubscribe(id, eventName);

    // Replay historical events to this subscriber
    if (options.replay !== false) {
      const history = this.replay.getHistory(eventName, {
        ...(options.replayLastMs !== undefined && { lastMs: options.replayLastMs }),
        ...(options.replayFilter !== undefined && { filter: options.replayFilter }),
      });
      if (history.length > 0) {
        this.metrics.recordReplay(history.length);
        this.telemetry.onReplay(eventName, history.length);
        for (const envelope of history) {
          Promise.resolve().then(async () => {
            try {
              await Promise.resolve(callback(envelope as EventEnvelope<T>));
            } catch (err) {
              this.handleError(err as Error, { phase: 'replay', eventName, subscriberId: id, envelope });
            }
          });
        }
      }
    }

    const bus = this;
    return {
      id,
      eventName,
      unsubscribe(): void { bus.unsubscribe(eventName, id); },
    };
  }

  subscribeOnce<T = unknown>(
    eventName: string,
    callback: SubscriberCallback<T>,
    options: Omit<SubscriberOptions, 'once'> = {}
  ): SubscriptionHandle {
    return this.subscribe(eventName, callback, { ...options, once: true });
  }

  unsubscribe(eventName: string, subscriberId: string): boolean {
    const record = this.subscribers.get(subscriberId);
    if (!record) return false;
    this.subscribers.delete(subscriberId);
    this.index.remove(eventName, subscriberId);
    this.metrics.recordUnsubscribe();
    this.metrics.activeSubscriptions = this.subscribers.size;
    this.telemetry.onUnsubscribe(subscriberId, eventName);
    return true;
  }

  unsubscribeAll(): void {
    this.subscribers.clear();
    this.index.clear();
    this.metrics.activeSubscriptions = 0;
  }

  // ─── Publish ──────────────────────────────────────────────────────────────

  async publish<T = unknown>(
    eventName: string,
    payload: T,
    options: PublishOptions = {}
  ): Promise<void> {
    const startMs = Date.now();

    // Build envelope
    const tenantId = options.tenantId ?? this.opts.tenantId;
    const envelope: EventEnvelope<T> = {
      id: generateId(),
      event: eventName,
      namespace: this.namespace,
      payload: sanitizePayload(payload),
      timestamp: Date.now(),
      correlationId: options.correlationId ?? generateId(),
      causationId: options.causationId ?? '',
      source: options.source ?? this.opts.source,
      version: options.version ?? '1',
      ...(tenantId !== undefined && { tenantId }),
      ...(options.userId !== undefined && { userId: options.userId }),
      ...(options.ttl !== undefined && { ttl: options.ttl }),
      ...(options.metadata !== undefined && { metadata: options.metadata }),
    };

    // Schema validation runs first (cheap, early exit before auth)
    await this.validateEnvelope(envelope);

    // PRO hook: rate limit, payload size, signing, authorization
    await this.onBeforePublish(envelope);

    // Store in replay history
    if (options.storeHistory !== false) {
      this.replay.store_(eventName, envelope);
      this.metrics.historySize = this.replay.size;
    }

    // Run middleware + dispatch to subscribers
    await this.runMiddlewareAndDispatch(envelope);

    // Record metrics
    const latencyMs = Date.now() - startMs;
    this.metrics.recordPublish(latencyMs);
    this.telemetry.onPublish(envelope, latencyMs);
  }

  // ─── Middleware pipeline ──────────────────────────────────────────────────

  private async runMiddlewareAndDispatch(envelope: EventEnvelope): Promise<void> {
    const middlewares = this.middlewares;
    let idx = 0;

    const next = async (): Promise<void> => {
      if (idx >= middlewares.length) {
        await this.dispatchToSubscribers(envelope, true);
        return;
      }
      const mw = middlewares[idx++]!;
      try {
        await Promise.resolve(mw(envelope, next));
      } catch (err) {
        if (err instanceof MiddlewareAbortError) throw err;
        this.metrics.recordMiddlewareRejection();
        this.telemetry.onMiddlewareRejection(envelope.event, (err as Error).message);
        this.handleError(err as Error, { phase: 'middleware', eventName: envelope.event, envelope });
        throw err;
      }
    };

    await next();
  }

  // ─── Dispatch to subscribers ──────────────────────────────────────────────

  protected async dispatchToSubscribers(
    envelope: EventEnvelope,
    local: boolean
  ): Promise<void> {
    // PRO hook: signature verify, replay attack check
    try {
      await this.onBeforeDispatch(envelope, local);
    } catch (err) {
      this.handleError(err as Error, { phase: 'transport', eventName: envelope.event, envelope });
      return;
    }

    // TTL check — expired events are silently dropped
    if (!local && isExpired(envelope)) return;

    // Collect matching subscribers
    const matchMap = this.index.getMatching(envelope.event);
    const candidates: Array<{ record: SubscriberRecord; pattern: string }> = [];

    for (const [pattern, ids] of matchMap.entries()) {
      for (const id of ids) {
        const record = this.subscribers.get(id);
        if (record) candidates.push({ record, pattern });
      }
    }

    if (candidates.length === 0) {
      this.resolveRequest(envelope);
      return;
    }

    // Priority ordering (descending)
    candidates.sort((a, b) => b.record.priority - a.record.priority);

    for (const { record, pattern } of candidates) {
      // Auto-unsubscribe once-subscribers
      if (record.once) this.unsubscribe(pattern, record.id);

      try {
        await Promise.resolve(record.callback(envelope));
      } catch (err) {
        this.metrics.recordFailedDelivery();
        this.handleError(err as Error, {
          phase: 'subscriber',
          eventName: envelope.event,
          subscriberId: record.id,
          envelope,
        });
      }
    }

    this.resolveRequest(envelope);
  }

  // ─── Validation ───────────────────────────────────────────────────────────

  private async validateEnvelope(envelope: EventEnvelope): Promise<void> {
    let validator = this.validators.get(envelope.event);
    if (!validator) {
      for (const [pattern, v] of this.validators.entries()) {
        if (pattern.includes('*') && this.matchesPattern(pattern, envelope.event)) {
          validator = v;
          break;
        }
      }
    }
    if (!validator) return;

    const result = validator.validate(envelope.payload);
    if (!result.valid) {
      this.metrics.recordValidationFailure();
      this.telemetry.onValidationFailure(envelope.event, result.errors);
      throw new ValidationFailedError(envelope.event, result.errors);
    }
  }

  protected matchesPattern(pattern: string, eventName: string): boolean {
    if (!this.opts.enableWildcard) return false;
    return wildcardMatcher.matches(pattern, eventName);
  }

  // ─── Request / Response (RPC) ─────────────────────────────────────────────

  request<TReq = unknown, TRes = unknown>(
    eventName: string,
    payload: TReq,
    options: RequestOptions = {}
  ): RequestHandle<TRes> {
    const correlationId = generateId();
    const timeoutMs = options.timeoutMs ?? 30_000;
    let cancelled = false;
    let resolveHandle!: (envelope: EventEnvelope<TRes>) => void;
    let rejectHandle!: (error: Error) => void;

    const responsePromise = new Promise<EventEnvelope<TRes>>((resolve, reject) => {
      resolveHandle = resolve;
      rejectHandle = reject;
    });

    this.pendingRequests.set(correlationId, {
      resolve: resolveHandle as (e: EventEnvelope) => void,
      reject: rejectHandle,
      timer: setTimeout(() => {
        if (cancelled) return;
        this.pendingRequests.delete(correlationId);
        this.metrics.recordRequestTimeout();
        rejectHandle(new RequestTimeoutError(eventName, timeoutMs));
      }, timeoutMs),
    });

    this.metrics.recordRequest();
    void this.publish(`${eventName}.__request__`, payload, { ...options, correlationId }).catch(rejectHandle);

    const self = this;
    return {
      response: responsePromise,
      cancel(): void {
        cancelled = true;
        const pending = self.pendingRequests.get(correlationId);
        if (pending) {
          if (pending.timer) clearTimeout(pending.timer);
          self.pendingRequests.delete(correlationId);
          pending.reject(new GAPubSubError('Request cancelled', 'REQUEST_CANCELLED'));
        }
      },
    };
  }

  respond<TReq = unknown, TRes = unknown>(
    eventName: string,
    handler: ResponderFn<TReq, TRes>
  ): SubscriptionHandle {
    return this.subscribe<TReq>(
      `${eventName}.__request__`,
      async (requestEnvelope) => {
        try {
          const result = await Promise.resolve(handler(requestEnvelope));
          await this.publish(
            `${eventName}.__response__.${requestEnvelope.correlationId}`,
            result,
            { correlationId: requestEnvelope.correlationId, causationId: requestEnvelope.id, storeHistory: false }
          );
        } catch (err) {
          this.handleError(err as Error, {
            phase: 'subscriber',
            eventName: `${eventName}.__request__`,
            envelope: requestEnvelope,
          });
        }
      }
    );
  }

  private resolveRequest(envelope: EventEnvelope): void {
    const match = envelope.event.match(/\.__response__\.([a-zA-Z0-9-]+)$/);
    if (!match) return;
    const correlationId = match[1]!;
    const pending = this.pendingRequests.get(correlationId);
    if (!pending) return;
    this.pendingRequests.delete(correlationId);
    if (pending.timer) clearTimeout(pending.timer);
    pending.resolve(envelope);
  }

  // ─── Introspection ────────────────────────────────────────────────────────

  getMetrics(): BusMetrics {
    this.metrics.historySize = this.replay.size;
    this.metrics.activeSubscriptions = this.subscribers.size;
    return this.metrics.snapshot();
  }

  getSubscriberCount(eventName: string): number {
    const ids = this.index.getMatching(eventName);
    let count = 0;
    for (const set of ids.values()) count += set.size;
    return count;
  }

  getRegisteredEvents(): string[] {
    return [...new Set([...this.subscribers.values()].map(r => r.eventPattern))];
  }

  // ─── Error handling ───────────────────────────────────────────────────────

  protected handleError(error: Error, context: import('./types.js').ErrorContext): void {
    this.telemetry.onError(error, context);
    const onError = this.opts.onError;
    if (typeof onError === 'function') {
      try { onError(error, context); } catch (hookErr) {
        console.error('[GA-PubSub] Error hook crashed:', hookErr);
        console.error('[GA-PubSub] Original error:', error);
      }
    } else {
      console.error(`[GA-PubSub] Unhandled error in phase [${context.phase}]:`, error);
    }
  }

  // ─── Lifecycle ────────────────────────────────────────────────────────────

  async destroy(): Promise<void> {
    this.unsubscribeAll();
    this.middlewares.length = 0;
    this.validators.clear();
    this.replay.destroy();

    for (const pending of this.pendingRequests.values()) {
      if (pending.timer) clearTimeout(pending.timer);
      pending.reject(new GAPubSubError('Bus destroyed', 'BUS_DESTROYED'));
    }
    this.pendingRequests.clear();
  }
}
