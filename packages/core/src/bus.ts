/**
 * GA-PubSub Core — EventBus  (FREE tier)
 *
 * Included in ga-pubsub (Elastic-2.0):
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
    expectedResponseEvent: string;
    timer?: ReturnType<typeof setTimeout>;
  }>();

  private readonly responderEvents = new Set<string>();
  private readonly recentEnvelopeIds = new Map<string, number>();
  private destroyed = false;

  constructor(options: BusOptions = {}) {
    this.namespace = options.namespace ?? 'default';
    assertName(this.namespace, 'namespace', false, 128);
    assertNonNegativeInteger(options.maxSubscriptions ?? 0, 'maxSubscriptions');
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
    this.assertActive();
    if (typeof fn !== 'function') throw new TypeError('Middleware must be a function');
    this.middlewares.push(fn as MiddlewareFn);
    return this;
  }

  // ─── Schema Validation ────────────────────────────────────────────────────

  registerSchema<T = unknown>(eventName: string, validator: Validator<T>): this {
    this.assertActive();
    assertName(eventName, 'event pattern', this.opts.enableWildcard);
    if (!validator || typeof validator.validate !== 'function') {
      throw new TypeError('Validator must expose a validate function');
    }
    this.validators.set(eventName, validator as Validator);
    return this;
  }

  // ─── Subscribe ────────────────────────────────────────────────────────────

  subscribe<T = unknown>(
    eventName: string,
    callback: SubscriberCallback<T>,
    options: SubscriberOptions = {}
  ): SubscriptionHandle {
    this.assertActive();
    assertName(eventName, 'event pattern', this.opts.enableWildcard);
    if (typeof callback !== 'function') throw new TypeError('Callback must be a function');
    const priority = options.priority ?? 0;
    if (!Number.isFinite(priority)) throw new RangeError('Subscriber priority must be finite');

    // Resolve replay before registration so a throwing filter cannot leak a subscription.
    const history = options.replay === false
      ? []
      : this.replay.getHistory(eventName, {
          ...(options.replayLastMs !== undefined && { lastMs: options.replayLastMs }),
          ...(options.replayFilter !== undefined && { filter: options.replayFilter }),
        });

    if (this.opts.maxSubscriptions > 0 && this.subscribers.size >= this.opts.maxSubscriptions) {
      throw new SubscriptionLimitError(this.opts.maxSubscriptions);
    }

    const id = generateId();
    const record: SubscriberRecord<T> = {
      id,
      eventPattern: eventName,
      callback,
      once: options.once ?? false,
      priority,
      ...(options.authContext !== undefined && { authContext: options.authContext }),
    };

    this.subscribers.set(id, record as SubscriberRecord);
    this.index.add(eventName, id);
    this.metrics.recordSubscribe();
    this.metrics.activeSubscriptions = this.subscribers.size;
    this.telemetry.onSubscribe(id, eventName);

    // Replay historical events to this subscriber
    if (history.length > 0) {
        const replayItems = record.once ? history.slice(0, 1) : history;
        this.metrics.recordReplay(replayItems.length);
        this.telemetry.onReplay(eventName, replayItems.length);
        for (const envelope of replayItems) {
          Promise.resolve().then(async () => {
            if (!this.subscribers.has(id)) return;
            if (record.once) this.unsubscribe(eventName, id);
            try {
              await Promise.resolve(callback(envelope as EventEnvelope<T>));
            } catch (err) {
              this.metrics.recordFailedDelivery();
              this.handleError(err as Error, { phase: 'replay', eventName, subscriberId: id, envelope });
            }
          });
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
    if (record.eventPattern !== eventName) return false;
    this.subscribers.delete(subscriberId);
    this.index.remove(record.eventPattern, subscriberId);
    this.metrics.recordUnsubscribe();
    this.metrics.activeSubscriptions = this.subscribers.size;
    this.telemetry.onUnsubscribe(subscriberId, eventName);
    return true;
  }

  unsubscribeAll(): void {
    for (const record of this.subscribers.values()) {
      this.metrics.recordUnsubscribe();
      this.telemetry.onUnsubscribe(record.id, record.eventPattern);
    }
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
    this.assertActive();
    assertName(eventName, 'event name', false);
    validatePublishOptions(options);
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
      ...(options.metadata !== undefined && { metadata: sanitizePayload(options.metadata) }),
    };

    // Schema validation runs first (cheap, early exit before auth)
    await this.validateEnvelope(envelope);

    // Prevent broker self-echo from delivering the same envelope twice.
    this.rememberEnvelopeId(envelope.id);

    // PRO hook: rate limit, payload size, signing, authorization
    await this.onBeforePublish(envelope);

    // Run middleware + dispatch to subscribers
    await this.runMiddlewareAndDispatch(envelope);

    // Store only successfully processed events.
    if (options.storeHistory !== false) {
      this.replay.store_(eventName, envelope);
      this.metrics.historySize = this.replay.size;
    }

    // Record metrics
    const latencyMs = Date.now() - startMs;
    this.metrics.recordPublish(latencyMs);
    this.telemetry.onPublish(envelope, latencyMs);
  }

  // ─── Middleware pipeline ──────────────────────────────────────────────────

  private async runMiddlewareAndDispatch(
    envelope: EventEnvelope,
    local = true,
    dispatchPrechecked = false
  ): Promise<void> {
    const middlewares = this.middlewares;
    let lastIndex = -1;
    let reported = false;

    const dispatch = async (index: number): Promise<void> => {
      if (index <= lastIndex) throw new Error('Middleware next() called more than once');
      lastIndex = index;
      if (index >= middlewares.length) {
        await this.dispatchToSubscribers(envelope, local, dispatchPrechecked);
        return;
      }
      const mw = middlewares[index]!;
      try {
        await Promise.resolve(mw(envelope, () => dispatch(index + 1)));
      } catch (err) {
        if (err instanceof MiddlewareAbortError) throw err;
        if (!reported && !(err instanceof AggregateError)) {
          reported = true;
          this.metrics.recordMiddlewareRejection();
          this.telemetry.onMiddlewareRejection(envelope.event, (err as Error).message);
          this.handleError(err as Error, { phase: 'middleware', eventName: envelope.event, envelope });
        }
        throw err;
      }
    };

    await dispatch(0);
  }

  /** Validates and processes an envelope received from a transport. */
  protected async processInboundEnvelope(input: EventEnvelope): Promise<void> {
    this.assertActive();
    if (!input || typeof input !== 'object') throw new TypeError('Inbound envelope must be an object');
    const envelope = sanitizePayload(input);
    assertName(envelope.event, 'event name', false);
    if (envelope.namespace !== this.namespace) {
      throw new GAPubSubError(
        `Inbound namespace mismatch: expected "${this.namespace}", got "${envelope.namespace}"`,
        'NAMESPACE_MISMATCH'
      );
    }
    if (this.recentEnvelopeIds.has(envelope.id)) return;
    this.rememberEnvelopeId(envelope.id);
    try {
      await this.onBeforeDispatch(envelope, false);
      await this.validateEnvelope(envelope);
      await this.runMiddlewareAndDispatch(envelope, false, true);
      this.replay.store_(envelope.event, envelope);
      this.metrics.historySize = this.replay.size;
      this.metrics.recordPublish(0);
      this.telemetry.onPublish(envelope, 0);
    } catch (error) {
      this.recentEnvelopeIds.delete(envelope.id);
      throw error;
    }
  }

  // ─── Dispatch to subscribers ──────────────────────────────────────────────

  protected async dispatchToSubscribers(
    envelope: EventEnvelope,
    local: boolean,
    prechecked = false
  ): Promise<void> {
    // PRO hook: signature verify, replay attack check
    if (!prechecked) {
      try {
        await this.onBeforeDispatch(envelope, local);
      } catch (err) {
        this.handleError(err as Error, { phase: 'transport', eventName: envelope.event, envelope });
        throw err;
      }
    }

    // TTL check — expired events are silently dropped
    if (isExpired(envelope)) return;

    // Resolve RPC before unrelated subscribers can delay the response.
    this.resolveRequest(envelope);

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
      return;
    }

    // Priority ordering (descending)
    candidates.sort((a, b) => b.record.priority - a.record.priority);

    const deliveryErrors: Error[] = [];
    for (const { record, pattern } of candidates) {
      // Auto-unsubscribe once-subscribers
      if (record.once) this.unsubscribe(pattern, record.id);

      try {
        await Promise.resolve(record.callback(envelope));
      } catch (err) {
        deliveryErrors.push(err as Error);
        this.metrics.recordFailedDelivery();
        this.handleError(err as Error, {
          phase: 'subscriber',
          eventName: envelope.event,
          subscriberId: record.id,
          envelope,
        });
      }
    }

    if (deliveryErrors.length > 0) {
      throw new AggregateError(deliveryErrors, `One or more subscribers failed for "${envelope.event}"`);
    }
  }

  // ─── Validation ───────────────────────────────────────────────────────────

  private async validateEnvelope(envelope: EventEnvelope): Promise<void> {
    let validator = this.validators.get(envelope.event);
    let selectedPattern = envelope.event;
    if (!validator) {
      for (const [pattern, v] of this.validators.entries()) {
        if (pattern.includes('*') && this.matchesPattern(pattern, envelope.event)) {
          if (!validator || validatorSpecificity(pattern) > validatorSpecificity(selectedPattern)) {
            validator = v;
            selectedPattern = pattern;
          }
        }
      }
    }
    if (!validator) return;

    const result = await Promise.resolve(validator.validate(envelope.payload));
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
    this.assertActive();
    assertName(eventName, 'request event name', false);
    const correlationId = generateId();
    const timeoutMs = options.timeoutMs ?? 30_000;
    if (!Number.isFinite(timeoutMs) || timeoutMs <= 0) {
      throw new RangeError('timeoutMs must be a finite number > 0');
    }
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
      expectedResponseEvent: `${eventName}.__response__.${correlationId}`,
      timer: setTimeout(() => {
        if (cancelled) return;
        this.pendingRequests.delete(correlationId);
        this.metrics.recordRequestTimeout();
        rejectHandle(new RequestTimeoutError(eventName, timeoutMs));
      }, timeoutMs),
    });

    this.metrics.recordRequest();
    void this.publish(`${eventName}.__request__`, payload, {
      ...options,
      correlationId,
      storeHistory: false,
    }).catch(error => {
      const pending = this.pendingRequests.get(correlationId);
      if (!pending) return;
      if (pending.timer) clearTimeout(pending.timer);
      this.pendingRequests.delete(correlationId);
      pending.reject(error as Error);
    });

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
    this.assertActive();
    assertName(eventName, 'response event name', false);
    if (this.responderEvents.has(eventName)) {
      throw new GAPubSubError(`A responder is already registered for "${eventName}"`, 'RESPONDER_EXISTS');
    }
    this.responderEvents.add(eventName);
    const requestEvent = `${eventName}.__request__`;
    const handle = this.subscribe<TReq>(
      requestEvent,
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
          await this.publish(
            `${eventName}.__response__.${requestEnvelope.correlationId}`,
            undefined,
            {
              correlationId: requestEnvelope.correlationId,
              causationId: requestEnvelope.id,
              storeHistory: false,
              metadata: {
                rpcError: {
                  code: err instanceof GAPubSubError ? err.code : 'RESPONDER_FAILED',
                  message: (err as Error).message,
                },
              },
            }
          );
        }
      },
      { replay: false }
    );
    return {
      ...handle,
      unsubscribe: () => {
        handle.unsubscribe();
        this.responderEvents.delete(eventName);
      },
    };
  }

  private resolveRequest(envelope: EventEnvelope): void {
    const match = envelope.event.match(/\.__response__\.([a-zA-Z0-9-]+)$/);
    if (!match) return;
    const correlationId = match[1]!;
    const pending = this.pendingRequests.get(correlationId);
    if (!pending) return;
    if (envelope.event !== pending.expectedResponseEvent) return;
    this.pendingRequests.delete(correlationId);
    if (pending.timer) clearTimeout(pending.timer);
    const rpcError = envelope.metadata?.['rpcError'];
    if (rpcError && typeof rpcError === 'object') {
      const value = rpcError as { code?: unknown; message?: unknown };
      pending.reject(new GAPubSubError(
        typeof value.message === 'string' ? value.message : 'Remote responder failed',
        typeof value.code === 'string' ? value.code : 'RESPONDER_FAILED'
      ));
    } else {
      pending.resolve(envelope);
    }
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
    for (const set of ids.values()) {
      for (const id of set) if (this.subscribers.has(id)) count++;
    }
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
    if (this.destroyed) return;
    this.destroyed = true;
    this.unsubscribeAll();
    this.middlewares.length = 0;
    this.validators.clear();
    this.responderEvents.clear();
    this.recentEnvelopeIds.clear();
    this.replay.destroy();

    for (const pending of this.pendingRequests.values()) {
      if (pending.timer) clearTimeout(pending.timer);
      pending.reject(new GAPubSubError('Bus destroyed', 'BUS_DESTROYED'));
    }
    this.pendingRequests.clear();
  }

  private assertActive(): void {
    if (this.destroyed) throw new GAPubSubError('Bus has been destroyed', 'BUS_DESTROYED');
  }

  private rememberEnvelopeId(id: string): void {
    if (typeof id !== 'string' || id.length === 0 || id.length > 128) {
      throw new TypeError('Envelope id must be a non-empty string of at most 128 characters');
    }
    if (this.recentEnvelopeIds.size >= 10_000) {
      const oldest = this.recentEnvelopeIds.keys().next().value;
      if (oldest !== undefined) this.recentEnvelopeIds.delete(oldest);
    }
    this.recentEnvelopeIds.set(id, Date.now());
  }
}

function assertName(value: string, label: string, allowWildcards: boolean, maxLength = 512): void {
  if (typeof value !== 'string' || value.length === 0 || value.length > maxLength) {
    throw new TypeError(`${label} must be a non-empty string of at most ${maxLength} characters`);
  }
  const segments = value.split('.');
  if (segments.some(segment => segment.length === 0)) throw new TypeError(`${label} cannot contain empty segments`);
  for (const segment of segments) {
    if (segment.includes('*') && (!allowWildcards || (segment !== '*' && segment !== '**'))) {
      throw new TypeError(`${label} contains an invalid wildcard segment`);
    }
  }
}

function assertNonNegativeInteger(value: number, label: string): void {
  if (!Number.isInteger(value) || value < 0) throw new RangeError(`${label} must be an integer >= 0`);
}

function validatePublishOptions(options: PublishOptions): void {
  if (options.ttl !== undefined && (!Number.isFinite(options.ttl) || options.ttl <= 0)) {
    throw new RangeError('ttl must be a finite number > 0');
  }
  for (const [label, value] of [
    ['correlationId', options.correlationId],
    ['causationId', options.causationId],
    ['source', options.source],
    ['version', options.version],
    ['tenantId', options.tenantId],
    ['userId', options.userId],
  ] as const) {
    if (value !== undefined && (typeof value !== 'string' || value.length > 256)) {
      throw new TypeError(`${label} must be a string of at most 256 characters`);
    }
  }
}

function validatorSpecificity(pattern: string): number {
  return pattern.split('.').reduce((score, segment) => score + (segment === '**' ? 0 : segment === '*' ? 1 : 4), 0);
}
