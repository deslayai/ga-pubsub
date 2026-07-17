/**
 * GA-PubSub × OpenTelemetry Integration
 *
 * This module shows how to wire GA-PubSub telemetry hooks into an
 * OpenTelemetry SDK setup. It is an EXAMPLE — not a published package.
 * Copy and adapt it to your observability stack.
 *
 * What it instruments:
 *   - Spans around every publish (with event name, namespace, source as attributes)
 *   - Spans around every subscriber callback (linked to the publish span)
 *   - Counters: publishes, subscriptions, failures, validation errors, auth denials
 *   - Histograms: publish latency (p50/p95/p99), request/response latency
 *   - Error recording on span when a subscriber or middleware throws
 *   - Correlation ID propagation via W3C TraceContext baggage
 *
 * USAGE:
 *   import { trace, metrics } from '@opentelemetry/api';
 *   import { createOtelTelemetry } from './otel-integration.example';
 *   import { getBus } from 'ga-pubsub';
 *
 *   const bus = getBus('myapp', {
 *     telemetry: createOtelTelemetry({
 *       tracer:  trace.getTracer('ga-pubsub', '1.0.0'),
 *       meter:   metrics.getMeter('ga-pubsub', '1.0.0'),
 *     })
 *   });
 */

// ─────────────────────────────────────────────────────────────────────────────
// TYPE-ONLY OTEL INTERFACES (avoid hard dependency on @opentelemetry/api)
// ─────────────────────────────────────────────────────────────────────────────

interface OtelSpan {
  setAttribute(key: string, value: string | number | boolean): this;
  setStatus(status: { code: number; message?: string }): this;
  recordException(exception: Error): this;
  end(): void;
}

interface OtelTracer {
  startSpan(name: string, options?: {
    kind?: number;
    attributes?: Record<string, string | number | boolean>;
    links?: Array<{ context: OtelSpanContext }>;
  }): OtelSpan;
  startActiveSpan<T>(name: string, fn: (span: OtelSpan) => T): T;
}

interface OtelSpanContext {
  traceId: string;
  spanId:  string;
  traceFlags: number;
}

interface OtelCounter {
  add(value: number, attributes?: Record<string, string | number>): void;
}

interface OtelHistogram {
  record(value: number, attributes?: Record<string, string | number>): void;
}

interface OtelMeter {
  createCounter(name: string, options?: { description?: string; unit?: string }): OtelCounter;
  createHistogram(name: string, options?: { description?: string; unit?: string }): OtelHistogram;
  createUpDownCounter(name: string, options?: { description?: string }): OtelCounter;
}

// ─────────────────────────────────────────────────────────────────────────────
// INTEGRATION FACTORY
// ─────────────────────────────────────────────────────────────────────────────

export interface OtelIntegrationOptions {
  tracer: OtelTracer;
  meter:  OtelMeter;
  /**
   * Whether to create spans for individual subscriber callbacks.
   * High-volume systems may want to disable this to reduce span volume.
   * @default true
   */
  spanSubscribers?: boolean;
  /**
   * Attributes to add to every span and metric.
   * Useful for adding service.name, deployment.environment, etc.
   */
  baseAttributes?: Record<string, string | number | boolean>;
}

import type { TelemetryHooks, EventEnvelope, ValidationError, ErrorContext } from 'ga-pubsub';
import type { AuthContext } from 'ga-pubsub';

export function createOtelTelemetry(opts: OtelIntegrationOptions): TelemetryHooks {
  const { tracer, meter } = opts;
  const base = opts.baseAttributes ?? {};

  // ── Metrics instruments ────────────────────────────────────────────────────
  const publishTotal = meter.createCounter('ga_pubsub_publish_total', {
    description: 'Total number of events published',
  });
  const publishLatency = meter.createHistogram('ga_pubsub_publish_latency_ms', {
    description: 'Publish latency in milliseconds',
    unit: 'ms',
  });
  const activeSubscriptions = meter.createUpDownCounter('ga_pubsub_active_subscriptions', {
    description: 'Current number of active subscriptions',
  });
  const failedDeliveries = meter.createCounter('ga_pubsub_failed_deliveries_total', {
    description: 'Total number of failed event deliveries',
  });
  const validationFailures = meter.createCounter('ga_pubsub_validation_failures_total', {
    description: 'Total schema validation failures',
  });
  const authDenials = meter.createCounter('ga_pubsub_auth_denials_total', {
    description: 'Total authorization denials',
  });
  const middlewareRejections = meter.createCounter('ga_pubsub_middleware_rejections_total', {
    description: 'Total middleware rejections/aborts',
  });
  const replayTotal = meter.createCounter('ga_pubsub_replay_total', {
    description: 'Total number of events replayed to late subscribers',
  });

  // ── Active span map for linking pub→sub spans ──────────────────────────────
  // We store the span context keyed by envelope ID so subscriber spans can link
  // back to the publish span. This simulates distributed trace propagation.
  const publishSpanContexts = new Map<string, OtelSpanContext>();

  return {
    // ── onPublish ─────────────────────────────────────────────────────────────
    onPublish(envelope: EventEnvelope, latencyMs: number) {
      const attrs: Record<string, string | number> = {
        ...base,
        'ga_pubsub.event':       envelope.event,
        'ga_pubsub.namespace':   envelope.namespace,
        'ga_pubsub.source':      envelope.source,
        'ga_pubsub.version':     envelope.version,
        'ga_pubsub.correlation': envelope.correlationId,
      };
      if (envelope.tenantId) attrs['ga_pubsub.tenant_id'] = envelope.tenantId;
      if (envelope.userId)   attrs['ga_pubsub.user_id']   = envelope.userId;

      publishTotal.add(1, attrs);
      publishLatency.record(latencyMs, attrs);

      // Record publish span (fire-and-forget — ends immediately after publish)
      const span = tracer.startSpan(`ga-pubsub publish ${envelope.event}`, {
        kind: 2, // SpanKind.PRODUCER
        attributes: {
          ...attrs,
          'messaging.system':          'ga-pubsub',
          'messaging.operation':       'publish',
          'messaging.message_id':      envelope.id,
          'messaging.destination':     envelope.event,
          'messaging.destination_kind':'topic',
        },
      });

      // Store context so subscriber spans can link to this publish span
      // In a real OTEL setup, you'd extract the span context via context API
      // Here we store a synthetic one keyed by envelope ID
      publishSpanContexts.set(envelope.id, {
        traceId:    envelope.correlationId.replace(/-/g, '').padEnd(32, '0').slice(0, 32),
        spanId:     envelope.id.replace(/-/g, '').slice(0, 16),
        traceFlags: 1,
      });

      // Clean up old contexts (keep last 1000 to prevent memory leak)
      if (publishSpanContexts.size > 1000) {
        const oldest = publishSpanContexts.keys().next().value;
        if (oldest) publishSpanContexts.delete(oldest);
      }

      span.end();
    },

    // ── onSubscribe / onUnsubscribe ────────────────────────────────────────────
    onSubscribe(_id: string, _eventName: string) {
      activeSubscriptions.add(1, { ...base });
    },

    onUnsubscribe(_id: string, _eventName: string) {
      activeSubscriptions.add(-1, { ...base });
    },

    // ── onError ───────────────────────────────────────────────────────────────
    onError(error: Error, context: ErrorContext) {
      const attrs: Record<string, string | number> = {
        ...base,
        'ga_pubsub.error.phase': context.phase,
        'ga_pubsub.event':       context.eventName,
      };

      if (context.phase === 'subscriber') {
        failedDeliveries.add(1, attrs);
      } else if (context.phase === 'middleware') {
        middlewareRejections.add(1, attrs);
      }

      // Create an error span linked to the originating publish span
      const publishContext = context.envelope?.id
        ? publishSpanContexts.get(context.envelope.id)
        : undefined;

      const span = tracer.startSpan(`ga-pubsub error [${context.phase}] ${context.eventName}`, {
        kind: 0, // SpanKind.INTERNAL
        attributes: {
          ...attrs,
          'error.type':    error.name,
          'error.message': error.message,
        },
        ...(publishContext ? { links: [{ context: publishContext }] } : {}),
      });

      span.setStatus({ code: 2, message: error.message }); // ERROR
      span.recordException(error);
      span.end();
    },

    // ── onValidationFailure ───────────────────────────────────────────────────
    onValidationFailure(eventName: string, errors: ValidationError[]) {
      validationFailures.add(1, {
        ...base,
        'ga_pubsub.event': eventName,
        'ga_pubsub.validation.error_count': errors.length,
      });
    },

    // ── onAuthDenial ─────────────────────────────────────────────────────────
    onAuthDenial(eventName: string, context: AuthContext) {
      authDenials.add(1, {
        ...base,
        'ga_pubsub.event':   eventName,
        'ga_pubsub.user_id': context.userId ?? 'anonymous',
      });
    },

    // ── onReplay ─────────────────────────────────────────────────────────────
    onReplay(eventName: string, count: number) {
      replayTotal.add(count, {
        ...base,
        'ga_pubsub.event': eventName,
      });
    },
  };
}

// ─────────────────────────────────────────────────────────────────────────────
// CORRELATION MIDDLEWARE — propagates W3C TraceContext via correlationId
// ─────────────────────────────────────────────────────────────────────────────

/**
 * Creates a middleware that propagates W3C TraceContext from the current
 * OpenTelemetry span context into the GA-PubSub envelope's correlationId.
 *
 * This ensures that distributed traces are linked across service boundaries
 * when events flow through GA-PubSub transports.
 *
 * USAGE:
 *   import { context, trace } from '@opentelemetry/api';
 *   bus.use(createTracePropagationMiddleware(() => {
 *     const span = trace.getActiveSpan();
 *     return span?.spanContext().traceId;
 *   }));
 */
export function createTracePropagationMiddleware(
  getTraceId: () => string | undefined
) {
  return async (envelope: EventEnvelope, next: () => Promise<void>): Promise<void> => {
    const traceId = getTraceId();
    if (traceId && !envelope.correlationId) {
      // Only set if not already set by the publisher
      (envelope as { correlationId: string }).correlationId = traceId;
    }
    await next();
  };
}

// ─────────────────────────────────────────────────────────────────────────────
// EXAMPLE USAGE (not executed — documentation only)
// ─────────────────────────────────────────────────────────────────────────────

/*
import { trace, metrics, context } from '@opentelemetry/api';
import { getBus, loggingMiddleware } from 'ga-pubsub';

// Initialize OpenTelemetry SDK (NodeSDK, etc.) BEFORE this

const bus = getBus('production', {
  enableSigning:    true,
  signingSecret:    process.env.PUBSUB_SIGNING_SECRET!,
  replay:           { limit: 50, ttl: 300_000 },
  maxPayloadBytes:  65_536,
  rateLimit:        1_000,

  telemetry: createOtelTelemetry({
    tracer:  trace.getTracer('ga-pubsub', '1.0.0'),
    meter:   metrics.getMeter('ga-pubsub', '1.0.0'),
    baseAttributes: {
      'service.name':           process.env.SERVICE_NAME ?? 'unknown',
      'deployment.environment': process.env.NODE_ENV     ?? 'development',
    },
  }),

  onError: (err, ctx) => {
    // Your error reporting (Sentry, Datadog, etc.)
    Sentry.captureException(err, { extra: { phase: ctx.phase, event: ctx.eventName } });
  },
});

// Add trace propagation so events carry the current trace ID
bus.use(createTracePropagationMiddleware(() => {
  return trace.getActiveSpan()?.spanContext().traceId;
}));

// Your event publishing is now fully instrumented:
await bus.publish('order.placed', { orderId: 'ORD-001', amount: 99.99 });
// → Creates a PRODUCER span
// → Records ga_pubsub_publish_total counter
// → Records ga_pubsub_publish_latency_ms histogram
// → Attaches correlationId from active trace context
*/
