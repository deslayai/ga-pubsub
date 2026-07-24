/**
 * GA-PubSub Core Demo — EventBus Setup
 *
 * Uses ONLY ga-pubsub (free, Elastic-2.0) features:
 *   - Middleware pipeline (logging + enrichment)
 *   - Schema validation on payment events
 *   - Replay engine (last 200 events, TTL-aware)
 *   - Error telemetry via onError hook
 */

import {
  EventBus,
  ValidationFailedError,
  loggingMiddleware,
} from 'ga-pubsub';
import type { EventEnvelope } from 'ga-pubsub';
import { serverConfig } from './config.js';

let _bus: EventBus | null = null;

export function getBus(): EventBus {
  if (!_bus) throw new Error('Bus not initialized — call initBus() first');
  return _bus;
}

export interface BusInitResult { bus: EventBus }

export async function initBus(): Promise<BusInitResult> {
  _bus = new EventBus({
    namespace: 'core-demo',
    source:    'ga-pubsub-demo-backend',
    replay:    { limit: 200 },
    onError: (err, ctx) => {
      const label =
        err instanceof ValidationFailedError ? 'VALIDATION_FAILED' : 'ERROR';
      console.error(`[Bus] ${label} in ${ctx.phase} on "${ctx.eventName}":`, err.message);
      busErrorHandler?.(label, err.message, ctx.eventName, ctx.phase);
    },
  });

  // Middleware: audit log
  _bus.use(loggingMiddleware({ level: 'debug' }));

  // Middleware: payment enrichment
  _bus.use(async (envelope: EventEnvelope, next) => {
    if (envelope.event.startsWith('payments.')) {
      const p = envelope.payload as Record<string, unknown>;
      p['processedAt']  = Date.now();
      p['serverVersion'] = '1.0.0';
    }
    await next();
  });

  // Schema: payments.created
  _bus.registerSchema('payments.created', {
    name: 'PaymentCreated',
    validate(payload: Record<string, unknown>) {
      const errors: Array<{ path: string; message: string }> = [];
      if (typeof payload['amount'] !== 'number' || payload['amount'] <= 0)
        errors.push({ path: 'amount', message: 'amount must be a positive number' });
      if (typeof payload['currency'] !== 'string')
        errors.push({ path: 'currency', message: 'currency is required' });
      if (typeof payload['fromUserId'] !== 'string')
        errors.push({ path: 'fromUserId', message: 'fromUserId is required' });
      return errors.length ? { valid: false, errors } : { valid: true };
    },
  });

  // Schema: payments.completed
  _bus.registerSchema('payments.completed', {
    name: 'PaymentCompleted',
    validate(payload: Record<string, unknown>) {
      const p = payload['payment'] as Record<string, unknown> | undefined;
      if (!p || typeof p['id'] !== 'string')
        return { valid: false, errors: [{ path: 'payment.id', message: 'payment.id required' }] };
      return { valid: true };
    },
  });

  console.log('[Bus] GA-PubSub Core EventBus initialized');
  console.log(`[Bus] CORS origin: ${serverConfig.corsOrigin}`);
  return { bus: _bus };
}

type ErrorHandler = (code: string, message: string, event: string, phase: string) => void;
let busErrorHandler: ErrorHandler | null = null;
export function setBusErrorHandler(handler: ErrorHandler): void {
  busErrorHandler = handler;
}
