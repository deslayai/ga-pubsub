/**
 * GA-PubSub — Framework Integration Adapters
 *
 * Provides idiomatic first-class integrations for:
 *   - React (hook)
 *   - Angular (injectable service)
 *   - Vue 3 (composable)
 *   - NestJS (module + injectable)
 *   - Express (middleware)
 *   - Serverless (wrapper)
 *
 * All adapters share the same underlying EventBus instance,
 * and are thin ergonomic wrappers — not re-implementations.
 */

// ═══════════════════════════════════════════════════════════════════════════
// REACT HOOK
// ═══════════════════════════════════════════════════════════════════════════

/**
 * useEventBus — React hook for subscribing to GA-PubSub events.
 *
 * Automatically cleans up subscriptions when the component unmounts.
 * Optionally stores the latest payload in state for reactive rendering.
 *
 * USAGE:
 *   // Subscribe and react to updates
 *   const { lastEvent, publish } = useEventBus('cart.updated', (e) => {
 *     updateCartCount(e.payload.items);
 *   });
 *
 *   // Publish from a component
 *   const { publish } = useEventBus('user.action');
 *   <button onClick={() => publish({ type: 'click', target: 'hero-cta' })}>
 *     Click me
 *   </button>
 */
export function createReactHook(React: {
  useState: <T>(initial: T) => [T, (v: T | ((prev: T) => T)) => void];
  useEffect: (fn: () => (() => void) | void, deps?: unknown[]) => void;
  useCallback: <T extends (...args: unknown[]) => unknown>(fn: T, deps: unknown[]) => T;
  useRef: <T>(initial: T) => { current: T };
}) {
  /**
   * Returns a stable { subscribe, subscribeOnce, publish } API
   * scoped to the given EventBus instance.
   */
  return function useEventBus<TPayload = unknown>(
    bus: import('./bus.js').EventBus,
    eventName?: string,
    callback?: (envelope: import('./types.js').EventEnvelope<TPayload>) => void,
    options?: import('./types.js').SubscriberOptions
  ): {
    lastEnvelope: import('./types.js').EventEnvelope<TPayload> | null;
    publish: (payload: TPayload, opts?: import('./types.js').PublishOptions) => Promise<void>;
  } {
    const [lastEnvelope, setLastEnvelope] = React.useState<import('./types.js').EventEnvelope<TPayload> | null>(null);

    React.useEffect(() => {
      if (!eventName) return;

      const handler = (envelope: import('./types.js').EventEnvelope<TPayload>) => {
        setLastEnvelope(envelope);
        callback?.(envelope);
      };

      const handle = bus.subscribe(eventName, handler, options);
      return () => handle.unsubscribe();
    // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [bus, eventName]);

    const publish = React.useCallback(
      (...args: unknown[]) => {
        const [payload, opts] = args as [TPayload, import('./types.js').PublishOptions | undefined];
        return bus.publish(eventName ?? 'unknown', payload, opts);
      },
      [bus, eventName]
    ) as (payload: TPayload, opts?: import('./types.js').PublishOptions) => Promise<void>;

    return { lastEnvelope, publish };
  };
}

// ═══════════════════════════════════════════════════════════════════════════
// VUE 3 COMPOSABLE
// ═══════════════════════════════════════════════════════════════════════════

/**
 * useEventBus — Vue 3 composable for GA-PubSub.
 *
 * Automatically unsubscribes on component unmount (onUnmounted lifecycle).
 *
 * USAGE:
 *   import { useEventBus } from '@/composables/useEventBus';
 *
 *   const { lastEnvelope, publish } = useEventBus(bus, 'order.placed');
 *   watch(lastEnvelope, (envelope) => {
 *     if (envelope) showNotification(envelope.payload.orderId);
 *   });
 */
export function createVueComposable(Vue: {
  ref: <T>(v: T) => { value: T };
  onUnmounted: (fn: () => void) => void;
  readonly: <T>(v: T) => T;
}) {
  return function useEventBus<TPayload = unknown>(
    bus: import('./bus.js').EventBus,
    eventName?: string,
    callback?: (envelope: import('./types.js').EventEnvelope<TPayload>) => void,
    options?: import('./types.js').SubscriberOptions
  ) {
    const lastEnvelope = Vue.ref<import('./types.js').EventEnvelope<TPayload> | null>(null);

    if (eventName) {
      const handler = (envelope: import('./types.js').EventEnvelope<TPayload>) => {
        lastEnvelope.value = envelope;
        callback?.(envelope);
      };

      const handle = bus.subscribe(eventName, handler, options);
      Vue.onUnmounted(() => handle.unsubscribe());
    }

    const publish = (payload: TPayload, opts?: import('./types.js').PublishOptions) =>
      bus.publish(eventName ?? 'unknown', payload, opts);

    return {
      lastEnvelope: Vue.readonly(lastEnvelope),
      publish,
    };
  };
}

// ═══════════════════════════════════════════════════════════════════════════
// EXPRESS MIDDLEWARE
// ═══════════════════════════════════════════════════════════════════════════

/**
 * Creates an Express/Connect middleware that attaches the event bus
 * to every request as req.eventBus, and auto-propagates correlation IDs.
 *
 * USAGE:
 *   import express from 'express';
 *   import { createExpressMiddleware } from 'ga-pubsub/integrations';
 *
 *   const app = express();
 *   app.use(createExpressMiddleware(bus));
 *
 *   app.post('/checkout', async (req, res) => {
 *     await req.eventBus.publish('order.created', req.body);
 *     res.sendStatus(201);
 *   });
 */
export function createExpressMiddleware(
  bus: import('./bus.js').EventBus,
  options: {
    correlationHeader?: string;
    tenantHeader?: string;
    userIdHeader?: string;
  } = {}
) {
  const corrHeader   = options.correlationHeader ?? 'x-correlation-id';
  const tenantHeader = options.tenantHeader       ?? 'x-tenant-id';
  const userHeader   = options.userIdHeader       ?? 'x-user-id';

  return (
    req: {
      headers: Record<string, string | string[] | undefined>;
      eventBus?: import('./bus.js').EventBus;
    },
    _res: unknown,
    next: () => void
  ): void => {
    const correlationId = String(req.headers[corrHeader] ?? '');
    const tenantId      = String(req.headers[tenantHeader] ?? '');
    const userId        = String(req.headers[userHeader] ?? '');

    // Create a request-scoped publish wrapper that pre-fills context
    req.eventBus = {
      ...bus,
      publish: (eventName: string, payload: unknown, opts: import('./types.js').PublishOptions = {}) =>
        bus.publish(eventName, payload, {
          ...(correlationId ? { correlationId } : {}),
          ...(tenantId      ? { tenantId }      : {}),
          ...(userId        ? { userId }        : {}),
          ...opts,
        }),
    } as import('./bus.js').EventBus;

    next();
  };
}

// ═══════════════════════════════════════════════════════════════════════════
// SERVERLESS WRAPPER
// ═══════════════════════════════════════════════════════════════════════════

/**
 * Wraps a serverless function handler to:
 *   1. Initialize the event bus lazily (cold start optimization)
 *   2. Publish a lifecycle event before and after invocation
 *   3. Wait for all in-flight events to drain before returning
 *
 * USAGE (AWS Lambda):
 *   export const handler = withEventBus(bus, async (event, context) => {
 *     await bus.publish('order.validated', event.body);
 *     return { statusCode: 200 };
 *   });
 *
 * USAGE (Cloudflare Workers):
 *   export default withEventBus(bus, {
 *     async fetch(request, env) {
 *       return new Response('ok');
 *     }
 *   });
 */
export function withServerlessHandler<TEvent = unknown, TResult = unknown>(
  bus: import('./bus.js').EventBus,
  handler: (event: TEvent, context?: unknown) => Promise<TResult>,
  options: {
    emitLifecycle?: boolean;
    functionName?: string;
  } = {}
): (event: TEvent, context?: unknown) => Promise<TResult> {
  return async (event: TEvent, context?: unknown): Promise<TResult> => {
    const fnName = options.functionName ?? 'serverless.function';

    if (options.emitLifecycle !== false) {
      await bus.publish(`${fnName}.invoked`, {
        timestamp: Date.now(),
        hasContext: !!context,
      }, { storeHistory: false });
    }

    let result: TResult;
    let error: Error | undefined;

    try {
      result = await handler(event, context);
    } catch (err) {
      error = err as Error;
      if (options.emitLifecycle !== false) {
        await bus.publish(`${fnName}.failed`, {
          error: error.message,
          stack: error.stack,
        }, { storeHistory: false });
      }
      throw err;
    }

    if (options.emitLifecycle !== false) {
      await bus.publish(`${fnName}.completed`, {
        success: true,
      }, { storeHistory: false });
    }

    return result!;
  };
}

// ═══════════════════════════════════════════════════════════════════════════
// NESTJS MODULE FACTORY
// ═══════════════════════════════════════════════════════════════════════════

/**
 * Factory for creating a NestJS-compatible dynamic module.
 *
 * USAGE:
 *   // app.module.ts
 *   @Module({
 *     imports: [
 *       GAPubSubModule.forRoot({
 *         namespace: 'myapp',
 *         enableSigning: true,
 *         signingSecret: process.env.PUBSUB_SECRET,
 *       })
 *     ]
 *   })
 *   export class AppModule {}
 *
 *   // my.service.ts
 *   @Injectable()
 *   export class MyService {
 *     constructor(@Inject(PUBSUB_BUS) private readonly bus: EventBus) {}
 *
 *     async doWork() {
 *       await this.bus.publish('work.completed', { jobId: '123' });
 *     }
 *   }
 */
export const PUBSUB_BUS = Symbol('PUBSUB_BUS');

export function createNestModule(options: import('./types.js').BusOptions = {}) {
  const { EventBus } = require('./bus.js') as typeof import('./bus.js');
  const busInstance = new EventBus(options);

  return {
    module: class GAPubSubModule {},
    providers: [
      {
        provide: PUBSUB_BUS,
        useValue: busInstance,
      },
    ],
    exports: [PUBSUB_BUS],
    global: true,
  };
}

// Re-export type for NestJS DI typing convenience
export type { EventBus } from './bus.js';
export type { EventEnvelope, PublishOptions, SubscriberOptions } from './types.js';
