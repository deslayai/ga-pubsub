import { EventBus, ValidationFailedError } from 'ga-pubsub';

type OrderCreated = { id: string; total: number };

const bus = new EventBus({
  source: 'local-training',
  replay: { limit: 20, ttl: 60_000, replayWildcards: true },
  telemetry: {
    onPublish: (envelope, latencyMs) =>
      console.log('[telemetry]', envelope.event, `${latencyMs.toFixed(2)}ms`),
    onError: (error, context) =>
      console.error('[telemetry:error]', context.phase, error.message),
  },
});

bus.registerSchema('orders.created', {
  name: 'order-created',
  validate(payload) {
    const order = payload as Partial<OrderCreated>;
    return typeof order.id === 'string' && typeof order.total === 'number'
      ? { valid: true as const }
      : {
          valid: false as const,
          errors: [{ path: 'orders.created', message: 'id and total are required' }],
        };
  },
});

bus.use(async (envelope, next) => {
  console.log('[middleware]', envelope.event);
  await next();
});

const orders = bus.subscribe<OrderCreated>(
  'orders.**',
  envelope => console.log('[subscriber]', envelope.event, envelope.payload),
  { priority: 100, replay: true },
);

bus.subscribeOnce('application.ready', envelope =>
  console.log('[once]', envelope.payload),
);

bus.respond<{ a: number; b: number }, number>(
  'math.add',
  envelope => envelope.payload.a + envelope.payload.b,
);

await bus.publish('orders.created', { id: 'ord_123', total: 42 }, { ttl: 60_000 });
await bus.publish('application.ready', { version: '3.1.0' });
await bus.publish('application.ready', { ignored: true });

const request = bus.request<{ a: number; b: number }, number>(
  'math.add',
  { a: 20, b: 22 },
  { timeoutMs: 3_000 },
);
console.log('[rpc]', (await request.response).payload);

try {
  await bus.publish('orders.created', { id: 'invalid' } as OrderCreated);
} catch (error) {
  if (error instanceof ValidationFailedError) {
    console.log('[expected validation error]', error.errors);
  } else {
    throw error;
  }
}

console.log('[metrics]', bus.getMetrics());
orders.unsubscribe();
bus.destroy();
