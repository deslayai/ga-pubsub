/**
 * GA-PubSub — Microservice Architecture Example
 *
 * Demonstrates a production-grade distributed order processing system
 * built with GA-PubSub and the NATS JetStream transport.
 *
 * Services:
 *   1. OrderService       — accepts new orders, publishes order.placed
 *   2. InventoryService   — reserves stock on order.placed, publishes inventory.reserved
 *   3. PaymentService     — charges on inventory.reserved, publishes payment.charged
 *   4. NotificationService — sends emails/SMS on payment.charged
 *   5. AuditService       — records ALL events for compliance
 *
 * Each service:
 *   - Has its own NATS consumer group (horizontal scaling)
 *   - Uses at-least-once delivery with manual ACK
 *   - Is independently deployable
 *   - Shares the same event schemas (validated with Zod)
 *
 * All services use the SAME ga-pubsub API surface:
 *   bus.publish() / bus.subscribe() / bus.request() / bus.respond()
 *
 * Transport selection is invisible to application code.
 */

// ─────────────────────────────────────────────────────────────────────────────
// SHARED: Event schemas (Zod)
// ─────────────────────────────────────────────────────────────────────────────

// In a monorepo these would live in packages/shared-events/src/schemas.ts
// and be imported by each service.

// import { z } from 'zod';
// import { zodValidator } from 'ga-pubsub/validators';
//
// export const OrderPlacedPayload = z.object({
//   orderId:    z.string().uuid(),
//   customerId: z.string().uuid(),
//   items:      z.array(z.object({
//     sku:      z.string(),
//     quantity: z.number().int().positive(),
//     price:    z.number().positive(),
//   })),
//   totalAmount: z.number().positive(),
//   currency:    z.string().length(3),
// });
//
// export const InventoryReservedPayload = z.object({
//   orderId:      z.string().uuid(),
//   reservations: z.array(z.object({ sku: z.string(), warehouseId: z.string() })),
// });
//
// export const PaymentChargedPayload = z.object({
//   orderId:       z.string().uuid(),
//   paymentId:     z.string(),
//   amountCharged: z.number().positive(),
//   currency:      z.string().length(3),
//   method:        z.enum(['card', 'bank_transfer', 'wallet']),
// });

// ─────────────────────────────────────────────────────────────────────────────
// SHARED: Bus factory per service
// ─────────────────────────────────────────────────────────────────────────────

import { getBus } from 'ga-pubsub';
import { loggingMiddleware, ttlGuardMiddleware } from 'ga-pubsub';
// import { NATSTransport } from '@ga-pubsub/nats';
// import { connect } from 'nats';
// import { createOtelTelemetry } from '../docs/otel-integration.example.js';
// import { trace, metrics } from '@opentelemetry/api';

interface ServiceBusConfig {
  serviceName: string;
  natsUrl?: string;
  consumerGroup?: string;
}

async function createServiceBus(config: ServiceBusConfig) {
  // const nc = await connect({ servers: config.natsUrl ?? 'nats://nats:4222' });
  // const transport = new NATSTransport({
  //   nc,
  //   enableJetStream: true,
  //   stream: 'ORDER_PIPELINE',
  //   queueGroup: config.consumerGroup ?? config.serviceName,
  // });

  const bus = getBus(config.serviceName, {
    source:          config.serviceName,
    enableSigning:   true,
    signingSecret:   process.env['PUBSUB_SECRET']!,
    maxPayloadBytes: 65_536,
    replay:          { limit: 0 }, // transport provides replay
    // transport,
    // telemetry: createOtelTelemetry({
    //   tracer:  trace.getTracer(config.serviceName),
    //   meter:   metrics.getMeter(config.serviceName),
    //   baseAttributes: { 'service.name': config.serviceName },
    // }),
    onError(err, ctx) {
      console.error(`[${config.serviceName}] Error in ${ctx.phase}:`, err.message, {
        event: ctx.eventName,
        subscriberId: ctx.subscriberId,
      });
    },
  });

  bus.use(ttlGuardMiddleware());
  bus.use(loggingMiddleware({ level: 'debug' }));

  return bus;
}

// ─────────────────────────────────────────────────────────────────────────────
// SERVICE 1: Order Service
// ─────────────────────────────────────────────────────────────────────────────

export class OrderService {
  private bus!: Awaited<ReturnType<typeof createServiceBus>>;

  async init() {
    this.bus = await createServiceBus({
      serviceName:   'order-service',
      consumerGroup: 'order-service',
    });

    // Respond to order creation requests (RPC pattern)
    this.bus.respond<
      { customerId: string; items: unknown[] },
      { orderId: string; status: string }
    >('order.create', async (request) => {
      const orderId = crypto.randomUUID();
      const payload = request.payload;

      // Persist to DB...
      // await db.orders.insert({ orderId, ...payload });

      // Publish domain event
      await this.bus.publish('order.placed', {
        orderId,
        customerId:  payload.customerId,
        items:       payload.items,
        totalAmount: 0, // calculated from items
        currency:    'USD',
      }, {
        causationId:  request.id,
        correlationId: request.correlationId,
      });

      return { orderId, status: 'placed' };
    });

    // Listen for order cancellations
    this.bus.subscribe('order.cancelled', async (envelope) => {
      const { orderId } = envelope.payload as { orderId: string };
      console.log(`[OrderService] Cancelling order ${orderId}`);
      // await db.orders.update(orderId, { status: 'cancelled' });
    });

    console.log('[OrderService] Ready');
  }

  async placeOrder(customerId: string, items: unknown[]): Promise<string> {
    const { response } = this.bus.request<
      { customerId: string; items: unknown[] },
      { orderId: string; status: string }
    >('order.create', { customerId, items }, { timeoutMs: 10_000 });

    const envelope = await response;
    return envelope.payload.orderId;
  }

  async shutdown() {
    await this.bus.destroy();
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// SERVICE 2: Inventory Service
// ─────────────────────────────────────────────────────────────────────────────

export class InventoryService {
  private bus!: Awaited<ReturnType<typeof createServiceBus>>;

  async init() {
    this.bus = await createServiceBus({
      serviceName:   'inventory-service',
      consumerGroup: 'inventory-service',
    });

    // Subscribe to order.placed — reserve stock for each order
    this.bus.subscribe<{ orderId: string; items: Array<{ sku: string; quantity: number }> }>(
      'order.placed',
      async (envelope) => {
        const { orderId, items } = envelope.payload;
        console.log(`[InventoryService] Reserving stock for order ${orderId}`);

        const reservations = await Promise.all(
          items.map(async (item) => {
            // const warehouseId = await db.inventory.reserve(item.sku, item.quantity);
            return { sku: item.sku, warehouseId: 'WH-001', quantity: item.quantity };
          })
        );

        await this.bus.publish('inventory.reserved', {
          orderId,
          reservations,
        }, {
          causationId:   envelope.id,
          correlationId: envelope.correlationId,
        });
      },
      { priority: 10 } // process before lower-priority listeners
    );

    // Handle reservation rollback on payment failure
    this.bus.subscribe<{ orderId: string }>(
      'payment.failed',
      async (envelope) => {
        const { orderId } = envelope.payload;
        console.log(`[InventoryService] Rolling back reservations for order ${orderId}`);
        // await db.inventory.releaseReservations(orderId);
      }
    );

    console.log('[InventoryService] Ready');
  }

  async shutdown() {
    await this.bus.destroy();
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// SERVICE 3: Payment Service
// ─────────────────────────────────────────────────────────────────────────────

export class PaymentService {
  private bus!: Awaited<ReturnType<typeof createServiceBus>>;

  async init() {
    this.bus = await createServiceBus({
      serviceName:   'payment-service',
      consumerGroup: 'payment-service',
    });

    // Process payment after inventory is confirmed reserved
    this.bus.subscribe<{ orderId: string }>(
      'inventory.reserved',
      async (envelope) => {
        const { orderId } = envelope.payload;
        console.log(`[PaymentService] Charging for order ${orderId}`);

        try {
          // const charge = await stripe.charges.create({ ... });
          const paymentId = `pay_${crypto.randomUUID().slice(0, 8)}`;

          await this.bus.publish('payment.charged', {
            orderId,
            paymentId,
            amountCharged: 0, // from order total
            currency:      'USD',
            method:        'card' as const,
          }, {
            causationId:   envelope.id,
            correlationId: envelope.correlationId,
          });
        } catch (err) {
          // Publish failure event for saga rollback
          await this.bus.publish('payment.failed', {
            orderId,
            reason: (err as Error).message,
          }, {
            causationId:   envelope.id,
            correlationId: envelope.correlationId,
          });
        }
      }
    );

    console.log('[PaymentService] Ready');
  }

  async shutdown() {
    await this.bus.destroy();
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// SERVICE 4: Notification Service
// ─────────────────────────────────────────────────────────────────────────────

export class NotificationService {
  private bus!: Awaited<ReturnType<typeof createServiceBus>>;

  async init() {
    this.bus = await createServiceBus({
      serviceName:   'notification-service',
      consumerGroup: 'notification-service',
    });

    // Notify customer on successful payment
    this.bus.subscribe<{ orderId: string; amountCharged: number; currency: string }>(
      'payment.charged',
      async (envelope) => {
        const { orderId, amountCharged, currency } = envelope.payload;
        console.log(`[NotificationService] Sending confirmation for order ${orderId}`);
        // await emailClient.send({ template: 'order-confirmation', data: { orderId, amountCharged } });
        // await smsClient.send({ message: `Order ${orderId} confirmed — ${currency} ${amountCharged}` });

        await this.bus.publish('notification.sent', {
          orderId,
          channels: ['email', 'sms'],
          templateId: 'order-confirmation',
        }, {
          causationId:   envelope.id,
          correlationId: envelope.correlationId,
        });
      }
    );

    // Notify on cancellation
    this.bus.subscribe<{ orderId: string }>('order.cancelled', async (envelope) => {
      const { orderId } = envelope.payload;
      console.log(`[NotificationService] Sending cancellation for order ${orderId}`);
    });

    console.log('[NotificationService] Ready');
  }

  async shutdown() {
    await this.bus.destroy();
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// SERVICE 5: Audit Service (wildcard — captures everything)
// ─────────────────────────────────────────────────────────────────────────────

export class AuditService {
  private bus!: Awaited<ReturnType<typeof createServiceBus>>;

  async init() {
    this.bus = await createServiceBus({
      serviceName:   'audit-service',
      consumerGroup: 'audit-service',
    });

    // Subscribe to ALL events — lowest priority so it never interferes
    this.bus.subscribe(
      '**',
      async (envelope) => {
        if (envelope.event.startsWith('$system')) return;

        const auditRecord = {
          eventId:       envelope.id,
          event:         envelope.event,
          correlationId: envelope.correlationId,
          causationId:   envelope.causationId,
          source:        envelope.source,
          timestamp:     envelope.timestamp,
          version:       envelope.version,
          // IMPORTANT: Only store a hash of sensitive payload fields for compliance
          payloadHash:   await hashPayload(envelope.payload),
        };

        console.log('[AuditService] Recording:', auditRecord.event, auditRecord.eventId);
        // await db.auditLog.insert(auditRecord);
      },
      { priority: -999 } // Always last
    );

    console.log('[AuditService] Ready — listening to all events');
  }

  async shutdown() {
    await this.bus.destroy();
  }
}

async function hashPayload(payload: unknown): Promise<string> {
  const data = new TextEncoder().encode(JSON.stringify(payload));
  const hash = await crypto.subtle.digest('SHA-256', data);
  return Array.from(new Uint8Array(hash))
    .map(b => b.toString(16).padStart(2, '0'))
    .join('');
}

// ─────────────────────────────────────────────────────────────────────────────
// MAIN — Start all services (in a monolith deploy; split into separate processes for prod)
// ─────────────────────────────────────────────────────────────────────────────

export async function startAllServices(): Promise<void> {
  const services = [
    new OrderService(),
    new InventoryService(),
    new PaymentService(),
    new NotificationService(),
    new AuditService(),
  ];

  await Promise.all(services.map(s => s.init()));
  console.log('\n✅ All services online — order pipeline ready\n');

  // Graceful shutdown
  const shutdown = async () => {
    console.log('\nShutting down...');
    await Promise.allSettled(services.map(s => s.shutdown()));
    process.exit(0);
  };

  process.on('SIGTERM', shutdown);
  process.on('SIGINT',  shutdown);
}

// Uncomment to run directly:
// startAllServices().catch(console.error);
