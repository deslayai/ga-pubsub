/**
 * Payment Service — orchestrates the full payment lifecycle using GA-PubSub.
 *
 * Each stage of the payment flow publishes an event on the bus, demonstrating
 * real-world pub/sub usage in a payment platform context.
 */

import { getBus } from '../bus.js';
import { randomPayment, type Payment, type PaymentEvent } from './types.js';

export class PaymentService {

  /** Initiate a new payment — publishes payments.created */
  async createPayment(overrides: Partial<Payment> = {}): Promise<Payment> {
    const bus = getBus();
    const payment = randomPayment(overrides);

    await bus.publish<PaymentEvent>('payments.created', {
      payment,
    }, {
      userId: payment.fromUserId,
    });

    // Simulate async processing pipeline
    setTimeout(() => this.authorizePayment(payment), 300);
    return payment;
  }

  /** Authorize — publishes payments.authorized */
  async authorizePayment(payment: Payment): Promise<void> {
    const bus = getBus();
    const updated: Payment = { ...payment, status: 'authorized', updatedAt: Date.now() };

    await bus.publish<PaymentEvent>('payments.authorized', {
      payment: updated,
      previousStatus: 'created',
    }, { userId: payment.fromUserId });

    setTimeout(() => this.processPayment(updated), 500);
  }

  /** Process — publishes payments.processing */
  async processPayment(payment: Payment): Promise<void> {
    const bus = getBus();
    const updated: Payment = { ...payment, status: 'processing', updatedAt: Date.now() };

    await bus.publish<PaymentEvent>('payments.processing', {
      payment: updated,
      previousStatus: 'authorized',
    }, { userId: payment.fromUserId });

    // 90% success, 10% failure
    const succeed = Math.random() > 0.1;
    setTimeout(() => {
      if (succeed) this.completePayment(updated);
      else this.failPayment(updated, 'Insufficient funds');
    }, 800);
  }

  /** Complete — publishes payments.completed + payments.invoice.created */
  async completePayment(payment: Payment): Promise<void> {
    const bus = getBus();
    const updated: Payment = { ...payment, status: 'completed', updatedAt: Date.now() };

    await bus.publish<PaymentEvent>('payments.completed', {
      payment: updated,
      previousStatus: 'processing',
      message: `Payment of $${updated.amount.toFixed(2)} completed`,
    }, { userId: payment.fromUserId });

    // Also publish invoice event (demonstrates ** wildcard depth)
    await bus.publish('payments.invoice.created', {
      invoiceId:  `inv_${Math.random().toString(36).slice(2, 10)}`,
      paymentId:  updated.id,
      amount:     updated.amount,
      currency:   updated.currency,
      lineItems:  [{ description: updated.description, amount: updated.amount }],
      dueDate:    Date.now() + 30 * 24 * 60 * 60 * 1000,
    }, { userId: payment.fromUserId });
  }

  /** Fail — publishes payments.failed */
  async failPayment(payment: Payment, reason: string): Promise<void> {
    const bus = getBus();
    const updated: Payment = { ...payment, status: 'failed', updatedAt: Date.now() };

    await bus.publish<PaymentEvent>('payments.failed', {
      payment: updated,
      previousStatus: 'processing',
      message: reason,
    }, { userId: payment.fromUserId });
  }

  /** Refund — publishes payments.refunded */
  async refundPayment(payment: Payment): Promise<void> {
    const bus = getBus();
    const updated: Payment = { ...payment, status: 'refunded', updatedAt: Date.now() };

    await bus.publish<PaymentEvent>('payments.refunded', {
      payment: updated,
      message: `Refund of $${updated.amount.toFixed(2)} issued`,
    }, { userId: payment.fromUserId });
  }

  /** Subscription payment — demonstrates deep wildcard payments.subscription.** */
  async createSubscription(plan: string, amount: number): Promise<void> {
    const bus = getBus();
    const userId = `user_${Math.random().toString(36).slice(2, 8)}`;

    await bus.publish('payments.subscription.created', {
      subscriptionId: `sub_${Math.random().toString(36).slice(2, 10)}`,
      plan,
      amount,
      currency: 'USD',
      billingCycle: 'monthly',
      nextBillingDate: Date.now() + 30 * 24 * 60 * 60 * 1000,
    }, { userId });

    setTimeout(async () => {
      await bus.publish('payments.subscription.renewed', {
        subscriptionId: `sub_${Math.random().toString(36).slice(2, 10)}`,
        plan,
        amount,
        currency: 'USD',
        renewedAt: Date.now(),
      }, { userId });
    }, 1500);
  }
}

export const paymentService = new PaymentService();
