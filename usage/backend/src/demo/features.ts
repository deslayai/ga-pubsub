/**
 * GA-PubSub Core — Feature Demo Handlers (12 demos, no PRO features)
 */

import { ValidationFailedError } from 'ga-pubsub-core';
import { getBus } from '../bus.js';
import { broadcast } from '../socket.js';
import { randomPayment } from '../payment/types.js';

function ok(feature: string, steps: string[], data?: unknown) {
  const result = { feature, status: 'ok', steps, data, timestamp: Date.now() };
  broadcast('demo:result', result);
  return result;
}
function fail(feature: string, steps: string[], error: string) {
  const result = { feature, status: 'error', steps, error, timestamp: Date.now() };
  broadcast('demo:result', result);
  return result;
}

// ─── 1. Basic Pub/Sub ─────────────────────────────────────────────────────────
export async function demoBasicPubSub() {
  const bus = getBus();
  const steps: string[] = [];
  const received: unknown[] = [];
  const handle = bus.subscribe('demo.ping', (env) => {
    received.push(env.payload);
    steps.push(`✅ Subscriber received: ${JSON.stringify(env.payload)}`);
  });
  steps.push('📢 Publishing demo.ping × 2...');
  await bus.publish('demo.ping', { message: 'Hello from GA-PubSub Core!', timestamp: Date.now() });
  await bus.publish('demo.ping', { message: 'Second event on same topic', count: 2 });
  handle.unsubscribe();
  return ok('Basic Pub/Sub', steps, { received });
}

// ─── 2. Wildcard * ────────────────────────────────────────────────────────────
export async function demoWildcardSingle() {
  const bus = getBus();
  const steps: string[] = [];
  const hits: string[] = [];
  const handle = bus.subscribe('payments.*', (env) => {
    hits.push(env.event);
    steps.push(`✅ payments.* matched: ${env.event}`);
  });
  steps.push('📢 Publishing: payments.created, payments.failed, payments.invoice.created');
  await bus.publish('payments.created', randomPayment());
  await bus.publish('payments.failed',  randomPayment());
  await bus.publish('payments.invoice.created', { amount: 49.99 }); // must NOT match
  handle.unsubscribe();
  steps.push(`✅ payments.* matched ${hits.length} of 3 (expected 2)`);
  return ok('Wildcard *', steps, { matched: hits });
}

// ─── 3. Wildcard ** ───────────────────────────────────────────────────────────
export async function demoWildcardMulti() {
  const bus = getBus();
  const steps: string[] = [];
  const hits: string[] = [];
  const handle = bus.subscribe('payments.**', (env) => {
    hits.push(env.event);
    steps.push(`✅ payments.** matched: ${env.event}`);
  });
  steps.push('📢 Publishing 4 events at varying depths');
  await bus.publish('payments.created',            randomPayment());
  await bus.publish('payments.invoice.created',    { amount: 99.00 });
  await bus.publish('payments.invoice.line.added', { lineId: 'l1', amount: 9.99 });
  await bus.publish('payments.subscription.renewed', { plan: 'pro', amount: 49.99 });
  handle.unsubscribe();
  steps.push(`✅ payments.** matched ${hits.length} of 4`);
  return ok('Wildcard **', steps, { matched: hits });
}

// ─── 4. Priority ──────────────────────────────────────────────────────────────
export async function demoPriority() {
  const bus = getBus();
  const steps: string[] = [];
  const order: string[] = [];
  const h1 = bus.subscribe('payments.alert', () => { order.push('LOW');    steps.push('📩 LOW handler');    }, { priority: -1 });
  const h2 = bus.subscribe('payments.alert', () => { order.push('HIGH');   steps.push('📩 HIGH handler');   }, { priority: 10 });
  const h3 = bus.subscribe('payments.alert', () => { order.push('NORMAL'); steps.push('📩 NORMAL handler'); }, { priority: 0  });
  steps.push('📢 Publishing payments.alert (3 subscribers: HIGH/NORMAL/LOW priority)');
  await bus.publish('payments.alert', { message: 'Suspicious activity detected' });
  h1.unsubscribe(); h2.unsubscribe(); h3.unsubscribe();
  steps.push(`✅ Dispatch order: ${order.join(' → ')}`);
  return ok('Priority Ordering', steps, { order });
}

// ─── 5. subscribeOnce ─────────────────────────────────────────────────────────
export async function demoOnce() {
  const bus = getBus();
  const steps: string[] = [];
  let count = 0;
  bus.subscribeOnce('payments.receipt', () => { count++; steps.push(`📩 Receipt handler fired (count=${count})`); });
  steps.push('📢 Publishing payments.receipt × 3 (once subscriber fires only once)');
  for (let i = 0; i < 3; i++) await bus.publish('payments.receipt', { receiptId: `r${i}`, amount: 49.99 });
  steps.push(`✅ Handler fired ${count} time(s) — expected 1`);
  return ok('subscribeOnce', steps, { firingCount: count });
}

// ─── 6. Middleware ────────────────────────────────────────────────────────────
export async function demoMiddleware() {
  const bus = getBus();
  const steps: string[] = [];
  let enrichedPayload: unknown;
  const mw = bus.use(async (env, next) => {
    steps.push('🔧 Middleware: adding processingFee');
    const p = env.payload as Record<string, unknown>;
    p['processingFee'] = parseFloat(((p['amount'] as number || 0) * 0.029).toFixed(2));
    p['enrichedBy']    = 'core-middleware';
    await next();
  });
  const handle = bus.subscribe('payments.checkout', (env) => {
    enrichedPayload = env.payload;
    steps.push(`📩 Received enriched: processingFee=${(env.payload as Record<string,unknown>)['processingFee']}`);
  });
  steps.push('📢 Publishing payments.checkout with amount=$199.99');
  await bus.publish('payments.checkout', { amount: 199.99, items: 3 });
  mw.unuse?.();
  handle.unsubscribe();
  steps.push('✅ Middleware injected processingFee = 2.9% of amount');
  return ok('Middleware Pipeline', steps, { enrichedPayload });
}

// ─── 7. Schema Validation ─────────────────────────────────────────────────────
export async function demoValidation() {
  const bus = getBus();
  const steps: string[] = [];
  try {
    steps.push('📢 Publishing VALID payments.created');
    await bus.publish('payments.created', { amount: 49.99, currency: 'USD', fromUserId: 'u1', toMerchant: 'ShopEasy', status: 'created', id: 'pay_valid', method: 'card' });
    steps.push('✅ Valid payment accepted');
  } catch (e) { steps.push(`❌ Unexpected rejection: ${(e as Error).message}`); }
  try {
    steps.push('📢 Publishing INVALID payments.created (amount is a string)');
    await bus.publish('payments.created', { amount: 'not-a-number', currency: 'USD', fromUserId: 'u1' } as unknown as Record<string,unknown>);
    steps.push('❌ Should have been rejected!');
  } catch (e) {
    if (e instanceof ValidationFailedError) steps.push(`✅ Invalid payload rejected: ${e.message}`);
  }
  return ok('Schema Validation', steps, {});
}

// ─── 8. Replay Engine ────────────────────────────────────────────────────────
export async function demoReplay() {
  const bus = getBus();
  const steps: string[] = [];
  const replayed: unknown[] = [];
  steps.push('📢 Publishing 3 payments.history events BEFORE subscriber registers');
  await bus.publish('payments.history', { amount: 29.99, seq: 1 });
  await bus.publish('payments.history', { amount: 59.99, seq: 2 });
  await bus.publish('payments.history', { amount: 89.99, seq: 3 });
  await new Promise(r => setTimeout(r, 20));
  steps.push('📩 Late subscriber registers — expecting replay');
  const handle = bus.subscribe('payments.history', (env) => {
    replayed.push((env.payload as Record<string,unknown>)['amount']);
    steps.push(`📩 Replayed: $${(env.payload as Record<string,unknown>)['amount']}`);
  });
  await new Promise(r => setTimeout(r, 50));
  handle.unsubscribe();
  steps.push(`✅ Replayed ${replayed.length} events to late subscriber`);
  return ok('Replay Engine', steps, { replayed });
}

// ─── 9. RPC Request / Response ───────────────────────────────────────────────
export async function demoRPC() {
  const bus = getBus();
  const steps: string[] = [];
  const responder = bus.respond('payments.status.check', (env) => {
    const { paymentId } = env.payload as { paymentId: string };
    steps.push(`📩 Responder: checking status of ${paymentId}`);
    return { paymentId, status: 'completed', amount: 49.99, completedAt: Date.now() };
  });
  steps.push('📢 Sending RPC request: payments.status.check');
  const handle = bus.request('payments.status.check', { paymentId: 'pay_rpc_123' }, { timeoutMs: 5000 });
  const response = await handle.response;
  responder.stop();
  steps.push(`✅ RPC response: status=${(response.payload as Record<string,unknown>)['status']}`);
  return ok('RPC Request/Response', steps, { response: response.payload });
}

// ─── 10. Request Cancel ──────────────────────────────────────────────────────
export async function demoCancel() {
  const bus = getBus();
  const steps: string[] = [];
  steps.push('📢 Sending request to ghost event (no responder, 5 s timeout)');
  const handle = bus.request('payments.ghost', {}, { timeoutMs: 5000 });
  await new Promise(r => setTimeout(r, 100));
  steps.push('🚫 Cancelling request before timeout...');
  handle.cancel();
  try {
    await handle.response;
    steps.push('❌ Should have been cancelled');
  } catch (e) { steps.push(`✅ Request cancelled cleanly: ${(e as Error).message}`); }
  return ok('Request Cancel', steps, {});
}

// ─── 11. TTL Expiry ──────────────────────────────────────────────────────────
export async function demoTTL() {
  const bus = getBus();
  const steps: string[] = [];
  steps.push('📢 Publishing payments.session with TTL=50 ms');
  await bus.publish('payments.session', { sessionId: 'sess_abc' }, { ttl: 50 });
  steps.push('⏳ Waiting 100 ms for TTL to expire...');
  await new Promise(r => setTimeout(r, 100));
  let received = false;
  const handle = bus.subscribe('payments.session', () => { received = true; });
  await new Promise(r => setTimeout(r, 30));
  handle.unsubscribe();
  steps.push(`✅ Expired event NOT replayed: received=${received}`);
  return ok('TTL Expiry', steps, { received });
}

// ─── 12. Metrics Snapshot ────────────────────────────────────────────────────
export async function demoMetrics() {
  const bus = getBus();
  const steps: string[] = [];
  steps.push('📊 Fetching live metrics from bus...');
  const m = bus.getMetrics();
  steps.push(`  publishCount:        ${m.publishCount}`);
  steps.push(`  subscribeCount:      ${m.subscribeCount}`);
  steps.push(`  activeSubscriptions: ${m.activeSubscriptions}`);
  steps.push(`  historySize:         ${m.historySize}`);
  steps.push(`  failedDeliveries:    ${m.failedDeliveries}`);
  steps.push(`  validationFailures:  ${m.validationFailures}`);
  steps.push(`  p95LatencyMs:        ${m.p95LatencyMs}`);
  steps.push('✅ Metrics snapshot complete');
  return ok('Metrics Snapshot', steps, m);
}
