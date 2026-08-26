import { describe, expect, it, vi } from 'vitest';
import { EventBus } from '../src/bus.js';
import { wildcardMatcher } from '../src/wildcard.js';

const tick = () => new Promise(resolve => setTimeout(resolve, 0));

describe('core hardening regressions', () => {
  it('keeps replay disabled unless explicitly configured', async () => {
    const bus = new EventBus();
    await bus.publish('account.updated', { id: 'a1' });
    const callback = vi.fn();
    bus.subscribe('account.updated', callback);
    await tick();
    expect(callback).not.toHaveBeenCalled();
    await bus.destroy();
  });

  it('does not replay RPC requests to a late responder', async () => {
    const bus = new EventBus({ replay: { limit: 10 } });
    const request = bus.request('billing.charge', { amount: 10 }, { timeoutMs: 20 });
    await expect(request.response).rejects.toMatchObject({ code: 'REQUEST_TIMEOUT' });
    const handler = vi.fn(() => ({ ok: true }));
    bus.respond('billing.charge', handler);
    await tick();
    expect(handler).not.toHaveBeenCalled();
    await bus.destroy();
  });

  it('honours zero-segment double-wildcard semantics', () => {
    expect(wildcardMatcher.matches('payments.**', 'payments')).toBe(true);
  });

  it('rejects wildcard subscriptions when wildcard matching is disabled', () => {
    const bus = new EventBus({ enableWildcard: false });
    expect(() => bus.subscribe('account.*', () => undefined)).toThrow(/wildcard/i);
  });

  it('does not remove a subscription through a mismatched event name', () => {
    const bus = new EventBus();
    const handle = bus.subscribe('account.updated', () => undefined);
    expect(bus.unsubscribe('account.deleted', handle.id)).toBe(false);
    expect(bus.getSubscriberCount('account.updated')).toBe(1);
  });

  it('rejects middleware that calls next more than once', async () => {
    const bus = new EventBus();
    bus.use(async (_envelope, next) => { await next(); await next(); });
    await expect(bus.publish('account.updated', {})).rejects.toThrow(/more than once/i);
  });

  it('awaits asynchronous schema validators', async () => {
    const bus = new EventBus();
    bus.registerSchema('account.updated', {
      name: 'async-schema',
      async validate() { return { valid: false, errors: [{ path: '', message: 'denied' }] }; },
    });
    await expect(bus.publish('account.updated', {})).rejects.toMatchObject({ code: 'VALIDATION_FAILED' });
  });

  it('delivers only one historical event to subscribeOnce', async () => {
    const bus = new EventBus({ replay: { limit: 10 } });
    await bus.publish('account.updated', { revision: 1 });
    await bus.publish('account.updated', { revision: 2 });
    const callback = vi.fn();
    bus.subscribeOnce('account.updated', callback);
    await tick();
    await bus.publish('account.updated', { revision: 3 });
    expect(callback).toHaveBeenCalledTimes(1);
    await bus.destroy();
  });
});
