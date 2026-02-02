import { describe, expect, jest, test } from '@jest/globals';

import { EventBus } from './event-bus.ts';
import type { EventMap } from './types.ts';

type Events = EventMap & {
  'order:create': { orderId: number };
  ping: void;
  'user:create': { id: string };
  'user:update': { id: string; name: string };
};

const flush = async (times = 1) => {
  for (let i = 0; i < times; i++) {
    await new Promise<void>((r) => queueMicrotask(r));
  }
};

const setup = () => {
  const bus = new EventBus<Events>();
  return { bus };
};

/**
 * EventBus.
 *
 * @author dafengzhen
 */
describe('EventBus', () => {
  test('on/emit: exact handler should receive payload', async () => {
    const { bus } = setup();
    const handler = jest.fn();

    bus.on('user:create', handler);
    bus.emit('user:create', { id: 'u1' });

    await flush();

    expect(handler).toHaveBeenCalledTimes(1);
    expect(handler).toHaveBeenCalledWith({ id: 'u1' });
  });

  test('emit: void payload event should call handler with undefined', async () => {
    const { bus } = setup();
    const handler = jest.fn();

    bus.on('ping', handler);
    bus.emit('ping');

    await flush();

    expect(handler).toHaveBeenCalledTimes(1);
    expect(handler).toHaveBeenCalledWith(undefined);
  });

  test('off removes handler and listenerCount reflects exact handlers only', async () => {
    const { bus } = setup();
    const h1 = jest.fn();
    const h2 = jest.fn();

    bus.on('user:create', h1);
    bus.on('user:create', h2);
    expect(bus.listenerCount('user:create')).toBe(2);

    bus.off('user:create', h1);
    expect(bus.listenerCount('user:create')).toBe(1);

    bus.emit('user:create', { id: 'u1' });
    await flush();

    expect(h1).not.toHaveBeenCalled();
    expect(h2).toHaveBeenCalledTimes(1);
    expect(h2).toHaveBeenCalledWith({ id: 'u1' });
  });

  test('once: handler should run only once', async () => {
    const { bus } = setup();
    const handler = jest.fn();

    bus.once('user:create', handler);

    bus.emit('user:create', { id: 'u1' });
    bus.emit('user:create', { id: 'u2' });

    await flush();

    expect(handler).toHaveBeenCalledTimes(1);
    expect(handler).toHaveBeenCalledWith({ id: 'u1' });
  });

  test('onAny: should receive (event, payload)', async () => {
    const { bus } = setup();
    const handler = jest.fn();

    bus.onAny(handler);
    bus.emit('order:create', { orderId: 42 });

    await flush();

    expect(handler).toHaveBeenCalledTimes(1);
    expect(handler).toHaveBeenCalledWith('order:create', { orderId: 42 });
  });

  test("onPattern('**'): should receive all events, and unsubscribe works", async () => {
    const { bus } = setup();
    const handler = jest.fn();

    const off = bus.onPattern('**', handler);

    bus.emit('user:create', { id: 'u1' });
    bus.emit('order:create', { orderId: 1 });

    await flush();

    expect(handler).toHaveBeenCalledTimes(2);

    off();

    bus.emit('user:update', { id: 'u1', name: 'n1' });
    await flush();

    expect(handler).toHaveBeenCalledTimes(2);
  });

  test("onPattern('prefix:*'): should receive matching prefix only", async () => {
    const { bus } = setup();
    const handler = jest.fn();

    bus.onPattern('user:*', handler);

    bus.emit('user:create', { id: 'u1' });
    bus.emit('order:create', { orderId: 1 });
    bus.emit('user:update', { id: 'u1', name: 'bob' });

    await flush();

    expect(handler).toHaveBeenCalledTimes(2);
    expect(handler).toHaveBeenNthCalledWith(1, 'user:create', { id: 'u1' }, {});
    expect(handler).toHaveBeenNthCalledWith(2, 'user:update', { id: 'u1', name: 'bob' }, {});
  });

  test('middleware: should run in order and wrap next()', async () => {
    const { bus } = setup();
    const steps: string[] = [];

    bus.use(async (_ctx, next) => {
      steps.push('mw1:before');
      await next();
      steps.push('mw1:after');
    });

    bus.use(async (_ctx, next) => {
      steps.push('mw2:before');
      await next();
      steps.push('mw2:after');
    });

    bus.on('user:create', () => steps.push('handler'));

    bus.emit('user:create', { id: 'u1' });
    await flush(3);

    expect(steps).toEqual(['mw1:before', 'mw2:before', 'handler', 'mw2:after', 'mw1:after']);
  });

  test('middleware: ctx.blocked should prevent handler execution', async () => {
    const { bus } = setup();
    const handler = jest.fn();
    const mw2 = jest.fn();

    bus.use(async (ctx, _next) => {
      ctx.block();
    });

    bus.use(async (_ctx, next) => {
      mw2();
      await next();
    });

    bus.on('user:create', handler);

    bus.emit('user:create', { id: 'u1' });
    await flush(2);

    expect(handler).not.toHaveBeenCalled();
    expect(mw2).not.toHaveBeenCalled();
  });

  test('emitAsync: should await async middlewares before returning', async () => {
    const { bus } = setup();
    const steps: string[] = [];

    bus.use(async (_ctx, next) => {
      steps.push('mw:before');
      await new Promise<void>((r) => setTimeout(r, 5));
      await next();
      steps.push('mw:after');
    });

    bus.on('user:create', () => steps.push('handler'));

    await bus.emitAsync('user:create', { id: 'u1' });

    expect(steps).toEqual(['mw:before', 'handler', 'mw:after']);
  });

  test('emit: may call handlers synchronously when there is no async boundary', () => {
    const { bus } = setup();
    const steps: string[] = [];

    bus.on('user:create', () => steps.push('handler'));

    bus.emit('user:create', { id: 'u1' });

    expect(steps).toEqual(['handler']);
  });

  test('emit: with async middleware, "after" runs later', async () => {
    const { bus } = setup();
    const steps: string[] = [];

    bus.use(async (_ctx, next) => {
      steps.push('mw:before');
      await next();
      steps.push('mw:after');
    });

    bus.on('user:create', () => steps.push('handler'));

    bus.emit('user:create', { id: 'u1' });

    expect(steps).toEqual(['mw:before', 'handler']);

    await flush();

    expect(steps).toEqual(['mw:before', 'handler', 'mw:after']);
  });

  test('clear(): should clear everything; clear(event) should clear exact handlers only', async () => {
    const { bus } = setup();
    const exact = jest.fn();
    const any = jest.fn();
    const star = jest.fn();
    const prefix = jest.fn();

    bus.on('user:create', exact);
    bus.onAny(any);
    bus.onPattern('**', star);
    bus.onPattern('user:*', prefix);

    bus.clear('user:create');
    bus.emit('user:create', { id: 'u1' });

    await flush();

    expect(exact).not.toHaveBeenCalled();
    expect(any).toHaveBeenCalledTimes(1);
    expect(star).toHaveBeenCalledTimes(1);
    expect(prefix).toHaveBeenCalledTimes(1);

    bus.clear();
    bus.emit('user:update', { id: 'u1', name: 'n1' });
    await flush();

    expect(any).toHaveBeenCalledTimes(1);
    expect(star).toHaveBeenCalledTimes(1);
    expect(prefix).toHaveBeenCalledTimes(1);
  });
});
