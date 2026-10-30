import { compile } from '@dafengzhen/derivative-matcher';
import { afterEach, describe, expect, expectTypeOf, it, vi } from 'vitest';

import type { EventContext, EventScope } from './index.js';

import { createEventBus, EventBus } from './index.js';

const token = Symbol('token');
interface Events {
  7: boolean;
  count: number;
  optional: number | undefined;
  ready: void;
  [token]: Date;
  'user:login': { id: string };
  'user:logout': string;
}
const makeBus = () => createEventBus<Events>();
afterEach(() => vi.useRealTimers());

describe('subscriptions and event contexts', () => {
  it('supports interface maps, omitted void payloads, number and symbol keys', () => {
    const bus = makeBus();
    const seen: unknown[] = [];
    bus.onAny((event) => {
      seen.push(event.type, event.payload);
    });
    bus.emit('ready');
    bus.emit('optional');
    bus.emit(7, true);
    const date = new Date();
    bus.emit(token, date);
    expect(seen).toEqual(['ready', undefined, 'optional', undefined, 7, true, token, date]);
  });

  it('provides frozen context and metadata copies with increasing IDs', () => {
    const bus = makeBus();
    const meta = { source: 'test' };
    const first = bus.emit('count', 1, { meta });
    meta.source = 'changed';
    expect(first.event.meta).toEqual({ source: 'test' });
    expect(first.event.replayed).toBe(false);
    expect(first.event.timestamp).toBeGreaterThan(0);
    expect(Object.isFrozen(first.event)).toBe(true);
    expect(Object.isFrozen(first.event.meta)).toBe(true);
    expect(Object.isFrozen(first)).toBe(true);
    expect(Object.isFrozen(first.errors)).toBe(true);
    expect(bus.emit('count', 2).event.id).toBe(first.event.id + 1);
  });

  it('orders all listener kinds together by priority and registration order', () => {
    const bus = makeBus();
    const order: string[] = [];
    bus.onAny(() => {
      order.push('any');
    });
    bus.on('count', () => {
      order.push('exact');
    });
    bus.onPattern('count', () => {
      order.push('pattern');
    });
    bus.on(
      'count',
      () => {
        order.push('first');
      },
      { priority: 5 },
    );
    bus.on(
      'count',
      () => {
        order.push('last');
      },
      { priority: -1 },
    );
    expect(bus.emit('count', 1).delivered).toBe(5);
    expect(order).toEqual(['first', 'any', 'exact', 'pattern', 'last']);
  });

  it('allows duplicate registrations and idempotent independent unsubscribe', () => {
    const bus = makeBus();
    const listener = vi.fn<(...args: any[]) => void>();
    const off = bus.on('count', listener);
    bus.on('count', listener);
    off();
    off();
    expect(bus.listenerCount()).toBe(1);
    bus.emit('count', 1);
    expect(listener).toHaveBeenCalledTimes(1);
  });

  it('snapshots registrations but skips listeners removed during delivery', () => {
    const bus = makeBus();
    const seen: string[] = [];
    bus.once('count', () => {
      off();
      bus.on('count', () => {
        seen.push('new');
      });
    });
    const off = bus.on('count', () => {
      seen.push('removed');
    });
    bus.emit('count', 1);
    expect(seen).toEqual([]);
    bus.emit('count', 2);
    expect(seen).toEqual(['new']);
  });

  it('removes once before invocation, including reentrant emits and failures', () => {
    const bus = makeBus();
    const listener = vi.fn<() => void>(() => {
      bus.emit('count', 2);
      throw new Error('failure');
    });
    bus.once('count', listener);
    expect(() => bus.emit('count', 1)).toThrow(AggregateError);
    expect(listener).toHaveBeenCalledTimes(1);
    expect(bus.listenerCount()).toBe(0);
  });

  it('stops propagation from a listener with reactive state', () => {
    const bus = makeBus();
    bus.on('count', (_, event) => event.stopPropagation());
    const listener = vi.fn<(...args: any[]) => void>();
    bus.onAny(listener);
    const result = bus.emit('count', 1);
    expect(result.event.propagationStopped).toBe(true);
    expect(result.delivered).toBe(1);
    expect(listener).not.toHaveBeenCalled();
  });

  it('handles already-aborted signals and abort during a dispatch', () => {
    const bus = makeBus();
    const controller = new AbortController();
    bus.onAny(() => controller.abort());
    const listener = vi.fn<(...args: any[]) => void>();
    bus.on('count', listener, { signal: controller.signal });
    bus.emit('count', 1);
    bus.on('count', listener, { replay: true, signal: controller.signal });
    expect(listener).not.toHaveBeenCalled();
    expect(bus.listenerCount('count')).toBe(1);
  });

  it.each([NaN, Infinity, -Infinity])(
    'rejects invalid priority %s without leaking a bucket',
    (priority) => {
      const bus = makeBus();
      expect(() => bus.on('count', () => {}, { priority })).toThrow(RangeError);
      expect(bus.listenerCount()).toBe(0);
    },
  );
});

describe('pattern matching', () => {
  it('uses full-string DSL with boolean operations', () => {
    const bus = makeBus();
    const listener = vi.fn<(...args: any[]) => void>();
    bus.onPattern('user:.* & !(user:logout)', listener);
    bus.emit('user:login', { id: 'a' });
    bus.emit('user:logout', 'a');
    expect(listener).toHaveBeenCalledTimes(1);
    expect(listener.mock.calls[0]?.[0].type).toBe('user:login');
    const exact = vi.fn<(...args: any[]) => void>();
    bus.onPattern('user:', exact);
    bus.emit('user:login', { id: 'b' });
    expect(exact).not.toHaveBeenCalled();
  });

  it('accepts reusable compiled matchers', () => {
    const bus = makeBus();
    const listener = vi.fn<(...args: any[]) => void>();
    bus.onPattern(compile('count'), listener, { once: true });
    bus.emit('count', 1);
    bus.emit('count', 2);
    expect(listener).toHaveBeenCalledTimes(1);
  });

  it.each([/user:/g, /user:/y])('isolates regex state for %s', (pattern) => {
    const bus = makeBus();
    pattern.lastIndex = 4;
    const listener = vi.fn<(...args: any[]) => void>();
    bus.onPattern(pattern, listener);
    bus.emit('user:login', { id: 'a' });
    bus.emit('user:login', { id: 'b' });
    expect(listener).toHaveBeenCalledTimes(2);
    expect(pattern.lastIndex).toBe(4);
  });

  it('matches only string keys; onAny includes number and symbol keys', () => {
    const bus = makeBus();
    const listener = vi.fn<(...args: any[]) => void>();
    bus.onPattern('.*', listener);
    bus.emit(7, true);
    bus.emit(token, new Date());
    expect(listener).not.toHaveBeenCalled();
    expect(bus.listenerCount(7)).toBe(0);
    expect(bus.listenerCount('ready')).toBe(1);
  });

  it('propagates invalid pattern errors without registering a listener', () => {
    const bus = makeBus();
    expect(() => bus.onPattern('[', () => {})).toThrow(Error);
    expect(() => bus.use(() => {}, { pattern: '[' })).toThrow(Error);
    expect(bus.listenerCount()).toBe(0);
  });
});

describe('middleware', () => {
  it('runs sync middleware as an onion with optional pattern filtering', () => {
    const bus = makeBus();
    const seen: string[] = [];
    bus.use((_, next) => {
      seen.push('a');
      next();
      seen.push('d');
    });
    bus.use(
      (_, next) => {
        seen.push('b');
        next();
        seen.push('c');
      },
      { pattern: 'count' },
    );
    bus.on('count', () => {
      seen.push('listener');
    });
    bus.emit('count', 1);
    expect(seen).toEqual(['a', 'b', 'listener', 'c', 'd']);
    seen.length = 0;
    bus.emit('ready');
    expect(seen).toEqual(['a', 'd']);
  });

  it.each(['omit-next', 'stop'] as const)('cancels delivery and retention with %s', (mode) => {
    const bus = makeBus();
    bus.use((event, next) => {
      if (mode === 'stop') {
        event.stopPropagation();
        next();
      }
    });
    expect(bus.emit('count', 1, { retain: true }).delivered).toBe(0);
    const listener = vi.fn<(...args: any[]) => void>();
    bus.on('count', listener, { replay: true });
    expect(listener).not.toHaveBeenCalled();
  });

  it('rejects double and late next() calls', () => {
    const bus = makeBus();
    bus.use((_, next) => {
      next();
      next();
    });
    expect(() => bus.emit('ready')).toThrow(AggregateError);
    bus.clear();
    let later: () => void = () => {};
    bus.use((_, next) => {
      later = next;
    });
    bus.emit('ready');
    expect(later).toThrow('next() must be called once');
  });

  it('skips removed middleware and snapshots additions', () => {
    const bus = makeBus();
    const removed = vi.fn<(...args: any[]) => void>();
    const added = vi.fn<(event: EventContext<Events>, next: () => void) => void>(
      (_, next: () => void) => next(),
    );
    bus.use((_, next) => {
      off();
      bus.use(added);
      next();
    });
    const off = bus.use(removed);
    bus.emit('ready');
    expect(removed).not.toHaveBeenCalled();
    expect(added).not.toHaveBeenCalled();
    bus.emit('ready');
    expect(added).toHaveBeenCalledTimes(1);
  });

  it('supports abort signals on middleware', () => {
    const bus = makeBus();
    const controller = new AbortController();
    controller.abort();
    bus.use(
      () => {
        throw new Error('unexpected');
      },
      { signal: controller.signal },
    );
    expect(bus.emit('ready').errors).toEqual([]);
  });

  it('preflights matching async middleware before running synchronous side effects', () => {
    const bus = makeBus();
    const sync = vi.fn<(event: EventContext<Events>, next: () => void) => void>(
      (_, next: () => void) => next(),
    );
    bus.use(sync);
    bus.useAsync(
      async (_, next) => {
        await next();
      },
      { pattern: 'count' },
    );
    expect(() => bus.emit('count', 1)).toThrow('use emitAsync()');
    expect(sync).not.toHaveBeenCalled();
    bus.emit('ready');
    expect(sync).toHaveBeenCalledTimes(1);
  });
});

describe('asynchronous dispatch and failures', () => {
  it('awaits listeners sequentially and composes sync and async middleware', async () => {
    const bus = makeBus();
    const seen: string[] = [];
    bus.useAsync(async (_, next) => {
      seen.push('before');
      await next();
      seen.push('after');
    });
    bus.use((_, next) => {
      seen.push('sync');
      next();
    });
    bus.on('ready', async () => {
      await Promise.resolve();
      seen.push('one');
    });
    bus.onAny(() => {
      seen.push('two');
    });
    const result = await bus.emitAsync('ready');
    expect(seen).toEqual(['before', 'sync', 'one', 'two', 'after']);
    expect(result.delivered).toBe(2);
  });

  it('drains an unawaited next()', async () => {
    const bus = makeBus();
    let done = false;
    bus.useAsync(async (_, next) => {
      void next();
    });
    bus.on('ready', async () => {
      await Promise.resolve();
      done = true;
    });
    await bus.emitAsync('ready');
    expect(done).toBe(true);
  });

  it('reports double/late async next calls and middleware rejections', async () => {
    const bus = makeBus();
    bus.useAsync(async (_, next) => {
      await next();
      await next();
    });
    await expect(bus.emitAsync('ready')).rejects.toBeInstanceOf(AggregateError);
    bus.clear();
    let later: () => Promise<void> = async () => {};
    bus.useAsync(async (_, next) => {
      later = next;
    });
    await bus.emitAsync('ready');
    expect(later).toThrow('next() must be called once');
    bus.clear();
    bus.useAsync(async () => {
      throw new Error('middleware');
    });
    await expect(bus.emitAsync('ready')).rejects.toMatchObject({
      errors: [new Error('middleware')],
    });
  });

  it('continues delivery and aggregates listener failures', async () => {
    const bus = makeBus();
    const seen = vi.fn<(...args: any[]) => void>();
    bus.on('ready', () => {
      throw new Error('one');
    });
    bus.on('ready', async () => {
      throw new Error('two');
    });
    bus.on('ready', seen);
    await expect(bus.emitAsync('ready')).rejects.toMatchObject({
      errors: [new Error('one'), new Error('two')],
    });
    expect(seen).toHaveBeenCalledTimes(1);
  });

  it('returns structured errors when onError handles them', () => {
    const onError = vi.fn<(...args: any[]) => void>();
    const bus = createEventBus<Events>({ onError });
    bus.on('ready', () => {
      throw new Error('listener');
    });
    const result = bus.emit('ready');
    expect(result.errors).toHaveLength(1);
    expect(result.errors[0]).toMatchObject({ event: { type: 'ready' }, phase: 'listener' });
    expect(onError).toHaveBeenCalledWith(result.errors[0]);
    expect(result.delivered).toBe(1);
  });

  it('propagates onError failures without recursive reporting', () => {
    const bus = createEventBus<Events>({
      onError: () => {
        throw new Error('reporter');
      },
    });
    bus.on('ready', () => {
      throw new Error('original');
    });
    expect(() => bus.emit('ready')).toThrow(AggregateError);
  });

  it('rejects async listeners and accidental async sync-middleware in emit()', async () => {
    const bus = makeBus();
    bus.on('ready', async () => {
      throw new Error('observed');
    });
    expect(() => bus.emit('ready')).toThrow(AggregateError);
    bus.clear();
    bus.use(async () => {
      throw new Error('observed');
    });
    expect(() => bus.emit('ready')).toThrow(AggregateError);
    await Promise.resolve();
  });

  it('reserves once subscriptions across concurrent dispatches', async () => {
    const bus = makeBus();
    const listener = vi.fn<() => Promise<void>>(async () => {
      await Promise.resolve();
    });
    bus.once('ready', listener);
    await Promise.all([bus.emitAsync('ready'), bus.emitAsync('ready')]);
    expect(listener).toHaveBeenCalledTimes(1);
  });

  it('stops async listeners after stopPropagation or disposal', async () => {
    const bus = makeBus();
    const listener = vi.fn<(...args: any[]) => void>();
    bus.on('ready', async (_, event) => {
      await Promise.resolve();
      event.stopPropagation();
    });
    bus.onAny(listener);
    expect((await bus.emitAsync('ready')).delivered).toBe(1);
    expect(listener).not.toHaveBeenCalled();
    bus.clear();
    bus.useAsync(async (_, next) => {
      bus.dispose();
      await next();
    });
    expect((await bus.emitAsync('ready')).delivered).toBe(0);
  });
});

describe('retention and replay', () => {
  it('retains only the latest payload and replays only when opted in', () => {
    const bus = makeBus();
    const first = bus.emit('count', 1, { meta: { request: 'a' }, retain: true });
    const second = bus.emit('count', 2, { meta: { request: 'b' }, retain: true });
    const listener = vi.fn<(...args: any[]) => void>();
    bus.on('count', listener);
    expect(listener).not.toHaveBeenCalled();
    bus.on('count', listener, { replay: true });
    expect(listener).toHaveBeenCalledExactlyOnceWith(
      2,
      expect.objectContaining({
        id: second.event.id,
        meta: { request: 'b' },
        replayed: true,
        timestamp: second.event.timestamp,
      }),
    );
    expect(first.event.replayed).toBe(false);
    expect(bus.listenerCount('count')).toBe(2);
  });

  it('replays patterns chronologically by last update and once replays only one', () => {
    const bus = makeBus();
    bus.emit('count', 1, { retain: true });
    bus.emit('ready', undefined, { retain: true });
    bus.emit('count', 2, { retain: true });
    const seen: string[] = [];
    bus.onPattern(
      '.*',
      (event) => {
        seen.push(String(event.type));
      },
      { replay: true },
    );
    expect(seen).toEqual(['ready', 'count']);
    const once = vi.fn<(...args: any[]) => void>();
    bus.onAny(once, { once: true, replay: true });
    expect(once).toHaveBeenCalledTimes(1);
    expect(bus.listenerCount()).toBe(1);
  });

  it('evicts oldest updated keys and supports disabling retention', () => {
    const bus = createEventBus<Events>({ retainedLimit: 1 });
    bus.emit('count', 1, { retain: true });
    bus.emit('ready', undefined, { retain: true });
    const seen = vi.fn<(...args: any[]) => void>();
    bus.onAny(seen, { replay: true });
    expect(seen.mock.calls.map(([event]) => event.type)).toEqual(['ready']);
    const disabled = createEventBus<Events>({ retainedLimit: 0 });
    disabled.emit('count', 1, { retain: true });
    disabled.onAny(seen, { replay: true });
    expect(seen).toHaveBeenCalledTimes(1);
  });

  it('does not rerun middleware or other listeners during replay', () => {
    const bus = makeBus();
    const middleware = vi.fn<(event: EventContext<Events>, next: () => void) => void>(
      (_, next: () => void) => next(),
    );
    const listener = vi.fn<(...args: any[]) => void>();
    bus.use(middleware);
    bus.onAny(listener);
    bus.emit('ready', undefined, { retain: true });
    bus.once('ready', () => {}, { replay: true });
    expect(middleware).toHaveBeenCalledTimes(1);
    expect(listener).toHaveBeenCalledTimes(1);
  });

  it('rolls back failed replay registrations, including async replay', () => {
    const bus = makeBus();
    bus.emit('ready', undefined, { retain: true });
    expect(() =>
      bus.on(
        'ready',
        () => {
          throw new Error('replay');
        },
        { replay: true },
      ),
    ).toThrow(AggregateError);
    expect(() => bus.onAny(async () => {}, { replay: true })).toThrow(AggregateError);
    expect(bus.listenerCount()).toBe(0);
  });

  it('reports replay phase and allows clearing one key or all keys', () => {
    const onError = vi.fn<(...args: any[]) => void>();
    const bus = createEventBus<Events>({ onError });
    bus.emit('count', 1, { retain: true });
    bus.onAny(
      () => {
        throw new Error('replay');
      },
      { replay: true },
    );
    expect(onError.mock.calls[0]?.[0].phase).toBe('replay');
    bus.clearRetained('count');
    const listener = vi.fn<(...args: any[]) => void>();
    bus.onAny(listener, { replay: true });
    expect(listener).not.toHaveBeenCalled();
    bus.clearRetained();
  });

  it('isolates propagation state between replay subscribers', () => {
    const bus = makeBus();
    bus.emit('ready', undefined, { retain: true });
    bus.once('ready', (_, event) => event.stopPropagation(), { replay: true });
    const seen = vi.fn<(...args: any[]) => void>();
    bus.once('ready', seen, { replay: true });
    expect(seen.mock.calls[0]?.[1].propagationStopped).toBe(false);
  });

  it.each([-1, NaN, Infinity, 1.5])('rejects invalid retained limit %s', (retainedLimit) => {
    expect(() => new EventBus({ retainedLimit })).toThrow(RangeError);
  });
});

describe('waiting and lifecycle', () => {
  it('waits for a matching payload and automatically unsubscribes', async () => {
    const bus = makeBus();
    const pending = bus.waitFor('count', { filter: (value) => value > 1 });
    bus.emit('count', 1);
    expect(bus.listenerCount()).toBe(1);
    bus.emit('count', 2);
    await expect(pending).resolves.toMatchObject({ payload: 2, type: 'count' });
    expect(bus.listenerCount()).toBe(0);
  });

  it('can resolve synchronously replayed values without leaking subscriptions', async () => {
    const bus = makeBus();
    bus.emit('count', 3, { retain: true });
    await expect(bus.waitFor('count', { replay: true })).resolves.toMatchObject({
      payload: 3,
      replayed: true,
    });
    expect(bus.listenerCount()).toBe(0);
  });

  it('cleans up timeout timers after resolution', async () => {
    vi.useFakeTimers();
    const bus = makeBus();
    const pending = bus.waitFor('ready', { timeout: 100 });
    bus.emit('ready');
    await pending;
    expect(vi.getTimerCount()).toBe(0);
  });

  it('rejects timeouts and removes subscriptions', async () => {
    vi.useFakeTimers();
    const bus = makeBus();
    const pending = bus.waitFor('ready', { timeout: 0 }).catch((error: unknown) => error);
    await vi.runAllTimersAsync();
    expect(await pending).toMatchObject({ name: 'TimeoutError' });
    expect(bus.listenerCount()).toBe(0);
  });

  it.each([-1, Infinity, NaN, 2_147_483_648])('rejects invalid timeout %s', async (timeout) => {
    const bus = makeBus();
    await expect(bus.waitFor('ready', { timeout })).rejects.toBeInstanceOf(RangeError);
    expect(bus.listenerCount()).toBe(0);
  });

  it('rejects aborted waits with the caller reason, including already aborted signals', async () => {
    const bus = makeBus();
    const controller = new AbortController();
    const first = bus.waitFor('ready', { signal: controller.signal });
    controller.abort('cancelled');
    await expect(first).rejects.toBe('cancelled');
    await expect(bus.waitFor('ready', { signal: controller.signal })).rejects.toBe('cancelled');
    expect(bus.listenerCount()).toBe(0);
  });

  it('rejects filter errors without making emit fail', async () => {
    const bus = makeBus();
    const pending = bus.waitFor('count', {
      filter: () => {
        throw new Error('filter');
      },
    });
    bus.emit('count', 1);
    await expect(pending).rejects.toThrow('filter');
    expect(bus.listenerCount()).toBe(0);
  });

  it('clears subscriptions and pending waits, leaving the bus reusable', async () => {
    const bus = makeBus();
    const pending = bus.waitFor('ready');
    bus.use((_, next) => next());
    bus.onAny(() => {});
    bus.onPattern('.*', () => {});
    bus.clear();
    await expect(pending).rejects.toMatchObject({ name: 'AbortError' });
    expect(bus.listenerCount()).toBe(0);
    expect(bus.emit('ready').delivered).toBe(0);
  });

  it('disposes permanently, stops an active dispatch, and rejects waits', async () => {
    const bus = makeBus();
    const pending = bus.waitFor('ready');
    bus.on('count', () => bus.dispose());
    const listener = vi.fn<(...args: any[]) => void>();
    bus.onAny(listener);
    bus.emit('count', 1);
    bus.dispose();
    expect(bus.disposed).toBe(true);
    expect(listener).not.toHaveBeenCalled();
    await expect(pending).rejects.toMatchObject({ name: 'AbortError' });
    expect(() => bus.on('ready', () => {})).toThrow('disposed');
    expect(() => bus.scope()).toThrow('disposed');
    expect(() => bus.emit('ready')).toThrow('disposed');
    await expect(bus.emitAsync('ready')).rejects.toThrow('disposed');
    await expect(bus.waitFor('ready')).rejects.toThrow('disposed');
  });
});

describe('scopes', () => {
  it('owns all registration kinds and middleware, with independent child disposal', async () => {
    const bus = makeBus();
    const parent = bus.scope();
    const child = parent.scope();
    const listener = vi.fn<(...args: any[]) => void>();
    parent.on('ready', listener);
    child.once('ready', listener);
    child.onAny(listener);
    child.onPattern('.*', listener);
    child.use(() => {
      throw new Error('disposed');
    });
    child.useAsync(async () => {
      throw new Error('disposed');
    });
    const pending = child.waitFor('count');
    child.dispose();
    child.dispose();
    await expect(pending).rejects.toMatchObject({ name: 'AbortError' });
    bus.emit('ready');
    expect(listener).toHaveBeenCalledTimes(1);
    expect(parent.disposed).toBe(false);
    expect(() => child.on('ready', listener)).toThrow('disposed');
    await expect(child.waitFor('ready')).rejects.toMatchObject({ name: 'AbortError' });
    parent.dispose();
    expect(bus.listenerCount()).toBe(0);
  });

  it('disposes descendants from both parent and bus', () => {
    const bus = makeBus();
    const parent = bus.scope();
    const child = parent.scope();
    child.on('ready', () => {});
    parent.dispose();
    expect(child.disposed).toBe(true);
    const other = bus.scope();
    bus.dispose();
    expect(other.disposed).toBe(true);
  });

  it('combines scope and caller cancellation without affecting siblings', () => {
    const bus = makeBus();
    const scope = bus.scope();
    const controller = new AbortController();
    scope.on('ready', () => {}, { signal: controller.signal });
    scope.on('ready', () => {});
    controller.abort();
    expect(bus.listenerCount()).toBe(1);
    scope.dispose();
    expect(bus.listenerCount()).toBe(0);
  });

  it('handles disposal during synchronous replay', () => {
    const bus = makeBus();
    bus.emit('count', 1, { retain: true });
    bus.emit('ready', undefined, { retain: true });
    const scope = bus.scope();
    const listener = vi.fn<() => void>(() => scope.dispose());
    scope.onAny(listener, { replay: true });
    expect(listener).toHaveBeenCalledTimes(1);
    expect(bus.listenerCount()).toBe(0);
  });

  it('has explicit ownership across awaits', async () => {
    const bus = makeBus();
    const first = bus.scope();
    const second = bus.scope();
    await Promise.resolve();
    first.on('ready', () => {});
    second.on('ready', () => {});
    first.dispose();
    expect(bus.listenerCount()).toBe(1);
    second.dispose();
  });
});

it('preserves inference and rejects invalid public API calls at compile time', () => {
  // Kept uncalled: tsc checks both accepted calls and @ts-expect-error assertions.
  function typeAssertions(
    bus: EventBus<Events>,
    scope: EventScope<Events>,
    key: 'count' | 'user:logout',
  ) {
    bus.on('count', (payload, event) => {
      expectTypeOf(payload).toEqualTypeOf<number>();
      expectTypeOf(event.type).toEqualTypeOf<'count'>();
    });
    bus.onAny((event) => {
      if (event.type === 'user:login') {
        expectTypeOf(event.payload).toEqualTypeOf<{ id: string }>();
      }
      if (event.type === token) {
        expectTypeOf(event.payload).toEqualTypeOf<Date>();
      }
    });
    bus.onPattern('.*', (event) => {
      if (event.type === 'count') {
        expectTypeOf(event.payload).toEqualTypeOf<number>();
      }
    });
    expectTypeOf(bus.waitFor('count')).toEqualTypeOf<Promise<EventContext<Events, 'count'>>>();
    expectTypeOf(scope.waitFor('ready')).toEqualTypeOf<Promise<EventContext<Events, 'ready'>>>();
    bus.emit('ready');
    bus.emit('optional');
    // @ts-expect-error unknown event
    bus.emit('missing');
    // @ts-expect-error missing required payload
    bus.emit('count');
    // @ts-expect-error incorrect payload
    bus.emit('count', 'wrong');
    // @ts-expect-error correlated tuple forbids union key with unmatched payload
    bus.emit(key, 1);
    // @ts-expect-error async emit has the same payload constraints
    void bus.emitAsync('user:login', { id: 123 });
    // @ts-expect-error listener cannot change key inference
    bus.on('count', (value: string) => {
      console.log(value);
    });
    // @ts-expect-error filter must accept the actual payload
    bus.waitFor('count', { filter: (value: string) => value.length > 1 });
    // @ts-expect-error scoped subscriptions retain payload constraints
    scope.on('count', (value: string) => {
      console.log(value);
    });
  }
  expect(typeAssertions).toBeTypeOf('function');
});

it('clear stops a suspended dispatch from retaining old data after reuse', async () => {
  const bus = makeBus();
  let resume: () => void = () => {};
  const gate = new Promise<void>((resolve) => {
    resume = resolve;
  });
  bus.useAsync(async (_, next) => {
    await gate;
    await next();
  });
  const pending = bus.emitAsync('count', 1, { retain: true });
  bus.clear();
  const seen: number[] = [];
  bus.on('count', (value) => {
    seen.push(value);
  });
  bus.emit('count', 2, { retain: true });
  resume();
  expect((await pending).delivered).toBe(0);
  bus.on(
    'count',
    (value) => {
      seen.push(value);
    },
    { replay: true },
  );
  expect(seen).toEqual([2, 2]);
});

it('async dispatch skips middleware and listeners removed before their turn', async () => {
  const bus = makeBus();
  bus.useAsync(async (_, next) => {
    offMiddleware();
    await next();
  });
  const offMiddleware = bus.use(() => {
    throw new Error('removed');
  });
  bus.on('ready', async () => {
    await Promise.resolve();
    offListener();
  });
  const offListener = bus.on('ready', () => {
    throw new Error('removed');
  });
  expect((await bus.emitAsync('ready')).delivered).toBe(1);
});

it('rejects an async onError callback without leaving an unhandled rejection', async () => {
  const bus = createEventBus<Events>({
    onError: async () => {
      throw new Error('reporter');
    },
  });
  bus.on('ready', () => {
    throw new Error('original');
  });
  expect(() => bus.emit('ready')).toThrow(
    expect.objectContaining({ errors: [new TypeError('onError must be synchronous.')] }),
  );
  await Promise.resolve();
});

it('snapshots retained values during recursive replay without unbounded iteration', () => {
  const bus = makeBus();
  bus.emit('count', 1, { retain: true });
  const seen: number[] = [];
  bus.on(
    'count',
    (value, event) => {
      seen.push(value);
      if (event.replayed) {
        bus.emit('count', 2, { retain: true });
      }
    },
    { replay: true },
  );
  expect(seen).toEqual([1, 2]);
});
