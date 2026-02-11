import { afterEach, beforeEach, describe, expect, jest, test } from '@jest/globals';

import { EventBus } from './event-bus.ts';

/**
 * EventBus.
 *
 * @author dafengzhen
 */
describe('EventBus', () => {
  type TestEvents = {
    'exact:event': string;
    'order:placed': { amount: number; id: string };
    'system:alert': { message: string };
    'user:created': { id: string; name: string };
    'user:updated': { id: string; name: string };
  };

  // Allow extra events used in tests (e.g. event1/event2, api:..., etc.)
  type AnyTestEvents = Record<string, any> & TestEvents;

  let bus: EventBus<AnyTestEvents>;

  const userJohn = { id: '1', name: 'John' };
  const userJane = { id: '2', name: 'Jane' };

  beforeEach(() => {
    bus = new EventBus<AnyTestEvents>();
  });

  afterEach(() => {
    bus.destroy();
    jest.restoreAllMocks();
  });

  describe('Basic functionality', () => {
    test('should register and emit exact event listeners', () => {
      const listener = jest.fn();

      bus.on('user:created', listener);
      bus.emit('user:created', userJohn);

      expect(listener).toHaveBeenCalledTimes(1);

      const [payload, ctx] = listener.mock.calls[0];
      expect(payload).toEqual(userJohn);
      expect((ctx as any).event).toBe('user:created');
    });

    test('should remove exact event listeners', () => {
      const listener = jest.fn();

      bus.on('user:created', listener);
      bus.off('user:created', listener);
      bus.emit('user:created', userJohn);

      expect(listener).not.toHaveBeenCalled();
    });

    test('should support once listeners', () => {
      const listener = jest.fn();

      bus.once('user:created', listener);
      bus.emit('user:created', userJohn);
      bus.emit('user:created', userJane);

      expect(listener).toHaveBeenCalledTimes(1);
      expect(listener).toHaveBeenCalledWith(userJohn);
    });

    test('should support async emitting', async () => {
      const syncListener = jest.fn();
      const asyncListener = jest.fn<any>().mockResolvedValue(undefined);

      bus.on('user:created', syncListener);
      bus.on('user:created', asyncListener);

      await bus.emitAsync('user:created', userJohn);

      expect(syncListener).toHaveBeenCalledTimes(1);
      expect(asyncListener).toHaveBeenCalledTimes(1);
    });
  });

  describe('Pattern matching', () => {
    test('should match single-star wildcard', () => {
      const listener = jest.fn();

      bus.on('user:*', listener);
      bus.emit('user:created', userJohn);
      bus.emit('user:updated', { id: '1', name: 'Jane' });
      bus.emit('order:placed', { amount: 100, id: '2' });

      expect(listener).toHaveBeenCalledTimes(2);
    });

    test('should match double-star wildcard', () => {
      const listener = jest.fn();

      bus.on('user:**', listener);
      bus.emit('user:created:admin', userJohn);
      bus.emit('user:updated:profile', { id: '1', name: 'Jane' });
      bus.emit('order:placed', { amount: 100, id: '2' });

      expect(listener).toHaveBeenCalledTimes(2);
    });

    test('should capture parameters', () => {
      const listener = jest.fn();

      bus.on('user:{userId}:action', listener);
      bus.emit('user:123:action', userJohn);

      expect(listener).toHaveBeenCalledTimes(1);
      expect(listener).toHaveBeenCalledWith(
        'user:123:action',
        userJohn,
        { userId: '123' },
        expect.anything(),
      );
    });

    test('should match glob patterns', () => {
      const listener = jest.fn();

      bus.on('user:test[12]', listener);
      bus.emit('user:test1', userJohn);
      bus.emit('user:test2', userJane);
      bus.emit('user:test3', { id: '3', name: 'Bob' });

      expect(listener).toHaveBeenCalledTimes(2);
    });

    test('should support custom separators', () => {
      const listener = jest.fn();

      bus.on('user/{id}/profile', listener, { separator: '/' });
      bus.emit('user/123/profile', userJohn);

      expect(listener).toHaveBeenCalledTimes(1);
      expect(listener).toHaveBeenCalledWith(
        'user/123/profile',
        userJohn,
        { id: '123' },
        expect.anything(),
      );
    });
  });

  describe('Priority', () => {
    test('should order pattern listeners by priority', () => {
      const calls: string[] = [];

      bus.on('user:*', () => calls.push('low'), { priority: 50 });
      bus.on('user:created', () => calls.push('high'), { priority: 100 });
      bus.on('user:{id}', () => calls.push('medium'), { priority: 80 });

      bus.emit('user:created', userJohn);

      expect(calls).toEqual(['high', 'medium', 'low']);
    });

    test('should preserve registration order for same priority', () => {
      const calls: string[] = [];

      bus.on('user:*', () => calls.push('first'));
      bus.on('user:*', () => calls.push('second'));
      bus.on('user:*', () => calls.push('third'));

      bus.emit('user:created', userJohn);

      expect(calls).toEqual(['first', 'second', 'third']);
    });
  });

  describe('Middleware', () => {
    test('should run middleware', async () => {
      const middleware = jest.fn((ctx, next: any) => next());
      const listener = jest.fn();

      bus.use(middleware);
      bus.on('user:created', listener);

      await bus.emitAsync('user:created', userJohn);

      expect(middleware).toHaveBeenCalledTimes(1);
      expect(listener).toHaveBeenCalledTimes(1);
    });

    test('middleware should be able to block events', async () => {
      const middleware = jest.fn((ctx: any) => ctx.block());
      const listener = jest.fn();

      bus.use(middleware);
      bus.on('user:created', listener);

      await bus.emitAsync('user:created', userJohn);

      expect(middleware).toHaveBeenCalledTimes(1);
      expect(listener).not.toHaveBeenCalled();
    });

    test('should support pattern-filtered middleware', async () => {
      const middleware = jest.fn((ctx, next: any) => next());
      const listener = jest.fn();

      bus.use(middleware, { pattern: 'user:*' });
      bus.on('user:created', listener);
      bus.on('order:placed', listener);

      await bus.emitAsync('user:created', userJohn);
      await bus.emitAsync('order:placed', { amount: 100, id: '2' });

      expect(middleware).toHaveBeenCalledTimes(1);
      expect(listener).toHaveBeenCalledTimes(2);
    });

    test('should support middleware with a custom matcher', async () => {
      const middleware = jest.fn((ctx, next: any) => next());
      const customMatcher = jest.fn<any>().mockReturnValue(true);

      bus.use(middleware, { match: customMatcher });
      bus.on('user:created', jest.fn());

      await bus.emitAsync('user:created', userJohn);

      expect(customMatcher).toHaveBeenCalledTimes(1);
      expect(middleware).toHaveBeenCalledTimes(1);
    });
  });

  describe('Sticky events', () => {
    test('should mark an event as sticky', () => {
      const listener = jest.fn();

      bus.emit('user:created', userJohn, { sticky: true });
      bus.on('user:created', listener);

      expect(listener).toHaveBeenCalledTimes(1);
      expect(listener).toHaveBeenCalledWith(userJohn);
    });

    test('sticky events should replay to pattern listeners', () => {
      const listener = jest.fn();

      bus.emit('user:123:action', userJohn, { sticky: true });
      bus.on('user:{id}:action', listener);

      expect(listener).toHaveBeenCalledTimes(1);
      expect(listener).toHaveBeenCalledWith('user:123:action', userJohn, { id: '123' });
    });

    test('should limit the number of sticky events', () => {
      const limitedBus = new EventBus<AnyTestEvents>({ stickyMax: 2 });

      limitedBus.emit('event1', 'data1', { sticky: true });
      limitedBus.emit('event2', 'data2', { sticky: true });
      limitedBus.emit('event3', 'data3', { sticky: true });

      const listener = jest.fn();
      limitedBus.on('event*', listener);

      expect(listener).toHaveBeenCalledTimes(2);
      limitedBus.destroy();
    });
  });

  describe('Scopes', () => {
    test('should create a scope', async () => {
      const baseListener = jest.fn();
      const scopedListener = jest.fn();

      bus.on('user:created', baseListener);

      await bus.withScope(async () => {
        bus.on('user:created', scopedListener);
        bus.emit('user:created', userJohn);
      });

      bus.emit('user:created', userJane);

      expect(baseListener).toHaveBeenCalledTimes(2);
      expect(scopedListener).toHaveBeenCalledTimes(1);
    });

    test('should automatically clean up scoped listeners', () => {
      const listener = jest.fn();
      const scope = bus.createScope();

      bus.runtime.runWithScope(scope, () => {
        bus.on('user:created', listener);
      });

      scope.destroy();
      bus.emit('user:created', userJohn);

      expect(listener).not.toHaveBeenCalled();
    });
  });

  describe('Error handling', () => {
    test('should rethrow listener errors asynchronously', async () => {
      const spy = jest.spyOn(console, 'error').mockImplementation(() => {});

      const error = new Error('Listener error');
      const errorListener = jest.fn(() => {
        throw error;
      });
      const normalListener = jest.fn();

      bus.on('user:created', errorListener);
      bus.on('user:created', normalListener);

      const caught = new Promise<Error>((resolve) => {
        window.addEventListener(
          'error',
          (e: ErrorEvent) => {
            e.preventDefault();
            resolve(e.error);
          },
          { once: true },
        );
      });

      bus.emit('user:created', userJohn);

      expect(errorListener).toHaveBeenCalledTimes(1);
      expect(normalListener).toHaveBeenCalledTimes(1);

      await expect(caught).resolves.toBe(error);

      spy.mockRestore();
    });

    test('middleware errors should propagate', async () => {
      const error = new Error('Middleware error');
      const errorMiddleware = jest.fn<any>().mockRejectedValue(error);
      const listener = jest.fn();

      bus.use(errorMiddleware);
      bus.on('user:created', listener);

      await expect(bus.emitAsync('user:created', userJohn)).rejects.toThrow('Middleware error');
      expect(listener).not.toHaveBeenCalled();
    });
  });

  describe('Cleanup and destruction', () => {
    test('should clear all listeners', () => {
      const listener1 = jest.fn();
      const listener2 = jest.fn();

      bus.on('user:created', listener1);
      bus.on('user:*', listener2);

      bus.clearListeners();
      bus.emit('user:created', userJohn);

      expect(listener1).not.toHaveBeenCalled();
      expect(listener2).not.toHaveBeenCalled();
    });

    test('should reset all state', () => {
      const listener = jest.fn();
      const middleware = jest.fn((ctx, next: any) => next());

      bus.on('user:created', listener);
      bus.use(middleware);

      bus.emit('user:created', userJohn, { sticky: true });
      expect(listener).toHaveBeenCalledTimes(1);
      expect(middleware).toHaveBeenCalledTimes(1);

      bus.reset();

      bus.emit('user:created', userJane);
      expect(listener).toHaveBeenCalledTimes(1); // no additional calls

      bus.emit('user:updated', { id: '3', name: 'Bob' });
      expect(middleware).toHaveBeenCalledTimes(1); // middleware should not run after reset
    });

    test('should throw after being destroyed', () => {
      bus.destroy();

      expect(() => bus.on('user:created', jest.fn())).toThrow(
        'EventBus instance has been destroyed.',
      );
      expect(() => bus.emit('user:created', userJohn)).toThrow(
        'EventBus instance has been destroyed.',
      );
    });
  });

  describe('Performance tests', () => {
    test('should efficiently handle a large number of listeners', () => {
      const listenerCount = 1000;
      const listeners = Array.from({ length: listenerCount }, () => jest.fn());

      listeners.forEach((listener, i) => {
        bus.on(`event:${i}`, listener);
      });

      const start = performance.now();
      listeners.forEach((_, i) => {
        bus.emit(`event:${i}`, `data${i}`);
      });
      const end = performance.now();

      listeners.forEach((listener, i) => {
        expect(listener).toHaveBeenCalledWith(`data${i}`, expect.anything());
      });

      expect(end - start).toBeLessThan(100);
    });

    test('should efficiently match patterns (more stable timing)', () => {
      const listener = jest.fn();

      bus.on('api:v1:users:**', listener);
      bus.on('api:v1:users:{id}:profile', listener);
      bus.on('api:v1:**', listener);
      bus.on('api:**', listener);

      // Warm-up once to reduce jitter in CI/Windows/jsdom
      bus.emit('api:v1:users:123:profile', userJohn);
      listener.mockClear();

      const runs = 50;
      const start = performance.now();
      for (let i = 0; i < runs; i++) {
        bus.emit('api:v1:users:123:profile', userJohn);
      }
      const end = performance.now();

      expect(listener).toHaveBeenCalledTimes(4 * runs);

      // Average per run should be small; this is more stable than single-shot timing.
      const avg = (end - start) / runs;
      expect(avg).toBeLessThan(10);
    });
  });

  describe('Edge cases', () => {
    test('should handle an empty separator', () => {
      const listener = jest.fn();

      bus.on('test*', listener, { separator: '' });
      bus.emit('test123', 'data');

      expect(listener).toHaveBeenCalledTimes(1);
    });

    test('should handle event names containing the separator', () => {
      const listener = jest.fn();

      bus.on('path/to/resource', listener, { separator: '/' });
      bus.emit('path/to/resource', 'data');

      expect(listener).toHaveBeenCalledTimes(1);
    });

    test('should correctly handle sticky events with once + consumeSticky', () => {
      const listener = jest.fn();

      bus.emit('user:created', userJohn, { sticky: true });

      bus.once('user:created', listener, { consumeSticky: true });
      expect(listener).toHaveBeenCalledTimes(1);

      const listener2 = jest.fn();
      bus.once('user:created', listener2, { consumeSticky: true });
      expect(listener2).not.toHaveBeenCalled();
    });

    test('metadata should be passed through context', async () => {
      const middleware = jest.fn((ctx: any, next: any) => {
        ctx.meta.timestamp = Date.now();
        return next();
      });

      const listener = jest.fn((payload, ctx: any) => {
        expect(ctx.meta.timestamp).toBeDefined();
      });

      bus.use(middleware);
      bus.on('user:created', listener);

      await bus.emitAsync('user:created', userJohn, {
        metaPatch: { source: 'test' },
      });

      expect(listener).toHaveBeenCalledTimes(1);
    });
  });
});
