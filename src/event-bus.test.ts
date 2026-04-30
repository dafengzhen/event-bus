import { afterEach, beforeEach, describe, expect, jest, test } from '@jest/globals';

import { DispatcherRuntime } from './dispatcher-runtime.ts';
import { EventBus } from './event-bus.ts';
import { EventScope } from './event-scope.ts';

/**
 * EventBus.
 *
 * @author dafengzhen
 */
describe('EventBus', () => {
  /**
   * Type definitions for all events used across the test suite.
   * Provides a centralized map of event names to their expected payload types.
   */
  type TestEvents = {
    'custom:event': { value: number };
    'data:sync': { items: string[] };
    'error:occurred': { code: number; message: string };
    'user:login': { timestamp: number; userId: string };
    'user:logout': { userId: string };
  };

  /** The EventBus instance under test */
  let bus: EventBus<TestEvents>;

  /**
   * Creates a fresh EventBus instance before each test case.
   * Ensures complete isolation between individual tests.
   */
  beforeEach(() => {
    bus = new EventBus<TestEvents>();
  });

  /**
   * Destroys the EventBus instance after each test case.
   * Prevents cross-test contamination from leaked listeners or sticky events.
   */
  afterEach(() => {
    bus.destroy();
  });

  describe('on and emit - Basic Functionality', () => {
    /**
     * Verifies that a listener registered via `on` is invoked
     * exactly once when the corresponding event is emitted,
     * and receives both the payload and a context object.
     */
    test('should register a listener and invoke it when the event is emitted', () => {
      const listener = jest.fn();
      bus.on('user:login', listener);

      bus.emit('user:login', { timestamp: Date.now(), userId: '123' });

      expect(listener).toHaveBeenCalledTimes(1);
      expect(listener).toHaveBeenCalledWith(
        expect.objectContaining({ userId: '123' }),
        expect.objectContaining({ event: 'user:login' }),
      );
    });

    /**
     * Verifies that the return value of `on` is an unsubscribe function,
     * and that calling it prevents the listener from being invoked.
     */
    test('should remove a listener via the returned off function', () => {
      const listener = jest.fn();
      const off = bus.on('user:login', listener);
      off();

      bus.emit('user:login', { timestamp: Date.now(), userId: '123' });
      expect(listener).not.toHaveBeenCalled();
    });

    /**
     * Verifies that calling the unsubscribe function multiple times is safe
     * and does not throw errors or cause side effects.
     */
    test('should be idempotent when off is called multiple times', () => {
      const listener = jest.fn();
      const off = bus.on('user:login', listener);
      off();
      off();
      off();

      bus.emit('user:login', { timestamp: Date.now(), userId: '123' });
      expect(listener).not.toHaveBeenCalled();
    });

    /**
     * Verifies that different events can have independent listeners
     * and that emitting one event does not trigger listeners for another.
     */
    test('should support multiple listeners for different events', () => {
      const loginListener = jest.fn();
      const logoutListener = jest.fn();

      bus.on('user:login', loginListener);
      bus.on('user:logout', logoutListener);

      bus.emit('user:login', { timestamp: Date.now(), userId: '123' });
      bus.emit('user:logout', { userId: '123' });

      expect(loginListener).toHaveBeenCalledTimes(1);
      expect(logoutListener).toHaveBeenCalledTimes(1);
    });

    /**
     * Verifies that a single event can have multiple listeners registered,
     * and that all of them are invoked in the expected order.
     */
    test('should support multiple listeners for the same event', () => {
      const listener1 = jest.fn();
      const listener2 = jest.fn();
      const listener3 = jest.fn();

      bus.on('user:login', listener1);
      bus.on('user:login', listener2);
      bus.on('user:login', listener3);

      bus.emit('user:login', { timestamp: Date.now(), userId: '123' });

      expect(listener1).toHaveBeenCalledTimes(1);
      expect(listener2).toHaveBeenCalledTimes(1);
      expect(listener3).toHaveBeenCalledTimes(1);
    });
  });

  describe('once - One-shot Listeners', () => {
    /**
     * Verifies that a listener registered via `once` is invoked only once,
     * even when the event is emitted multiple times.
     */
    test('should invoke a once listener only on the first emission', () => {
      const listener = jest.fn();
      bus.once('user:login', listener);

      bus.emit('user:login', { timestamp: Date.now(), userId: '123' });
      bus.emit('user:login', { timestamp: Date.now(), userId: '456' });

      expect(listener).toHaveBeenCalledTimes(1);
      expect(listener).toHaveBeenCalledWith(
        expect.objectContaining({ userId: '123' }),
        expect.objectContaining({ event: 'user:login' }),
      );
    });

    /**
     * Verifies that a once listener can be manually removed before
     * the event is emitted.
     */
    test('should allow a once listener to be removed before invocation', () => {
      const listener = jest.fn();
      const off = bus.once('user:login', listener);
      off();

      bus.emit('user:login', { timestamp: Date.now(), userId: '123' });
      expect(listener).not.toHaveBeenCalled();
    });
  });

  describe('off - Removing Listeners', () => {
    /**
     * Verifies that `off` removes a specific listener by reference,
     * leaving other listeners for the same event intact.
     */
    test('should remove a specific listener by reference', () => {
      const listener1 = jest.fn();
      const listener2 = jest.fn();

      bus.on('user:login', listener1);
      bus.on('user:login', listener2);
      bus.off('user:login', listener1);

      bus.emit('user:login', { timestamp: Date.now(), userId: '123' });

      expect(listener1).not.toHaveBeenCalled();
      expect(listener2).toHaveBeenCalledTimes(1);
    });

    /**
     * Verifies that removing a listener that was never registered
     * does not throw an error.
     */
    test('should not throw when removing an unregistered listener', () => {
      const listener = jest.fn();
      expect(() => bus.off('user:login', listener)).not.toThrow();
    });

    /**
     * Verifies that attempting to remove a listener for an event type
     * that has no registered listeners does not throw an error.
     */
    test('should not throw when removing a listener for an event with no listeners', () => {
      const listener = jest.fn();
      bus.on('user:login', listener);

      expect(() => bus.off('user:logout' as any, listener)).not.toThrow();
    });
  });

  describe('Priority System', () => {
    /**
     * Verifies that listeners are invoked in descending priority order,
     * with higher priority values executing first.
     */
    test('should invoke listeners in descending priority order', () => {
      const order: number[] = [];

      bus.on('user:login', () => order.push(1), { priority: 10 });
      bus.on('user:login', () => order.push(2), { priority: 100 });
      bus.on('user:login', () => order.push(3), { priority: 50 });

      bus.emit('user:login', { timestamp: Date.now(), userId: '123' });

      expect(order).toEqual([2, 3, 1]);
    });

    /**
     * Verifies that listeners with the same priority are invoked
     * in the order they were registered.
     */
    test('should invoke listeners with equal priority in registration order', () => {
      const order: number[] = [];

      bus.on('user:login', () => order.push(1), { priority: 10 });
      bus.on('user:login', () => order.push(2), { priority: 10 });
      bus.on('user:login', () => order.push(3), { priority: 10 });

      bus.emit('user:login', { timestamp: Date.now(), userId: '123' });

      expect(order).toEqual([1, 2, 3]);
    });

    /**
     * Verifies that listeners registered without an explicit priority
     * are assigned the default priority of 0.
     */
    test('should default to priority 0 when not specified', () => {
      const order: number[] = [];

      bus.on('user:login', () => order.push(1), { priority: 5 });
      bus.on('user:login', () => order.push(2));
      bus.on('user:login', () => order.push(3), { priority: -5 });

      bus.emit('user:login', { timestamp: Date.now(), userId: '123' });

      expect(order).toEqual([1, 2, 3]);
    });
  });

  describe('emit Options', () => {
    /**
     * Verifies that emit can be called with only options
     * and no payload, which is useful for sticky-only invocations.
     */
    test('should support emit with options only and no payload', () => {
      const listener = jest.fn();
      bus.on('user:login', listener);

      bus.emit('user:login', { sticky: true } as any);

      expect(listener).toHaveBeenCalledTimes(1);
    });
  });

  describe('emitAsync', () => {
    /**
     * Verifies that events can be emitted asynchronously and that
     * listeners are still invoked.
     */
    test('should emit events asynchronously', async () => {
      const listener = jest.fn();
      jest.fn((_ctx: any, next: () => Promise<void>) => next());

      bus.on('user:login', listener);

      await bus.emitAsync('user:login', { timestamp: Date.now(), userId: '123' });

      expect(listener).toHaveBeenCalledTimes(1);
    });

    /**
     * Verifies that synchronous emit does not throw when there are
     * no async middlewares registered.
     */
    test('should not throw during synchronous emit when there are no async middlewares', () => {
      expect(() => {
        bus.emit('user:login', { timestamp: Date.now(), userId: '123' });
      }).not.toThrow();
    });
  });

  describe('ctx Context', () => {
    /**
     * Verifies that the context object passed to listeners contains
     * all expected properties: event, block, blocked, matched,
     * meta, params, and payload.
     */
    test('should pass a context object with expected properties to listeners', () => {
      const listener = jest.fn();
      bus.on('user:login', listener);

      bus.emit('user:login', { timestamp: Date.now(), userId: '123' });

      const ctx = listener.mock.calls[0][1];
      expect(ctx).toHaveProperty('event', 'user:login');
      expect(ctx).toHaveProperty('block');
      expect(ctx).toHaveProperty('blocked');
      expect(ctx).toHaveProperty('matched');
      expect(ctx).toHaveProperty('meta');
      expect(ctx).toHaveProperty('params');
      expect(ctx).toHaveProperty('payload');
    });

    /**
     * Verifies that calling ctx.block() inside a listener prevents
     * all subsequent listeners from being invoked.
     */
    test('should prevent subsequent listeners from executing when ctx.block() is called', () => {
      const listener1 = jest.fn((payload: any, ctx: any) => {
        if (ctx) {
          ctx.block();
        }
      });
      const listener2 = jest.fn();
      const listener3 = jest.fn();

      bus.on('user:login', listener1, { priority: 100 });
      bus.on('user:login', listener2, { priority: 50 });
      bus.on('user:login', listener3, { priority: 10 });

      bus.emit('user:login', { timestamp: Date.now(), userId: '123' });

      expect(listener1).toHaveBeenCalledTimes(1);
      expect(listener2).not.toHaveBeenCalled();
      expect(listener3).not.toHaveBeenCalled();
    });

    /**
     * Verifies that ctx.meta is populated with values from the
     * metaPatch option passed to emit.
     */
    test('should populate ctx.meta from the metaPatch emit option', () => {
      const listener = jest.fn();
      bus.on('user:login', listener);

      bus.emit(
        'user:login',
        { timestamp: Date.now(), userId: '123' },
        {
          metaPatch: { priority: 1, source: 'test' },
        },
      );

      const ctx: any = listener.mock.calls[0][1];
      expect(ctx.meta).toEqual({ priority: 1, source: 'test' });
    });
  });

  describe('onMatch - Pattern Matching', () => {
    /**
     * Verifies that onMatch with a string pattern triggers for
     * all events matching the glob-like pattern.
     */
    test('should match events against a string pattern', () => {
      const handler = jest.fn();
      bus.onMatch('user:.*', handler);

      bus.emit('user:login', { timestamp: Date.now(), userId: '123' });
      bus.emit('user:logout', { userId: '123' });

      expect(handler).toHaveBeenCalledTimes(2);
    });

    /**
     * Verifies that onMatch does not trigger for events that
     * do not satisfy the pattern.
     */
    test('should not trigger for events that do not match the pattern', () => {
      const handler = jest.fn();
      bus.onMatch('user:*', handler);

      bus.emit('data:sync', { items: [] });

      expect(handler).not.toHaveBeenCalled();
    });

    /**
     * Verifies that pattern-based handlers receive the matched
     * event name, payload, context, and match parameters.
     */
    test('should pass match parameters to the pattern handler', () => {
      const handler = jest.fn();
      bus.onMatch('user:.*', handler);

      bus.emit('user:login', { timestamp: Date.now(), userId: '123' });

      expect(handler).toHaveBeenCalledWith(
        'user:login',
        expect.objectContaining({ userId: '123' }),
        expect.any(Object),
        expect.any(Object),
      );
    });

    /**
     * Verifies that onceMatch triggers only once, even when
     * multiple matching events are emitted.
     */
    test('should trigger onceMatch handler only once', () => {
      const handler = jest.fn();
      bus.onceMatch('user:.*', handler);

      bus.emit('user:login', { timestamp: Date.now(), userId: '123' });
      bus.emit('user:logout', { userId: '123' });

      expect(handler).toHaveBeenCalledTimes(1);
    });

    /**
     * Verifies that patterns can be specified as RegExp objects
     * in addition to strings.
     */
    test('should support RegExp pattern matching', () => {
      const handler = jest.fn();
      bus.onMatch(/^user:/, handler);

      bus.emit('user:login', { timestamp: Date.now(), userId: '123' });

      expect(handler).toHaveBeenCalledTimes(1);
    });

    /**
     * Verifies that pattern-based listeners respect the priority
     * system, invoking higher priority handlers first.
     */
    test('should respect priority ordering for pattern-based listeners', () => {
      const order: number[] = [];

      bus.onMatch('user:.*', () => order.push(1), { priority: 10 });
      bus.onMatch('user:.*', () => order.push(2), { priority: 100 });
      bus.onMatch('user:.*', () => order.push(3), { priority: 50 });

      bus.emit('user:login', { timestamp: Date.now(), userId: '123' });

      expect(order).toEqual([2, 3, 1]);
    });

    /**
     * Verifies that ctx.block() in a regular listener also prevents
     * pattern-based listeners from executing.
     */
    test('should prevent pattern-based listeners when a regular listener calls ctx.block()', () => {
      const handler = jest.fn();
      const blockingListener = jest.fn((payload: any, ctx: any) => {
        if (ctx) {
          ctx.block();
        }
      });

      bus.on('user:login', blockingListener, { priority: 100 });
      bus.onMatch('user:*', handler, { priority: 50 });

      bus.emit('user:login', { timestamp: Date.now(), userId: '123' });

      expect(handler).not.toHaveBeenCalled();
    });
  });

  describe('Sticky Events', () => {
    /**
     * Verifies that a sticky event emitted before a listener is registered
     * is replayed to that listener upon registration.
     */
    test('should replay sticky events to listeners registered after emission', () => {
      const payload = { timestamp: Date.now(), userId: '123' };
      bus.emit('user:login', payload, { sticky: true });

      const listener = jest.fn();
      bus.on('user:login', listener);

      expect(listener).toHaveBeenCalledTimes(1);
      expect(listener).toHaveBeenCalledWith(payload);
    });

    /**
     * Verifies that a sticky event is consumed by a once listener,
     * meaning the listener is invoked and then automatically removed.
     */
    test('should be consumed by a once listener', () => {
      bus.emit('user:login', { timestamp: Date.now(), userId: '123' }, { sticky: true });

      const listener = jest.fn();
      bus.once('user:login', listener);

      const listener2 = jest.fn();
      bus.on('user:login', listener2);

      expect(listener).toHaveBeenCalledTimes(1);
      expect(listener2).toHaveBeenCalledTimes(1);
    });

    /**
     * Verifies that the 'consume' sticky mode removes the sticky event
     * after the first replay, preventing subsequent listeners from
     * receiving it.
     */
    test('should remove sticky event after first replay in consume mode', () => {
      const payload = { timestamp: Date.now(), userId: '123' };
      bus.emit('user:login', payload, {
        sticky: true,
        stickyMode: 'consume',
      });

      const listener1 = jest.fn();
      bus.on('user:login', listener1);

      expect(listener1).toHaveBeenCalledTimes(1);

      const listener2 = jest.fn();
      bus.on('user:login', listener2);

      expect(listener2).not.toHaveBeenCalled();
    });

    /**
     * Verifies that a listener can opt-in to consuming a sticky event
     * via the consumeSticky option, even when the event was emitted
     * in replay mode.
     */
    test('should allow consumeSticky option on listener to consume sticky events', () => {
      bus.emit(
        'user:login',
        { timestamp: Date.now(), userId: '123' },
        {
          sticky: true,
          stickyMode: 'replay',
        },
      );

      const listener = jest.fn();
      bus.on('user:login', listener, { consumeSticky: true });

      expect(listener).toHaveBeenCalledTimes(1);

      const listener2 = jest.fn();
      bus.on('user:login', listener2);
      expect(listener2).not.toHaveBeenCalled();
    });

    /**
     * Verifies that the stickyMax configuration limits the total number
     * of sticky events stored across all event types.
     */
    test('should enforce the stickyMax global limit', () => {
      const smallBus = new EventBus<TestEvents>({ stickyMax: 2 });

      smallBus.emit('user:login', { timestamp: Date.now(), userId: '1' }, { sticky: true });
      smallBus.emit('user:logout', { userId: '2' }, { sticky: true });
      smallBus.emit('data:sync', { items: [] }, { sticky: true });

      const handler = jest.fn();
      smallBus.onMatch('.*', handler);

      expect(handler).toHaveBeenCalledTimes(2);
      smallBus.destroy();
    });

    /**
     * Verifies that the stickyExactMax configuration limits the number
     * of sticky events stored per exact event name, keeping only the
     * most recent ones.
     */
    test('should enforce the stickyExactMax per-event limit', () => {
      const smallBus = new EventBus<TestEvents>({ stickyExactMax: 1 });

      smallBus.emit('user:login', { timestamp: Date.now(), userId: '1' }, { sticky: true });
      smallBus.emit('user:login', { timestamp: Date.now(), userId: '2' }, { sticky: true });

      const listener = jest.fn();
      smallBus.on('user:login', listener);

      expect(listener).toHaveBeenCalledTimes(1);
      expect(listener).toHaveBeenCalledWith(expect.objectContaining({ userId: '2' }));

      smallBus.destroy();
    });
  });

  describe('Middleware', () => {
    /**
     * Verifies that synchronous middleware executes before listeners
     * for the same event emission.
     */
    test('should execute synchronous middleware before listeners', () => {
      const order: string[] = [];

      bus.use((ctx, next) => {
        order.push('middleware');
        next();
      });

      bus.on('user:login', () => {
        order.push('listener');
      });

      bus.emit('user:login', { timestamp: Date.now(), userId: '123' });

      expect(order).toEqual(['middleware', 'listener']);
    });

    /**
     * Verifies that multiple middleware functions execute in registration
     * order, following the onion model (wrap around each other).
     */
    test('should execute middleware in registration order with onion model', () => {
      const order: string[] = [];

      bus.use((ctx, next) => {
        order.push('mw1-before');
        next();
        order.push('mw1-after');
      });

      bus.use((ctx, next) => {
        order.push('mw2-before');
        next();
        order.push('mw2-after');
      });

      bus.emit('user:login', { timestamp: Date.now(), userId: '123' });

      expect(order).toEqual(['mw1-before', 'mw2-before', 'mw2-after', 'mw1-after']);
    });

    /**
     * Verifies that calling ctx.block() inside middleware prevents
     * subsequent middleware and listeners from executing.
     */
    test('should prevent subsequent middleware and listeners when ctx.block() is called', () => {
      const mw1 = jest.fn((ctx: any, next: any) => next());
      const mw2 = jest.fn((ctx: any, next: any) => {
        ctx.block();
        next();
      });
      const mw3 = jest.fn((ctx: any, next: any) => next());
      const listener = jest.fn();

      bus.use(mw1);
      bus.use(mw2);
      bus.use(mw3);
      bus.on('user:login', listener);

      bus.emit('user:login', { timestamp: Date.now(), userId: '123' });

      expect(mw1).toHaveBeenCalled();
      expect(mw2).toHaveBeenCalled();
      expect(mw3).not.toHaveBeenCalled();
      expect(listener).not.toHaveBeenCalled();
    });

    /**
     * Verifies that middleware with a match function only executes
     * for events that satisfy the predicate.
     */
    test('should filter middleware execution with the match option', () => {
      const mw = jest.fn((ctx: any, next: any) => next());

      bus.use(mw, {
        match: (ctx) => ctx.event === 'user:login',
      });

      bus.emit('user:login', { timestamp: Date.now(), userId: '123' });
      bus.emit('user:logout', { userId: '123' });

      expect(mw).toHaveBeenCalledTimes(1);
    });

    /**
     * Verifies that middleware with a pattern option only executes
     * for events matching the glob pattern.
     */
    test('should filter middleware execution with the pattern option', () => {
      const mw = jest.fn((ctx: any, next: any) => next());

      bus.use(mw, { pattern: 'user:.*' });

      bus.emit('user:login', { timestamp: Date.now(), userId: '123' });
      bus.emit('data:sync', { items: [] });

      expect(mw).toHaveBeenCalledTimes(1);
    });

    /**
     * Verifies that middleware can be removed by calling the
     * unsubscribe function returned from bus.use().
     */
    test('should allow middleware to be removed', () => {
      const mw = jest.fn((ctx: any, next: any) => next());
      const off = bus.use(mw);
      off();

      bus.emit('user:login', { timestamp: Date.now(), userId: '123' });

      expect(mw).not.toHaveBeenCalled();
    });
  });

  describe('EventScope - Scope Management', () => {
    /**
     * Verifies that bus.createScope() returns a valid EventScope
     * instance.
     */
    test('should create a new EventScope instance', () => {
      const scope = bus.createScope();
      expect(scope).toBeInstanceOf(EventScope);
    });

    /**
     * Verifies that listeners registered within a scope are
     * automatically removed when the scope is destroyed.
     */
    test('should automatically remove scope listeners when the scope is destroyed', () => {
      const scope = bus.createScope();
      const listener = jest.fn();

      scope.on('user:login', listener);
      scope.destroy();

      bus.emit('user:login', { timestamp: Date.now(), userId: '123' });
      expect(listener).not.toHaveBeenCalled();
    });

    /**
     * Verifies that withScope creates a temporary scope, executes
     * the callback, and then destroys the scope automatically.
     */
    test('should create a temporary scope with withScope and destroy it after the callback', async () => {
      let capturedScope: EventScope<TestEvents> | undefined;

      await bus.withScope((scope) => {
        capturedScope = scope;
        scope.on('user:login', () => {});
      });

      expect(capturedScope!.isDestroyed).toBe(true);
      expect(() => bus.emit('user:login', { timestamp: Date.now(), userId: '123' })).not.toThrow();
    });

    /**
     * Verifies that scopes can be nested, and destroying the parent
     * scope also removes listeners in the child scope.
     */
    test('should support nested scopes and cascade destruction', () => {
      const parentScope = bus.createScope();
      const childScope = bus.createScope(parentScope);

      const listener = jest.fn();
      childScope.on('user:login', listener);

      parentScope.destroy();

      bus.emit('user:login', { timestamp: Date.now(), userId: '123' });
      expect(listener).not.toHaveBeenCalled();
    });

    /**
     * Verifies that emitting an event from a scope correctly stamps
     * the context meta with a reference to that scope.
     */
    test('should stamp the scope reference in ctx.meta when emitting from a scope', () => {
      const scope = bus.createScope();
      const listener = jest.fn();

      bus.on('user:login', listener);

      scope.emit('user:login', { timestamp: Date.now(), userId: '123' });

      expect(listener).toHaveBeenCalledTimes(1);

      const ctx: any = listener.mock.calls[0][1];
      expect(ctx.meta).toHaveProperty('scope', scope);
    });

    /**
     * Verifies that scope.run() executes a callback with the scope
     * set as the current scope, so that listeners registered inside
     * the callback are associated with that scope.
     */
    test('should associate listeners registered inside scope.run() with the scope', () => {
      const scope = bus.createScope();
      const listener = jest.fn();

      scope.run(() => {
        bus.on('user:login', listener);
      });

      scope.destroy();

      bus.emit('user:login', { timestamp: Date.now(), userId: '123' });
      expect(listener).not.toHaveBeenCalled();
    });

    /**
     * Verifies that parentScope.withChild creates a child scope,
     * executes the callback, and then destroys the child scope.
     */
    test('should create a child scope with withChild and destroy it after the callback', async () => {
      const parentScope = bus.createScope();

      await parentScope.withChild((childScope) => {
        childScope.on('user:login', () => {});
        expect(childScope.isDestroyed).toBe(false);
      });

      expect(() => bus.emit('user:login', { timestamp: Date.now(), userId: '123' })).not.toThrow();
    });
  });

  describe('DispatcherRuntime - Custom Runtime', () => {
    /**
     * Verifies that a custom DispatcherRuntime can be provided to
     * the EventBus, and that the runtime correctly tracks the active
     * scope.
     */
    test('should support a custom DispatcherRuntime for scope tracking', () => {
      const runtime = new DispatcherRuntime<TestEvents>();
      const customBus = new EventBus<TestEvents>({ runtime });

      const listener = jest.fn();
      const scope = customBus.createScope();

      scope.run(() => {
        expect(runtime.getScope()).toBeDefined();
        expect(runtime.getScope()).toBe(scope);
        customBus.on('user:login', listener);
      });

      expect(runtime.getScope()).toBeUndefined();

      scope.destroy();
      customBus.emit('user:login', { timestamp: Date.now(), userId: '123' });
      expect(listener).not.toHaveBeenCalled();

      customBus.destroy();
    });
  });

  describe('Error Handling', () => {
    /**
     * Verifies that the onError handler is invoked when a listener
     * throws an error during emission.
     */
    test('should invoke the onError handler when a listener throws', () => {
      const onError = jest.fn();
      const errorBus = new EventBus<TestEvents>({ logErrors: false, onError });

      const listener = jest.fn(() => {
        throw new Error('Listener error');
      });

      errorBus.on('user:login', listener);
      errorBus.emit('user:login', { timestamp: Date.now(), userId: '123' });

      expect(onError).toHaveBeenCalledTimes(1);
      expect(onError).toHaveBeenCalledWith(expect.any(Error));
      errorBus.destroy();
    });

    /**
     * Verifies that an error thrown in middleware is re-thrown
     * and not swallowed.
     */
    test('should throw when middleware throws an error', () => {
      const onError = jest.fn();
      const errorBus = new EventBus<TestEvents>({ logErrors: false, onError });

      errorBus.use(() => {
        throw new Error('Middleware error');
      });

      errorBus.on('user:login', () => {});

      expect(() => {
        errorBus.emit('user:login', { timestamp: Date.now(), userId: '123' });
      }).toThrow('Middleware error');

      errorBus.destroy();
    });

    /**
     * Verifies that the logErrors configuration controls whether
     * errors are printed to the console.
     */
    test('should control error logging via the logErrors configuration option', () => {
      const consoleSpy1 = jest.spyOn(console, 'error').mockImplementation(() => {});
      const onError1 = jest.fn();
      const bus1 = new EventBus<TestEvents>({ logErrors: true, onError: onError1 });

      bus1.on('user:login', () => {
        throw new Error('Error 1');
      });
      bus1.emit('user:login', { timestamp: Date.now(), userId: '1' });

      expect(consoleSpy1).toHaveBeenCalled();
      consoleSpy1.mockRestore();
      bus1.destroy();

      const consoleSpy2 = jest.spyOn(console, 'error').mockImplementation(() => {});
      const onError2 = jest.fn();
      const bus2 = new EventBus<TestEvents>({ logErrors: false, onError: onError2 });

      bus2.on('user:login', () => {
        throw new Error('Error 2');
      });
      bus2.emit('user:login', { timestamp: Date.now(), userId: '2' });

      expect(consoleSpy2).not.toHaveBeenCalled();
      consoleSpy2.mockRestore();
      bus2.destroy();
    });
  });

  describe('destroy - Destruction and Cleanup', () => {
    /**
     * Verifies that after calling destroy(), emitting an event
     * throws an error indicating the instance has been destroyed.
     */
    test('should throw when emitting after destruction', () => {
      const listener = jest.fn();
      bus.on('user:login', listener);

      bus.destroy();

      expect(() => bus.emit('user:login', { timestamp: Date.now(), userId: '123' })).toThrow(
        'EventBus instance has been destroyed.',
      );
    });

    /**
     * Verifies that after calling destroy(), registering a new
     * listener throws an error indicating the instance has been
     * destroyed.
     */
    test('should throw when registering a listener after destruction', () => {
      bus.destroy();

      expect(() => bus.on('user:login', () => {})).toThrow('EventBus instance has been destroyed.');
    });

    /**
     * Verifies that calling destroy() multiple times is safe and
     * does not throw.
     */
    test('should be idempotent when destroy is called multiple times', () => {
      bus.destroy();
      expect(() => bus.destroy()).not.toThrow();
    });

    /**
     * Verifies that reset() clears all listeners and sticky events
     * but keeps the EventBus instance usable for new registrations.
     */
    test('should clear all listeners on reset while keeping the instance usable', () => {
      const listener = jest.fn();
      bus.on('user:login', listener);
      bus.reset();

      bus.emit('user:login', { timestamp: Date.now(), userId: '123' });
      expect(listener).not.toHaveBeenCalled();

      const newListener = jest.fn();
      bus.on('user:login', newListener);
      bus.emit('user:login', { timestamp: Date.now(), userId: '456' });
      expect(newListener).toHaveBeenCalledTimes(1);
    });

    /**
     * Verifies that clearListeners() removes all event listeners
     * but retains any registered middleware.
     */
    test('should clear only listeners on clearListeners while keeping middleware', () => {
      const mw = jest.fn((ctx: any, next: any) => next());
      const listener = jest.fn();

      bus.use(mw);
      bus.on('user:login', listener);
      bus.clearListeners();

      bus.emit('user:login', { timestamp: Date.now(), userId: '123' });

      expect(listener).not.toHaveBeenCalled();
      expect(mw).toHaveBeenCalled();
    });
  });

  describe('Edge Cases', () => {
    /**
     * Verifies that emitting an event with no registered listeners
     * does not throw an error.
     */
    test('should not throw when emitting an event with no listeners', () => {
      expect(() => {
        bus.emit('user:login', { timestamp: Date.now(), userId: '123' });
      }).not.toThrow();
    });

    /**
     * Verifies that if a listener removes another listener during
     * emission, all remaining listeners are still invoked correctly
     * (snapshot semantics).
     */
    test('should safely handle a listener removing another listener during emission', () => {
      const listener1 = jest.fn(function (this: any, _payload: any, _ctx: any) {
        off2();
      });
      const listener2 = jest.fn();

      bus.on('user:login', listener1);
      const off2 = bus.on('user:login', listener2);

      bus.emit('user:login', { timestamp: Date.now(), userId: '123' });

      expect(listener1).toHaveBeenCalledTimes(1);
      expect(listener2).toHaveBeenCalledTimes(1);
    });

    /**
     * Verifies that an EventBus instantiated with an empty event map
     * type does not throw and behaves correctly.
     */
    test('should not throw when initialized with an empty event map type', () => {
      type EmptyEvents = Record<string, never>;
      const emptyBus = new EventBus<EmptyEvents>();

      expect(emptyBus).toBeInstanceOf(EventBus);
      emptyBus.destroy();
    });

    /**
     * Verifies that listeners receive payloads of varying types
     * (number, string, null) correctly.
     */
    test('should correctly dispatch payloads of different types', () => {
      type MixedEvents = {
        arrayEvent: string[];
        nullEvent: null;
        numberEvent: number;
        objectEvent: { data: string };
        stringEvent: string;
      };

      const mixedBus = new EventBus<MixedEvents>();
      const listener = jest.fn();

      mixedBus.on('numberEvent', listener);
      mixedBus.emit('numberEvent', 123);
      expect(listener).toHaveBeenCalledWith(123, expect.any(Object));

      mixedBus.on('stringEvent', listener);
      mixedBus.emit('stringEvent', 'test');
      expect(listener).toHaveBeenCalledWith('test', expect.any(Object));

      mixedBus.on('nullEvent', listener);
      mixedBus.emit('nullEvent', null);
      expect(listener).toHaveBeenCalledWith(null, expect.any(Object));

      mixedBus.destroy();
    });

    /**
     * Verifies that calling methods on a destroyed scope throws
     * an appropriate error.
     */
    test('should throw when calling methods on a destroyed scope', () => {
      const scope = bus.createScope();
      scope.destroy();

      expect(() => scope.on('user:login', () => {})).toThrow('EventScope already destroyed.');
      expect(() => scope.emit('user:login', { timestamp: Date.now(), userId: '123' })).toThrow(
        'EventScope already destroyed.',
      );
    });

    /**
     * Verifies that ctx.matched is an empty array when no
     * pattern-based handlers matched the event.
     */
    test('should have an empty matched array when no pattern handlers match', () => {
      const listener = jest.fn((payload: any, ctx: any) => {
        expect(ctx.matched).toEqual([]);
      });

      bus.on('user:login', listener);
      bus.emit('user:login', { timestamp: Date.now(), userId: '123' });
    });
  });
});
