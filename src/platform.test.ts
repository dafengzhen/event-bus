import { afterEach, describe, expect, it, vi } from 'vitest';

import type { EventBusPlatform, Middleware } from './types.js';

import { createEventBus } from './index.js';
import { createPlatform } from './platform.js';

afterEach(() => {
  vi.unstubAllGlobals();
  vi.restoreAllMocks();
  vi.useRealTimers();
});

describe('platform compatibility', () => {
  it('supports scope and caller cancellation without AbortSignal.any', async () => {
    vi.stubGlobal('AbortSignal', undefined);
    const bus = createEventBus<{ ready: void }>();
    const scope = bus.scope();
    const caller = new AbortController();
    const listener = vi.fn<() => void>();
    scope.on('ready', listener, { signal: caller.signal });
    scope.use((_, next) => next(), { signal: caller.signal });
    const pending = scope.waitFor('ready', { signal: caller.signal });
    scope.dispose();
    await expect(pending).rejects.toMatchObject({ name: 'AbortError' });
    expect(bus.emit('ready').delivered).toBe(0);
    expect(listener).not.toHaveBeenCalled();
    expect(caller.signal.aborted).toBe(false);
  });

  it('uses injected controllers throughout the scope tree without a global controller', async () => {
    const Controller = AbortController;
    const createAbortController = vi.fn<() => AbortController>(() => new Controller());
    vi.stubGlobal('AbortController', undefined);
    const bus = createEventBus<{ ready: void }>({ platform: { createAbortController } });
    const parent = bus.scope();
    const child = parent.scope();
    const pending = child.waitFor('ready');
    bus.dispose();
    await expect(pending).rejects.toMatchObject({ name: 'AbortError' });
    expect(createAbortController).toHaveBeenCalledTimes(3);
    expect(parent.disposed).toBe(true);
    expect(child.disposed).toBe(true);
  });

  it('prefers native controllers when available', () => {
    expect(createPlatform().createAbortController()).toBeInstanceOf(AbortController);
  });

  it('prefers injected controllers even when native controllers are available', () => {
    const controller = new AbortController();
    const createAbortController = vi.fn<() => AbortController>(() => controller);
    expect(createPlatform({ createAbortController }).createAbortController()).toBe(controller);
    expect(createAbortController).toHaveBeenCalledExactlyOnceWith();
  });

  it('cancels the scope tree without DOM cancellation or event globals', async () => {
    vi.stubGlobal('AbortController', undefined);
    vi.stubGlobal('AbortSignal', undefined);
    vi.stubGlobal('EventTarget', undefined);
    vi.stubGlobal('Event', undefined);
    vi.stubGlobal('DOMException', undefined);
    const bus = createEventBus<{ ready: void }>();
    const parent = bus.scope();
    const child = parent.scope();
    const listener = vi.fn<() => void>();
    const middleware = vi.fn<Middleware<{ ready: void }>>((_, next) => next());
    child.on('ready', listener);
    child.use(middleware);
    const pending = child.waitFor('ready');
    bus.dispose();
    await expect(pending).rejects.toMatchObject({ name: 'AbortError' });
    await expect(child.waitFor('ready')).rejects.toBe(child.signal.reason);
    expect(parent.disposed).toBe(true);
    expect(child.disposed).toBe(true);
    expect(bus.listenerCount()).toBe(0);
    expect(listener).not.toHaveBeenCalled();
    expect(middleware).not.toHaveBeenCalled();
    expect(() => child.signal.throwIfAborted()).toThrow(child.signal.reason);
    expect(AbortController).toBeUndefined();
  });

  it('cleans up fallback scope hooks without cancelling siblings or the bus', async () => {
    vi.stubGlobal('AbortController', undefined);
    const bus = createEventBus<{ ready: void }>();
    const scope = bus.scope();
    const sibling = bus.scope();
    const listener = vi.fn<() => void>();
    const middleware = vi.fn<Middleware<{ ready: void }>>((_, next) => next());
    scope.on('ready', listener);
    scope.use(middleware);
    const cancelled = scope.waitFor('ready');
    const pending = sibling.waitFor('ready');
    scope.dispose();
    scope.dispose();
    await expect(cancelled).rejects.toMatchObject({ name: 'AbortError' });
    expect(bus.emit('ready').delivered).toBe(1);
    await expect(pending).resolves.toMatchObject({ type: 'ready' });
    expect(listener).not.toHaveBeenCalled();
    expect(middleware).not.toHaveBeenCalled();
    expect(sibling.disposed).toBe(false);
    bus.dispose();
  });

  it.each([undefined, null, false, 0, '', 'cancelled'])(
    'preserves fallback abort reason %s and only notifies once',
    async (reason) => {
      vi.stubGlobal('AbortController', undefined);
      const createError = vi.fn<EventBusPlatform['createError']>(() => new Error('custom abort'));
      const controller = createPlatform({ createError }).createAbortController();
      const listener = vi.fn<(event: Event) => void>();
      controller.signal.onabort = listener;
      const bus = createEventBus<{ ready: void }>();
      const pending = bus.waitFor('ready', { signal: controller.signal });
      expect(() => controller.signal.throwIfAborted()).not.toThrow();
      controller.abort(reason);
      controller.abort('ignored');
      const expected = reason === undefined ? createError.mock.results[0]!.value : reason;
      await expect(pending).rejects.toBe(expected);
      expect(controller.signal.reason).toBe(expected);
      expect(listener).toHaveBeenCalledExactlyOnceWith(
        expect.objectContaining({
          currentTarget: controller.signal,
          target: controller.signal,
          type: 'abort',
        }),
      );
      bus.dispose();
    },
  );

  it('honors fallback listener removal, deduplication, once, and listener objects', () => {
    vi.stubGlobal('AbortController', undefined);
    const { abort, signal } = createPlatform().createAbortController();
    const removed = vi.fn<() => void>();
    const once = vi.fn<() => void>();
    const listener = { handleEvent: vi.fn<(event: Event) => void>() };
    signal.addEventListener('abort', removed);
    signal.removeEventListener('abort', removed, true);
    signal.removeEventListener('abort', removed);
    signal.addEventListener('abort', once, { once: true });
    signal.addEventListener('abort', once);
    signal.addEventListener('abort', listener);
    abort();
    signal.dispatchEvent(new Event('abort'));
    expect(removed).not.toHaveBeenCalled();
    expect(once).toHaveBeenCalledTimes(1);
    expect(listener.handleEvent).toHaveBeenCalledTimes(2);
  });

  it('continues fallback cancellation when an abort listener throws', () => {
    vi.useFakeTimers();
    vi.stubGlobal('AbortController', undefined);
    const bus = createEventBus<{ ready: void }>();
    const parent = bus.scope();
    parent.signal.addEventListener('abort', () => {
      throw new Error('observer failed');
    });
    const child = parent.scope();
    bus.dispose();
    expect(child.disposed).toBe(true);
    expect(() => vi.runAllTimers()).toThrow('observer failed');
  });

  it('skips fallback listeners removed during abort dispatch', () => {
    vi.stubGlobal('AbortController', undefined);
    const controller = createPlatform().createAbortController();
    const listener = vi.fn<() => void>();
    controller.signal.addEventListener('abort', () => {
      controller.signal.removeEventListener('abort', listener);
    });
    controller.signal.addEventListener('abort', listener);
    controller.abort();
    expect(listener).not.toHaveBeenCalled();
  });

  it('preserves error names without DOMException, including old signals without reason', async () => {
    vi.useFakeTimers();
    vi.stubGlobal('DOMException', undefined);
    const bus = createEventBus<{ ready: void }>();
    const timed = bus.waitFor('ready', { timeout: 0 }).catch((error: unknown) => error);
    await vi.runAllTimersAsync();
    expect(await timed).toMatchObject({ name: 'TimeoutError' });
    const cleared = bus.waitFor('ready');
    bus.clear();
    await expect(cleared).rejects.toMatchObject({ name: 'AbortError' });
    const caller = new AbortController();
    Object.defineProperty(caller.signal, 'reason', { value: undefined });
    const aborted = bus.waitFor('ready', { signal: caller.signal });
    caller.abort();
    await expect(aborted).rejects.toMatchObject({ name: 'AbortError' });
    const scope = bus.scope();
    const disposed = scope.waitFor('ready');
    scope.dispose();
    await expect(disposed).rejects.toMatchObject({ name: 'AbortError' });
    await expect(scope.waitFor('ready')).rejects.toMatchObject({ name: 'AbortError' });
    bus.dispose();
  });

  it.each([null, false, 0, ''])('preserves the explicit abort reason %s', async (reason) => {
    const bus = createEventBus<{ ready: void }>();
    const caller = new AbortController();
    const pending = bus.waitFor('ready', { signal: caller.signal });
    caller.abort(reason);
    await expect(pending).rejects.toBe(reason);
  });

  it('supports opaque timer handles and inherits error and timer adapters in scopes', async () => {
    let fire = () => {};
    const timer = {};
    const clearTimeout = vi.fn<(timer: unknown) => void>();
    const createError = vi.fn<(message: string, name: string) => Error>((message, name) =>
      Object.assign(new Error(message), { name }),
    );
    const setTimeout = vi.fn<(callback: () => void, timeout: number) => unknown>((callback) => {
      fire = callback;
      return timer;
    });
    const bus = createEventBus<{ ready: void }>({
      platform: { clearTimeout, createError, setTimeout },
    });
    const scope = bus.scope().scope();
    const timed = scope.waitFor('ready', { timeout: 123 });
    expect(setTimeout).toHaveBeenCalledWith(expect.any(Function), 123);
    fire();
    await expect(timed).rejects.toMatchObject({ name: 'TimeoutError' });
    expect(clearTimeout).toHaveBeenCalledExactlyOnceWith(timer);
    expect(createError).toHaveBeenCalledWith('Timed out waiting for an event', 'TimeoutError');
    const resolved = scope.waitFor('ready', { timeout: 123 });
    bus.emit('ready');
    await resolved;
    expect(clearTimeout).toHaveBeenCalledTimes(2);
    scope.dispose();
    expect(createError).toHaveBeenCalledWith('The operation was aborted', 'AbortError');
  });

  it('cleans up caller abort hooks when scheduling a timeout fails', async () => {
    const caller = new AbortController();
    const remove = vi.spyOn(caller.signal, 'removeEventListener');
    const bus = createEventBus<{ ready: void }>({
      platform: {
        setTimeout: () => {
          throw new Error('scheduler');
        },
      },
    });
    await expect(bus.waitFor('ready', { signal: caller.signal, timeout: 1 })).rejects.toThrow(
      'scheduler',
    );
    expect(remove).toHaveBeenCalledTimes(1);
    expect(bus.listenerCount()).toBe(0);
    bus.clear();
  });
});

describe('scope abort hook cleanup', () => {
  it.each(['off', 'once', 'replay', 'clear', 'caller', 'scope'] as const)(
    'detaches both signals after %s',
    (action) => {
      const bus = createEventBus<{ ready: void }>();
      const scope = bus.scope();
      const caller = new AbortController();
      const removeScope = vi.spyOn(scope.signal, 'removeEventListener');
      const removeCaller = vi.spyOn(caller.signal, 'removeEventListener');
      bus.emit('ready', undefined, { retain: true });
      const off = scope.on('ready', () => {}, {
        once: action === 'once' || action === 'replay',
        replay: action === 'replay',
        signal: caller.signal,
      });
      if (action === 'off') {
        off();
      }
      if (action === 'once') {
        bus.emit('ready');
      }
      if (action === 'clear') {
        bus.clear();
      }
      if (action === 'caller') {
        caller.abort();
      }
      if (action === 'scope') {
        scope.dispose();
      }
      off();
      expect(removeScope).toHaveBeenCalledTimes(1);
      expect(removeCaller).toHaveBeenCalledTimes(1);
      expect(bus.listenerCount()).toBe(0);
    },
  );

  it('does not attach hooks for an already aborted caller', async () => {
    const bus = createEventBus<{ ready: void }>();
    const scope = bus.scope();
    const caller = new AbortController();
    caller.abort('cancelled');
    const addScope = vi.spyOn(scope.signal, 'addEventListener');
    scope.on('ready', () => {}, { signal: caller.signal });
    await expect(scope.waitFor('ready', { signal: caller.signal })).rejects.toBe('cancelled');
    expect(addScope).not.toHaveBeenCalled();
    expect(bus.listenerCount()).toBe(0);
  });
});
