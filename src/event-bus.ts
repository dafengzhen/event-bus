import { createPatternMatcher } from '@dafengzhen/derivative-matcher';

import type {
  AsyncMiddleware,
  DispatchFailure,
  DispatchResult,
  EmitArgs,
  EventBusOptions,
  EventContext,
  EventMap,
  EventPattern,
  Listener,
  Middleware,
  MiddlewareOptions,
  Observer,
  SubscriptionOptions,
  Unsubscribe,
  WaitForOptions,
} from './types.js';

import { EventScope } from './event-scope.js';
import { abortReason, onAbort } from './lifecycle.js';

interface Dispatch<E extends object> {
  delivered: number;
  readonly errors: DispatchFailure<E>[];
  readonly event: EventContext<E>;
  readonly generation: number;
  readonly listeners: ListenerEntry<E>[];
  readonly middleware: MiddlewareEntry<E>[];
}

interface ListenerEntry<E extends object> {
  active: boolean;
  readonly handler: Observer<E>;
  readonly matches: (type: keyof E) => boolean;
  off: Unsubscribe;
  readonly once: boolean;
  readonly order: number;
  readonly priority: number;
}

interface MiddlewareEntry<E extends object> {
  active: boolean;
  readonly async: boolean;
  readonly handler: AsyncMiddleware<E> | Middleware<E>;
  readonly matches: (type: keyof E) => boolean;
  off: Unsubscribe;
}

/** Explicit subscriptions, deterministic delivery, and instance-owned lifecycle. */
export class EventBus<E extends object = EventMap> {
  get disposed(): boolean {
    return this.lifecycle.signal.aborted;
  }
  private eventId = 0;
  private readonly exact = new Map<keyof E, Set<ListenerEntry<E>>>();
  private generation = 0;
  private readonly lifecycle = new AbortController();
  private readonly middleware = new Set<MiddlewareEntry<E>>();
  private readonly observers = new Set<ListenerEntry<E>>();
  private readonly options: EventBusOptions<E>;
  private readonly retained = new Map<keyof E, EventContext<E>>();
  private sequence = 0;

  private readonly waits = new Set<(reason: unknown) => void>();

  constructor(options: EventBusOptions<E> = {}) {
    const retainedLimit = options.retainedLimit ?? 100;
    if (!Number.isSafeInteger(retainedLimit) || retainedLimit < 0) {
      throw new RangeError('retainedLimit must be a nonnegative safe integer.');
    }
    this.options = { ...options, retainedLimit };
  }

  /** Remove subscriptions, middleware and retained data; reject pending waitFor calls. */
  clear(): void {
    this.generation += 1;
    for (const fail of this.waits) {
      fail(new DOMException('EventBus was cleared', 'AbortError'));
    }
    for (const entries of this.exact.values()) {
      for (const entry of entries) {
        entry.off();
      }
    }
    for (const entry of this.observers) {
      entry.off();
    }
    for (const entry of this.middleware) {
      entry.off();
    }
    this.clearRetained();
  }

  /** Forget retained data without removing subscriptions. Omit type to forget all. */
  clearRetained(type?: keyof E): void {
    if (type === undefined) {
      this.retained.clear();
    } else {
      this.retained.delete(type);
    }
  }

  dispose(): void {
    if (!this.disposed) {
      this.lifecycle.abort();
      this.clear();
    }
  }

  /** Synchronous delivery. Async handlers are an error; use emitAsync for them. */
  emit(...args: EmitArgs<E>): DispatchResult<E> {
    const state = this.prepare(args);
    if (state.middleware.some((entry) => entry.async)) {
      throw new TypeError('Async middleware registered for this event: use emitAsync().');
    }
    this.runSync(state, 0, () => {
      this.retain(state.event, args[2]?.retain);
      for (const entry of state.listeners) {
        if (this.halted(state)) {
          break;
        }
        if (entry.active) {
          this.invokeSync(entry, state, 'listener');
        }
      }
    });
    return this.finish(state);
  }

  /** Sequential delivery, awaiting each listener and middleware before continuing. */
  async emitAsync(...args: EmitArgs<E>): Promise<DispatchResult<E>> {
    const state = this.prepare(args);
    const run = async (index: number): Promise<void> => {
      if (this.halted(state)) {
        return;
      }
      const entry = state.middleware[index];
      if (!entry) {
        this.retain(state.event, args[2]?.retain);
        for (const listener of state.listeners) {
          if (this.halted(state)) {
            break;
          }
          if (!listener.active) {
            continue;
          }
          if (listener.once) {
            listener.off();
          }
          state.delivered += 1;
          try {
            await listener.handler(state.event);
          } catch (error) {
            this.failure(state, error, 'listener');
          }
        }
        return;
      }
      if (!entry.active) {
        return run(index + 1);
      }
      let called = false;
      let open = true;
      let downstream: Promise<void> | undefined;
      try {
        await entry.handler(state.event, () => {
          if (called || !open) {
            throw new Error('next() must be called once, before middleware completes.');
          }
          called = true;
          downstream = run(index + 1);
          return downstream;
        });
      } catch (error) {
        this.failure(state, error, 'middleware');
      } finally {
        open = false;
        // Drain next() even when middleware forgot to await it.
        await downstream;
      }
    };
    await run(0);
    return this.finish(state);
  }

  listenerCount(type?: keyof E): number {
    if (type !== undefined) {
      return (
        (this.exact.get(type)?.size ?? 0) +
        [...this.observers].filter((entry) => entry.matches(type)).length
      );
    }
    return [...this.exact.values()].reduce(
      (count, entries) => count + entries.size,
      this.observers.size,
    );
  }

  on<K extends keyof E>(
    type: K,
    listener: Listener<E, NoInfer<K>>,
    options?: SubscriptionOptions,
  ): Unsubscribe {
    this.assertActive();
    let entries = this.exact.get(type);
    if (!entries) {
      entries = new Set();
      this.exact.set(type, entries);
    }
    const bucket = entries;
    return this.subscribe(
      bucket,
      (event) => listener(event.payload as E[K], event as EventContext<E, K>),
      (key) => key === type,
      options,
      () => {
        if (bucket.size === 0 && this.exact.get(type) === bucket) {
          this.exact.delete(type);
        }
      },
    );
  }

  onAny(observer: Observer<E>, options?: SubscriptionOptions): Unsubscribe {
    this.assertActive();
    return this.subscribe(this.observers, observer, () => true, options);
  }

  once<K extends keyof E>(
    type: K,
    listener: Listener<E, NoInfer<K>>,
    options?: Omit<SubscriptionOptions, 'once'>,
  ): Unsubscribe {
    return this.on(type, listener, { ...options, once: true });
  }

  onPattern(
    pattern: EventPattern,
    observer: Observer<E>,
    options?: SubscriptionOptions,
  ): Unsubscribe {
    this.assertActive();
    return this.subscribe(this.observers, observer, compilePattern(pattern), options);
  }

  scope(): EventScope<E> {
    this.assertActive();
    return new EventScope(this, this.lifecycle.signal);
  }

  use(middleware: Middleware<E>, options?: MiddlewareOptions): Unsubscribe {
    return this.addMiddleware(middleware, false, options);
  }

  useAsync(middleware: AsyncMiddleware<E>, options?: MiddlewareOptions): Unsubscribe {
    return this.addMiddleware(middleware, true, options);
  }

  waitFor<K extends keyof E>(
    type: K,
    options: WaitForOptions<E, NoInfer<K>> = {},
  ): Promise<EventContext<E, K>> {
    return new Promise((resolve, reject) => {
      this.assertActive();
      if (
        options.timeout !== undefined &&
        (!Number.isFinite(options.timeout) ||
          options.timeout < 0 ||
          options.timeout > 2_147_483_647)
      ) {
        throw new RangeError('timeout must be between 0 and 2147483647 milliseconds.');
      }
      let off: Unsubscribe = () => {};
      let detachSignal: Unsubscribe = () => {};
      let timer: ReturnType<typeof setTimeout> | undefined;
      let settled = false;
      const cleanup = () => {
        settled = true;
        off();
        detachSignal();
        this.waits.delete(fail);
        clearTimeout(timer);
      };
      const fail = (reason: unknown) => {
        cleanup();
        reject(reason);
      };
      this.waits.add(fail);
      detachSignal = onAbort(options.signal, () => fail(abortReason(options.signal!)));
      if (settled) {
        return;
      }
      if (options.timeout !== undefined) {
        timer = setTimeout(
          () => fail(new DOMException('Timed out waiting for an event', 'TimeoutError')),
          options.timeout,
        );
      }
      try {
        off = this.on(
          type,
          (payload, event) => {
            try {
              if (!options.filter || options.filter(payload, event)) {
                cleanup();
                resolve(event);
              }
            } catch (error) {
              fail(error);
            }
          },
          { replay: options.replay },
        );
        // Replay can settle synchronously before on() returns its unsubscribe handle.
        if (settled) {
          off();
        }
      } catch (error) {
        fail(error);
      }
    });
  }

  private addMiddleware(
    handler: AsyncMiddleware<E> | Middleware<E>,
    async: boolean,
    options: MiddlewareOptions = {},
  ): Unsubscribe {
    this.assertActive();
    let detach: Unsubscribe = () => {};
    const entry: MiddlewareEntry<E> = {
      active: true,
      async,
      handler,
      matches: options.pattern === undefined ? () => true : compilePattern(options.pattern),
      off: () => {
        entry.active = false;
        this.middleware.delete(entry);
        detach();
      },
    };
    this.middleware.add(entry);
    detach = onAbort(options.signal, entry.off);
    return entry.off;
  }

  private assertActive(): void {
    if (this.disposed) {
      throw new Error('EventBus has been disposed.');
    }
  }

  private context(
    type: keyof E,
    payload: E[keyof E],
    id: number,
    timestamp: number,
    meta: Readonly<Record<string, unknown>>,
    replayed = false,
  ): EventContext<E> {
    let stopped = false;
    return Object.freeze({
      id,
      meta,
      payload,
      get propagationStopped() {
        return stopped;
      },
      replayed,
      stopPropagation() {
        stopped = true;
      },
      timestamp,
      type,
    }) as EventContext<E>;
  }

  private failure(state: Dispatch<E>, error: unknown, phase: DispatchFailure<E>['phase']): void {
    state.errors.push(Object.freeze({ error, event: state.event, phase }));
  }

  private finish(state: Dispatch<E>): DispatchResult<E> {
    const unhandled: unknown[] = [];
    for (const failure of state.errors) {
      if (this.options.onError) {
        try {
          callSync(this.options.onError, [failure], 'onError must be synchronous.');
        } catch (error) {
          unhandled.push(error);
        }
      } else {
        unhandled.push(failure.error);
      }
    }
    if (unhandled.length) {
      throw new AggregateError(unhandled, 'Event dispatch failed.');
    }
    return Object.freeze({
      delivered: state.delivered,
      errors: Object.freeze(state.errors),
      event: state.event,
    });
  }

  private halted(state: Dispatch<E>): boolean {
    return this.disposed || state.generation !== this.generation || state.event.propagationStopped;
  }

  private invokeSync(
    entry: ListenerEntry<E>,
    state: Dispatch<E>,
    phase: 'listener' | 'replay',
  ): void {
    if (entry.once) {
      entry.off();
    }
    state.delivered += 1;
    try {
      callSync(entry.handler, [state.event]);
    } catch (error) {
      this.failure(state, error, phase);
    }
  }

  private prepare(args: EmitArgs<E>): Dispatch<E> {
    this.assertActive();
    const [type, payload, options] = args;
    const event = this.context(
      type,
      payload as E[keyof E],
      ++this.eventId,
      Date.now(),
      Object.freeze({ ...options?.meta }),
    );
    const listeners = [...(this.exact.get(type) ?? []), ...this.observers].filter((entry) =>
      entry.matches(type),
    );
    listeners.sort((a, b) => b.priority - a.priority || a.order - b.order);
    return {
      delivered: 0,
      errors: [],
      event,
      generation: this.generation,
      listeners,
      middleware: [...this.middleware].filter((entry) => entry.matches(type)),
    };
  }

  private retain(event: EventContext<E>, enabled?: boolean): void {
    const limit = this.options.retainedLimit!;
    if (!enabled || limit === 0) {
      return;
    }
    this.retained.delete(event.type);
    this.retained.set(event.type, event);
    if (this.retained.size > limit) {
      this.retained.delete(this.retained.keys().next().value!);
    }
  }

  private runSync(state: Dispatch<E>, index: number, deliver: Unsubscribe): void {
    if (this.halted(state)) {
      return;
    }
    const entry = state.middleware[index];
    if (!entry) {
      deliver();
      return;
    }
    if (!entry.active) {
      this.runSync(state, index + 1, deliver);
      return;
    }
    let called = false;
    let open = true;
    try {
      callSync(entry.handler as Middleware<E>, [
        state.event,
        () => {
          if (called || !open) {
            throw new Error('next() must be called once, before middleware completes.');
          }
          called = true;
          this.runSync(state, index + 1, deliver);
        },
      ]);
    } catch (error) {
      this.failure(state, error, 'middleware');
    } finally {
      open = false;
    }
  }

  private subscribe(
    entries: Set<ListenerEntry<E>>,
    handler: Observer<E>,
    matches: (type: keyof E) => boolean,
    options: SubscriptionOptions = {},
    onRemove?: Unsubscribe,
  ): Unsubscribe {
    const priority = options.priority ?? 0;
    if (!Number.isFinite(priority)) {
      onRemove?.();
      throw new RangeError('priority must be finite.');
    }
    let detach: Unsubscribe = () => {};
    const entry: ListenerEntry<E> = {
      active: true,
      handler,
      matches,
      off: () => {
        if (entry.active) {
          entry.active = false;
          entries.delete(entry);
          detach();
          onRemove?.();
        }
      },
      once: options.once ?? false,
      order: this.sequence++,
      priority,
    };
    entries.add(entry);
    detach = onAbort(options.signal, entry.off);
    if (entry.active && options.replay) {
      try {
        // Snapshot: replay callbacks can mutate retained data or emit recursively.
        // oxlint-disable-next-line unicorn/no-useless-spread
        for (const saved of [...this.retained.values()]) {
          if (!entry.active || this.disposed) {
            break;
          }
          if (matches(saved.type)) {
            const event = this.context(
              saved.type,
              saved.payload,
              saved.id,
              saved.timestamp,
              saved.meta,
              true,
            );
            const state: Dispatch<E> = {
              delivered: 0,
              errors: [],
              event,
              generation: this.generation,
              listeners: [],
              middleware: [],
            };
            this.invokeSync(entry, state, 'replay');
            this.finish(state);
          }
        }
      } catch (error) {
        entry.off();
        throw error;
      }
    }
    return entry.off;
  }
}

export function createEventBus<E extends object = EventMap>(
  options?: EventBusOptions<E>,
): EventBus<E> {
  return new EventBus<E>(options);
}

/** Observe rejected promises even when an async handler is mistakenly used in emit(). */
function callSync<Args extends unknown[]>(
  callback: (...args: Args) => unknown,
  args: Args,
  message = 'Async handler encountered: use emitAsync() (or avoid async replay).',
): void {
  const value = callback(...args);
  if (isThenable(value)) {
    void Promise.resolve(value).catch(() => {});
    throw new TypeError(message);
  }
}

function compilePattern(pattern: EventPattern): (type: PropertyKey) => boolean {
  const matcher = typeof pattern === 'string' ? createPatternMatcher(pattern) : pattern;
  // Own the cursor: g/y regexes and caller mutations cannot affect matching.
  if (matcher instanceof RegExp) {
    const regex = new RegExp(matcher.source, matcher.flags);
    return (type) => {
      regex.lastIndex = 0;
      return typeof type === 'string' && regex.test(type);
    };
  }
  return (type) => typeof type === 'string' && matcher.test(type);
}

function isThenable(value: unknown): value is PromiseLike<unknown> {
  return (
    value !== null &&
    (typeof value === 'object' || typeof value === 'function') &&
    typeof (value as PromiseLike<unknown>).then === 'function'
  );
}
