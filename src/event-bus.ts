import { clearCompileCache, compile, type Matcher } from '@dafengzhen/regex-derivative';

import type {
  CompiledPatternListenerEntry,
  EmitContext,
  EmitOptions,
  EventBusOptions,
  EventMap,
  ExactListenerEntry,
  Listener,
  MatchedPattern,
  MiddlewareAsync,
  MiddlewareEntry,
  MiddlewareSync,
  OnOptions,
  PatternHandler,
  PatternListenerInfo,
  ReplayOneResult,
  StickyEvent,
  StickyMode,
  UseOptions,
} from './types.ts';

import { DispatcherRuntime } from './dispatcher-runtime.ts';
import { EventScope } from './event-scope.ts';

const NO_REPLAY: ReplayOneResult = { found: false };

/**
 * A type-safe event bus with support for exact event matching, pattern-based matching,
 * middleware pipelines, sticky events, and scoped listener lifecycles.
 *
 * @typeParam E - An event map where keys are event names and values are their payload types.
 *
 * @example
 * ```ts
 * type MyEvents = {
 *   'user:login': { userId: string };
 *   'user:logout': { userId: string };
 * };
 *
 * const bus = new EventBus<MyEvents>();
 *
 * bus.on('user:login', (payload, ctx) => {
 *   console.log(payload.userId);
 * });
 *
 * bus.emit('user:login', { userId: '123' });
 * ```
 *
 * @author dafengzhen
 */
export class EventBus<E extends EventMap> {
  /**
   * Optional error handler called when a listener or middleware throws an error.
   * If not provided, errors are rethrown asynchronously.
   */
  readonly onError?: (e: unknown) => void;

  /**
   * The dispatcher runtime responsible for managing execution scope and context.
   */
  readonly runtime: DispatcherRuntime<E>;

  /**
   * Whether to clear the global DFA compile cache when this instance is destroyed.
   */
  private readonly clearGlobalCacheOnDestroy: boolean;

  /**
   * Whether this instance has been destroyed. A destroyed instance cannot emit or register listeners.
   */
  private destroyed = false;

  /**
   * Internal cache of compiled DFA matchers keyed by pattern string.
   */
  private readonly dfaCache = new Map<string, Matcher>();

  /**
   * Map of exact (string-keyed) listeners, stored in insertion order for unregistration.
   */
  private readonly exactListeners = new Map<keyof E, Array<ExactListenerEntry<E, any>>>();

  /**
   * Map of exact listeners sorted by priority (descending) then insertion order (ascending) for dispatch.
   */
  private readonly exactListenersSorted = new Map<keyof E, Array<ExactListenerEntry<E, any>>>();

  /**
   * Whether to log errors to the console when listeners or middleware throw.
   */
  private readonly logErrors: boolean;

  /**
   * Ordered array of middleware entries. Middleware are executed in registration order,
   * subject to their optional match filters.
   */
  private middlewares: Array<MiddlewareEntry<E>> = [];

  /**
   * Ordered array of compiled pattern listeners, sorted by priority then insertion order.
   */
  private patternListeners: Array<CompiledPatternListenerEntry<E>> = [];

  /**
   * Monotonically increasing sequence counter used to determine registration order among
   * entries with equal priority.
   */
  private seq = 0;

  /**
   * Keys of sticky events maintained in insertion order for replay consistency.
   */
  private stickyEventKeys: string[] = [];

  /**
   * Map of sticky event batches keyed by event string (non-exact/pattern matching).
   */
  private stickyEvents = new Map<string, StickyEvent[]>();

  /**
   * Map of sticky exact events keyed by the exact event key.
   */
  private stickyExact = new Map<keyof E, StickyEvent[]>();

  /**
   * Maximum number of sticky events stored per exact event key.
   */
  private readonly stickyExactMax: number;

  /**
   * Maximum number of distinct sticky event keys stored for pattern replay.
   */
  private readonly stickyMax: number;

  /**
   * Creates a new EventBus instance.
   *
   * @param options - Configuration options for the event bus.
   * @param options.onError - Optional global error handler for listener/middleware errors.
   * @param options.logErrors - Whether to log errors to `console.error`. Defaults to `true`.
   * @param options.clearGlobalCacheOnDestroy - Whether to clear the shared DFA compile cache
   *   when this instance is destroyed. Defaults to `false`.
   * @param options.runtime - A custom {@link DispatcherRuntime} instance. If not provided,
   *   a new one is created.
   * @param options.stickyMax - Maximum number of distinct sticky event keys to retain
   *   for pattern replay. Defaults to `200`.
   * @param options.stickyExactMax - Maximum number of sticky events to retain per exact event key.
   *   Defaults to `1`.
   */
  constructor(options?: EventBusOptions) {
    this.onError = options?.onError;
    this.logErrors = options?.logErrors ?? true;
    this.clearGlobalCacheOnDestroy = options?.clearGlobalCacheOnDestroy ?? false;
    this.runtime = options?.runtime ?? new DispatcherRuntime<E>();
    this.stickyMax = this.normalizeLimit(options?.stickyMax ?? 200);
    this.stickyExactMax = this.normalizeLimit(options?.stickyExactMax ?? 1);
  }

  /**
   * Removes all registered exact and pattern listeners.
   * Middleware and sticky events are NOT cleared by this method (use {@link reset} for that).
   */
  clearListeners(): void {
    this.exactListeners.clear();
    this.exactListenersSorted.clear();
    this.patternListeners = [];
  }

  /**
   * Creates a new {@link EventScope} bound to this bus.
   * All listeners registered via the scope's `on`/`onMatch` methods will be automatically
   * removed when the scope is destroyed.
   *
   * @param parent - An optional parent scope to inherit from.
   * @returns A new {@link EventScope} instance.
   */
  createScope(parent?: EventScope<E>): EventScope<E> {
    this.assertNotDestroyed();
    return new EventScope(this, parent);
  }

  /**
   * Destroys the event bus, clearing all listeners, middleware, sticky events, and caches.
   * Once destroyed, the instance can no longer be used.
   */
  destroy(): void {
    if (this.destroyed) {
      return;
    }

    this.reset();
    this.dfaCache.clear();

    if (this.clearGlobalCacheOnDestroy) {
      clearCompileCache();
    }

    this.destroyed = true;
  }

  /**
   * Synchronously emits an event to all matching listeners and middleware.
   *
   * **Throws** if any registered middleware returns a Promise (use {@link emitAsync} for async middleware).
   *
   * @param event - The event key to emit.
   * @param payload - The payload to pass to listeners.
   * @param options - Optional emission options (sticky, metaPatch, etc.).
   *
   * @example
   * ```ts
   * bus.emit('user:login', { userId: '123' });
   * ```
   */
  emit<K extends keyof E>(event: K, payload: E[K], options?: EmitOptions): void;
  /**
   * Synchronously emits an event with only options (no payload).
   *
   * @param event - The event key to emit.
   * @param options - Emission options.
   */
  emit<K extends keyof E>(event: K, options: EmitOptions): void;
  emit<K extends keyof E>(
    event: K,
    payloadOrOptions?: E[K] | EmitOptions,
    options?: EmitOptions,
  ): void {
    this.assertNotDestroyed();
    const { emitOptions, payload } = this.parseEmitArgs<E[K]>(payloadOrOptions, options);
    const result = this._emit(event, payload as E[K], emitOptions);

    if (this.isPromiseLike(result)) {
      throw new Error(
        '[EventBus] Async middleware detected in sync emit(). Use emitAsync() instead.',
      );
    }
  }

  /**
   * Asynchronously emits an event, supporting async middleware.
   *
   * @param event - The event key to emit.
   * @param payload - The payload to pass to listeners.
   * @param options - Optional emission options.
   * @returns A promise that resolves when all middleware and listeners have completed.
   */
  emitAsync<K extends keyof E>(event: K, payload: E[K], options?: EmitOptions): Promise<void>;
  /**
   * Asynchronously emits an event with only options (no payload).
   *
   * @param event - The event key to emit.
   * @param options - Emission options.
   * @returns A promise that resolves when all middleware and listeners have completed.
   */
  emitAsync<K extends keyof E>(event: K, options: EmitOptions): Promise<void>;
  async emitAsync<K extends keyof E>(
    event: K,
    payloadOrOptions?: E[K] | EmitOptions,
    options?: EmitOptions,
  ): Promise<void> {
    this.assertNotDestroyed();
    const { emitOptions, payload } = this.parseEmitArgs<E[K]>(payloadOrOptions, options);
    await this._emit(event, payload as E[K], emitOptions);
  }

  /**
   * Removes a previously registered exact listener.
   *
   * @param event - The event key the listener was registered for.
   * @param listener - The listener function to remove.
   */
  off<K extends keyof E>(event: K, listener: Listener<E[K], E, K>): void {
    this.assertNotDestroyed();
    this.removeExactListener(event, listener as Listener<any, E, any>);
  }

  /**
   * Registers a listener for a specific event key.
   *
   * @param event - The event key to listen for.
   * @param listener - The callback to invoke when the event is emitted.
   * @param options - Registration options (priority, consumeSticky, etc.).
   * @returns A function that, when called, removes the listener.
   *
   * @example
   * ```ts
   * const off = bus.on('user:login', (payload) => console.log(payload.userId));
   * // later:
   * off();
   * ```
   */
  on<K extends keyof E>(event: K, listener: Listener<E[K], E, K>, options?: OnOptions): () => void {
    this.assertNotDestroyed();
    return this.bindOffToCurrentScope(this.addExactListener(false, event, listener, options));
  }

  /**
   * Registers a listener that is automatically removed after its first invocation.
   *
   * @param event - The event key to listen for.
   * @param listener - The callback to invoke once.
   * @param options - Registration options.
   * @returns A function that, when called, removes the listener before it fires.
   */
  once<K extends keyof E>(
    event: K,
    listener: Listener<E[K], E, K>,
    options?: OnOptions,
  ): () => void {
    this.assertNotDestroyed();
    return this.bindOffToCurrentScope(this.addExactListener(true, event, listener, options));
  }

  /**
   * Registers a pattern-based handler that fires only once for any matching event.
   *
   * @param pattern - A string pattern (compiled via DFA) or a native RegExp.
   * @param handler - The callback invoked when a matching event is emitted.
   * @param options - Registration options.
   * @returns A function that removes the listener.
   */
  onceMatch(pattern: RegExp | string, handler: PatternHandler<E>, options?: OnOptions): () => void {
    this.assertNotDestroyed();
    return this.bindOffToCurrentScope(this.addPatternListener(true, pattern, handler, options));
  }

  /**
   * Registers a pattern-based handler that fires whenever a matching string event is emitted.
   *
   * Exact (keyed) events do NOT trigger pattern listeners.
   *
   * @param pattern - A string pattern (compiled via DFA) or a native RegExp.
   * @param handler - The callback invoked with the event string, payload, and extracted params.
   * @param options - Registration options.
   * @returns A function that removes the listener.
   *
   * @example
   * ```ts
   * bus.onMatch('user:*', (event, payload, params) => {
   *   console.log(`Matched event: ${event}`);
   * });
   * ```
   */
  onMatch(pattern: RegExp | string, handler: PatternHandler<E>, options?: OnOptions): () => void {
    this.assertNotDestroyed();
    return this.bindOffToCurrentScope(this.addPatternListener(false, pattern, handler, options));
  }

  /**
   * Resets the event bus completely: removes all listeners, middleware, and sticky events.
   * The instance remains usable after reset.
   */
  reset(): void {
    this.clearListeners();
    this.middlewares = [];
    this.stickyExact.clear();
    this.stickyEvents.clear();
    this.stickyEventKeys = [];
  }

  /**
   * Registers synchronous middleware that intercepts events before they reach listeners.
   *
   * Middleware execute in registration order and can inspect, modify, or block events via
   * the {@link EmitContext}.
   *
   * **Throws** if the middleware function returns a Promise. Use {@link useAsync} for async middleware.
   *
   * @param mw - The middleware function receiving the emit context and a `next` callback.
   * @param options - Options to filter which events the middleware applies to.
   * @returns A function that removes the middleware.
   *
   * @example
   * ```ts
   * const off = bus.use((ctx, next) => {
   *   console.log('Before:', ctx.event);
   *   next();
   *   console.log('After:', ctx.event);
   * });
   * ```
   */
  use(mw: MiddlewareSync<E>, options?: UseOptions<E>): () => void {
    this.assertNotDestroyed();

    const entry: MiddlewareEntry<E> = {
      fn: ((ctx: EmitContext<E>, next: () => void) => {
        const result = mw(ctx, next);
        if (this.isPromiseLike(result)) {
          throw new Error(
            '[EventBus] Synchronous middleware cannot return a Promise. Use useAsync() for async middleware.',
          );
        }
      }) as MiddlewareSync<E>,
      isAsync: false,
      match: this.buildMiddlewareMatch(options),
    };

    return this.addMiddlewareEntry(entry);
  }

  /**
   * Registers asynchronous middleware that intercepts events before they reach listeners.
   *
   * Async middleware MUST call `next()` (which may return a Promise) and await its result.
   *
   * @param mw - The async middleware function.
   * @param options - Options to filter which events the middleware applies to.
   * @returns A function that removes the middleware.
   *
   * @example
   * ```ts
   * bus.useAsync(async (ctx, next) => {
   *   await someAsyncOperation();
   *   await next();
   * });
   * ```
   */
  useAsync(mw: MiddlewareAsync<E>, options?: UseOptions<E>): () => void {
    this.assertNotDestroyed();

    const entry: MiddlewareEntry<E> = {
      fn: mw,
      isAsync: true,
      match: this.buildMiddlewareMatch(options),
    };

    return this.addMiddlewareEntry(entry);
  }

  /**
   * Creates a temporary {@link EventScope}, executes the provided function within it,
   * and automatically destroys the scope when done (even on error).
   *
   * @param fn - The function to execute with the new scope.
   * @param options - Optional configuration (e.g., a parent scope).
   * @returns A promise resolving to the return value of `fn`.
   *
   * @example
   * ```ts
   * const result = await bus.withScope((scope) => {
   *   scope.on('user:login', handler);
   *   return someAsyncWork();
   * });
   * // All scope listeners are now removed.
   * ```
   */
  async withScope<T>(
    fn: (scope: EventScope<E>) => Promise<T> | T,
    options?: { parent?: EventScope<E> },
  ): Promise<T> {
    this.assertNotDestroyed();
    const parent = options?.parent ?? this.runtime.getScope();
    const scope = this.createScope(parent);

    try {
      const ret = this.runtime.runWithScope(scope, () => fn(scope));
      return this.isPromiseLike<T>(ret) ? await ret : (ret as T);
    } finally {
      scope.destroy();
    }
  }

  /**
   * Internal emit implementation that builds the emit context, handles sticky replay,
   * matches pattern listeners, and executes the middleware chain + dispatch.
   *
   * @param event - The event key.
   * @param payload - The event payload.
   * @param options - Emit options.
   * @returns A Promise if any middleware is async, otherwise void.
   */
  private _emit<K extends keyof E>(
    event: K,
    payload: E[K],
    options?: EmitOptions,
  ): Promise<void> | void {
    let blocked = false;

    if (options?.sticky) {
      const mode: StickyMode = options.stickyMode ?? 'replay';
      this.pushStickyExact(event, payload, mode);
      if (typeof event === 'string') {
        this.pushStickyEvent(event, payload, mode);
      }
    }

    const matchedRaw = typeof event === 'string' ? this.matchPatternListeners(event) : [];
    let matchedInfo: ReadonlyArray<PatternListenerInfo<E>> | undefined;

    const ctx: EmitContext<E, K> = {
      block() {
        blocked = true;
      },

      get blocked() {
        return blocked;
      },

      event,

      get matched() {
        matchedInfo ??= Object.freeze(
          matchedRaw.map(({ entry, params }) => ({
            handler: entry.handler,
            once: entry.once,
            params: Object.freeze({ ...params }),
            pattern: entry.pattern,
            priority: entry.priority,
          })),
        );
        return matchedInfo;
      },

      meta: options?.metaPatch ? { ...options.metaPatch } : {},
      params: {},
      payload,
    };

    return this.runMiddlewares(ctx, matchedRaw);
  }

  /**
   * Registers an exact listener, schedules sticky replay if applicable, and returns
   * an unregistration function.
   *
   * @param once - Whether the listener should auto-remove after one invocation.
   * @param event - The event key.
   * @param listener - The callback.
   * @param options - Registration options.
   * @returns A function that removes the listener.
   */
  private addExactListener<K extends keyof E>(
    once: boolean,
    event: K,
    listener: Listener<E[K], E, K>,
    options?: OnOptions,
  ): () => void {
    const consumeStickyOverride = options?.consumeSticky;
    const priority = options?.priority ?? 0;
    const seq = ++this.seq;
    let registeredListener: Listener<E[K], E, K>;

    if (once) {
      registeredListener = ((payload: E[K], ctx?: EmitContext<E, K>) => {
        this.removeExactListener(event, registeredListener as Listener<any, E, any>);
        listener(payload, ctx);
      }) as Listener<E[K], E, K>;
    } else {
      registeredListener = listener;
    }

    this.addExactListenerEntry(event, {
      listener: registeredListener,
      priority,
      seq,
    });

    if (once) {
      const replay = this.replayExactStickyOne(event, consumeStickyOverride);
      if (replay.found) {
        this.safeCall(() => registeredListener(replay.payload as E[K]));
      }
    } else {
      for (const p of this.replayExactStickyAll(event, consumeStickyOverride)) {
        this.safeCall(() => registeredListener(p as E[K]));
      }
    }

    return this.makeOff(() =>
      this.removeExactListener(event, registeredListener as Listener<any, E, any>),
    );
  }

  /**
   * Adds an exact listener entry to both the unsorted (lookup) and sorted (dispatch) maps.
   *
   * @param event - The event key.
   * @param entry - The listener entry to add.
   */
  private addExactListenerEntry<K extends keyof E>(
    event: K,
    entry: ExactListenerEntry<E, any>,
  ): void {
    let entries = this.exactListeners.get(event);
    if (!entries) {
      entries = [];
      this.exactListeners.set(event, entries);
    }
    entries.push(entry);

    let sorted = this.exactListenersSorted.get(event);
    if (!sorted) {
      sorted = [];
      this.exactListenersSorted.set(event, sorted);
    }
    this.insertSorted(sorted, entry);
  }

  /**
   * Appends a middleware entry to the chain and returns its remover.
   *
   * @param entry - The middleware entry.
   * @returns A function that removes the middleware.
   */
  private addMiddlewareEntry(entry: MiddlewareEntry<E>): () => void {
    this.middlewares.push(entry);

    return this.makeOff(() => {
      const idx = this.middlewares.indexOf(entry);
      if (idx !== -1) {
        this.middlewares.splice(idx, 1);
      }
    });
  }

  /**
   * Builds and inserts a compiled pattern listener, then replays any matching sticky events.
   *
   * @param once - Whether the handler is one-shot.
   * @param pattern - The pattern to match.
   * @param handler - The handler callback.
   * @param options - Registration options.
   * @returns A function that removes the pattern listener.
   */
  private addPatternListener(
    once: boolean,
    pattern: RegExp | string,
    handler: PatternHandler<E>,
    options?: OnOptions,
  ): () => void {
    const entry = this.buildPatternEntry(once, pattern, handler, options);
    this.insertSorted(this.patternListeners, entry);

    this.replayStickyForEntry(entry, options?.consumeSticky);

    if (once && !this.patternListeners.includes(entry)) {
      return this.makeOff(() => {});
    }

    return this.makeOff(() => this.removePatternEntry(entry));
  }

  /**
   * Throws if the instance has been destroyed.
   */
  private assertNotDestroyed(): void {
    if (this.destroyed) {
      throw new Error('EventBus instance has been destroyed.');
    }
  }

  /**
   * If an active scope is present, registers the `off` function with it so the listener
   * is cleaned up when the scope is destroyed.
   *
   * @param off - The unregistration function.
   * @returns The same `off` function (for chaining).
   */
  private bindOffToCurrentScope(off: () => void): () => void {
    const scope = this.runtime.getScope();
    if (scope) {
      scope.registerOff(off);
    }
    return off;
  }

  /**
   * Builds a filter function for middleware based on the provided options.
   * Supports DFA-compiled pattern matching and/or a custom match callback.
   *
   * @param options - Options containing an optional pattern and/or match function.
   * @returns A function that tests whether the middleware applies to a given context, or `undefined` if no filter.
   */
  private buildMiddlewareMatch(options?: UseOptions<E>): MiddlewareEntry<E>['match'] {
    const hasPattern = !!options?.pattern;
    const customMatch = options?.match;

    if (!hasPattern && !customMatch) {
      return undefined;
    }

    const dfa = hasPattern ? this.getOrCompileDfa(options!.pattern as string) : undefined;

    return (ctx: EmitContext<E>) => {
      if (dfa && (typeof ctx.event !== 'string' || !dfa.match(ctx.event))) {
        return false;
      }
      return customMatch ? customMatch(ctx) : true;
    };
  }

  /**
   * Compiles a pattern listener entry, using either a DFA (string pattern) or a native RegExp.
   *
   * @param once - Whether the handler is one-shot.
   * @param pattern - The pattern.
   * @param handler - The handler callback.
   * @param options - Registration options (priority, etc.).
   * @returns A compiled pattern listener entry ready for insertion.
   */
  private buildPatternEntry(
    once: boolean,
    pattern: RegExp | string,
    handler: PatternHandler<E>,
    options?: OnOptions,
  ): CompiledPatternListenerEntry<E> {
    if (typeof pattern === 'string') {
      const dfa = this.getOrCompileDfa(pattern);
      return {
        handler,
        isNativeRegExp: false,
        match: (event: string) => (dfa.match(event) ? {} : null),
        once,
        pattern,
        priority: options?.priority ?? 80,
        seq: ++this.seq,
      };
    }

    const regex = new RegExp(pattern.source, pattern.flags);
    return {
      handler,
      isNativeRegExp: true,
      match: (event: string) => {
        regex.lastIndex = 0;
        const match = regex.exec(event);
        regex.lastIndex = 0;
        return match ? ({ ...match.groups } as Record<string, string>) : null;
      },
      once,
      pattern: pattern.toString(),
      priority: options?.priority ?? 80,
      seq: ++this.seq,
    };
  }

  /**
   * Comparator for sorting listeners/middleware by priority (descending) and sequence (ascending).
   *
   * @param a - First entry.
   * @param b - Second entry.
   * @returns Negative if `a` should come before `b`, positive otherwise.
   */
  private compareListenerOrder(
    a: { priority: number; seq: number },
    b: { priority: number; seq: number },
  ): number {
    if (b.priority !== a.priority) {
      return b.priority - a.priority;
    }
    return a.seq - b.seq;
  }

  /**
   * Removes a sticky event key from the tracking structures if its batch is empty.
   *
   * @param eventKey - The sticky event key.
   * @param batch - The optional batch to check (defaults to the map value).
   */
  private deleteStickyKeyIfEmpty(eventKey: string, batch?: StickyEvent[]): void {
    const stickyBatch = batch ?? this.stickyEvents.get(eventKey);
    if (stickyBatch && stickyBatch.length > 0) {
      return;
    }

    if (this.stickyEvents.delete(eventKey)) {
      const keyIdx = this.stickyEventKeys.indexOf(eventKey);
      if (keyIdx !== -1) {
        this.stickyEventKeys.splice(keyIdx, 1);
      }
    }
  }

  /**
   * Retrieves a compiled DFA matcher from the cache, or compiles and caches it if not present.
   *
   * @param pattern - The string pattern to compile.
   * @returns A DFA {@link Matcher}.
   */
  private getOrCompileDfa(pattern: string): Matcher {
    let cached = this.dfaCache.get(pattern);
    if (!cached) {
      cached = compile(pattern);
      this.dfaCache.set(pattern, cached);
    }
    return cached;
  }

  /**
   * Handles errors thrown by middleware, logging and/or forwarding to the `onError` handler.
   *
   * @param err - The error.
   */
  private handleMiddlewareError(err: unknown): void {
    if (this.logErrors) {
      console.error('[EventBus] Middleware error:', err);
    }

    if (this.onError) {
      try {
        this.onError(err);
      } catch (handlerErr) {
        this.rethrowAsync(handlerErr);
      }
    }
  }

  /**
   * Safe wrapper around `Object.prototype.hasOwnProperty.call`.
   * Checks whether an object has a property as its own (not inherited).
   *
   * @param value - The object to check.
   * @param key - The property key to test.
   * @returns `true` if the object owns the specified property directly.
   */
  private hasOwn(value: object, key: PropertyKey): boolean {
    return Object.prototype.hasOwnProperty.call(value, key);
  }

  /**
   * Inserts an entry into a sorted bucket using binary search to maintain invariant order
   * (priority descending, sequence ascending).
   *
   * @param bucket - The sorted array.
   * @param entry - The entry to insert.
   */
  private insertSorted<T extends { priority: number; seq: number }>(bucket: T[], entry: T): void {
    let lo = 0;
    let hi = bucket.length;

    while (lo < hi) {
      const mid = (lo + hi) >>> 1;
      if (this.compareListenerOrder(entry, bucket[mid]) < 0) {
        hi = mid;
      } else {
        lo = mid + 1;
      }
    }

    bucket.splice(lo, 0, entry);
  }

  /**
   * Dispatches an event to exact listeners first, then to matched pattern listeners,
   * respecting the `blocked` flag on the context.
   *
   * @param ctx - The emit context.
   * @param matched - The array of matched pattern listeners and their params.
   */
  private invokeDispatch(ctx: EmitContext<E, keyof E>, matched: MatchedPattern<E>[]): void {
    this.invokeExactListeners(ctx.event, ctx.payload, ctx);

    if (ctx.blocked) {
      return;
    }

    for (const { entry, params } of matched) {
      if (ctx.blocked) {
        return;
      }

      if (entry.once) {
        this.removePatternEntry(entry);
      }

      ctx.params = params;
      this.safeCall(() => entry.handler(ctx.event as string, ctx.payload, params, ctx as any));
    }
  }

  /**
   * Invokes all exact listeners for a given event key, respecting `ctx.blocked`.
   *
   * Listeners are invoked from a snapshot of the sorted array to avoid issues if a listener
   * removes itself during iteration.
   *
   * @param event - The event key.
   * @param payload - The event payload.
   * @param ctx - The emit context.
   */
  private invokeExactListeners<K extends keyof E>(
    event: K,
    payload: E[K],
    ctx: EmitContext<E, K>,
  ): void {
    const entries = this.exactListenersSorted.get(event);
    if (!entries || entries.length === 0) {
      return;
    }

    for (const { listener } of entries.slice()) {
      if (ctx.blocked) {
        return;
      }

      ctx.params = {};
      this.safeCall(() => (listener as Listener<E[K], E, K>)(payload, ctx));
    }
  }

  /**
   * Invokes a single middleware entry, wrapping `next` to enforce proper usage
   * (must call `next()` or `ctx.block()`, and `next()` must not be called multiple times).
   *
   * @param entry - The middleware entry.
   * @param ctx - The emit context.
   * @param next - The callback that continues the chain.
   * @returns A Promise if the middleware or the rest of the chain is async, otherwise void.
   */
  private invokeMiddleware(
    entry: MiddlewareEntry<E>,
    ctx: EmitContext<E>,
    next: () => Promise<void> | void,
  ): Promise<void> | void {
    let called = false;
    let childResult: Promise<void> | void = undefined;

    const assertContinued = () => {
      if (!called && !ctx.blocked) {
        throw new Error(
          'Middleware: next() was not called. Call next() to continue, or ctx.block() to stop dispatch.',
        );
      }
    };

    const nextFn = () => {
      if (called) {
        throw new Error('Middleware: next() called multiple times.');
      }
      called = true;
      childResult = next();
      return entry.isAsync ? Promise.resolve(childResult) : childResult;
    };

    let result: Promise<void> | void;
    try {
      result = (entry.fn as any)(ctx, nextFn);
    } catch (err) {
      this.handleMiddlewareError(err);
      throw err;
    }

    if (this.isPromiseLike(result)) {
      return Promise.resolve(result)
        .then(() => {
          assertContinued();
          return this.isPromiseLike(childResult) ? childResult : undefined;
        })
        .catch((err) => {
          this.handleMiddlewareError(err);
          throw err;
        });
    }

    assertContinued();

    if (this.isPromiseLike(childResult)) {
      return Promise.resolve(childResult).catch((err) => {
        this.handleMiddlewareError(err);
        throw err;
      }) as Promise<void>;
    }
  }

  /**
   * Checks whether a value is "thenable" (has a `.then` method).
   *
   * @param value - The value to test.
   * @returns `true` if the value is promise-like.
   */
  private isPromiseLike<T = unknown>(value: unknown): value is PromiseLike<T> {
    return (
      value !== null &&
      (typeof value === 'object' || typeof value === 'function') &&
      typeof (value as { then?: unknown }).then === 'function'
    );
  }

  /**
   * Heuristic to determine if a value looks like an {@link EmitOptions} object
   * by checking for the presence of known emit option keys (`sticky`, `stickyMode`, `metaPatch`).
   *
   * @param value - The value to test.
   * @returns `true` if the value is a non-null object containing at least one
   *   recognized emit option property.
   */
  private looksLikeEmitOptions(value: unknown): value is EmitOptions {
    if (!value || typeof value !== 'object') {
      return false;
    }

    return (
      this.hasOwn(value, 'sticky') ||
      this.hasOwn(value, 'stickyMode') ||
      this.hasOwn(value, 'metaPatch')
    );
  }

  /**
   * Creates an idempotent function that invokes the provided callback at most once.
   *
   * @param fn - The callback to guard.
   * @returns A function that forwards the first call and ignores subsequent calls.
   */
  private makeOff(fn: () => void): () => void {
    let done = false;
    return () => {
      if (done) {
        return;
      }
      done = true;
      fn();
    };
  }

  /**
   * Iterates over all compiled pattern listeners and returns those whose match function
   * succeeds against the given event string.
   *
   * @param event - The string event to match.
   * @returns An array of matched pattern entries with their extracted params.
   */
  private matchPatternListeners(event: string): MatchedPattern<E>[] {
    const matches: MatchedPattern<E>[] = [];

    for (const entry of this.patternListeners) {
      const params = entry.match(event);
      if (params) {
        matches.push({ entry, params });
      }
    }

    return matches;
  }

  /**
   * Coerces a value to a non-negative integer, clamping at `0`. Non-finite values become `0`.
   *
   * @param value - The value to normalize.
   * @returns The normalized limit.
   */
  private normalizeLimit(value: number): number {
    return Number.isFinite(value) ? Math.max(0, Math.floor(value)) : 0;
  }

  /**
   * Parses the overloaded arguments of `emit`/`emitAsync` into a normalized structure
   * indicating whether the call was options-only and what the payload and options are.
   *
   * @param payloadOrOptions - Either the payload or the options object.
   * @param options - The options object, if the first argument was the payload.
   * @returns An object containing the extracted `emitOptions`, a flag `optionsOnly`
   *   indicating whether the first argument was treated as options, and the `payload`
   *   (if provided).
   */
  private parseEmitArgs<P>(
    payloadOrOptions?: EmitOptions | P,
    options?: EmitOptions,
  ): {
    emitOptions: EmitOptions | undefined;
    optionsOnly: boolean;
    payload: P | undefined;
  } {
    if (options !== undefined) {
      return {
        emitOptions: options,
        optionsOnly: false,
        payload: payloadOrOptions as P | undefined,
      };
    }

    if (this.looksLikeEmitOptions(payloadOrOptions)) {
      return {
        emitOptions: payloadOrOptions,
        optionsOnly: true,
        payload: undefined,
      };
    }

    return {
      emitOptions: undefined,
      optionsOnly: false,
      payload: payloadOrOptions as P | undefined,
    };
  }

  /**
   * Stores a sticky event for pattern replay, respecting the `stickyMax` limit.
   * When the limit is exceeded, the oldest key is evicted.
   *
   * @param event - The string event key.
   * @param payload - The event payload.
   * @param mode - The sticky mode (`replay` or `consume`).
   */
  private pushStickyEvent(event: string, payload: unknown, mode: StickyMode): void {
    if (this.stickyMax <= 0) {
      return;
    }

    let events = this.stickyEvents.get(event);
    if (!events) {
      events = [];
      this.stickyEvents.set(event, events);
      this.stickyEventKeys.push(event);
    }

    events.push({ mode, payload });

    while (this.stickyEventKeys.length > this.stickyMax) {
      const oldestKey = this.stickyEventKeys.shift()!;
      this.stickyEvents.delete(oldestKey);
    }
  }

  /**
   * Stores a sticky exact event, respecting the per-key `stickyExactMax` limit by evicting
   * the oldest entries when overflow occurs.
   *
   * @param event - The exact event key.
   * @param payload - The event payload.
   * @param mode - The sticky mode.
   */
  private pushStickyExact<K extends keyof E>(event: K, payload: unknown, mode: StickyMode): void {
    if (this.stickyExactMax <= 0) {
      return;
    }

    const q = this.stickyExact.get(event) ?? [];
    q.push({ mode, payload });

    const overflow = q.length - this.stickyExactMax;
    if (overflow > 0) {
      q.splice(0, overflow);
    }

    this.stickyExact.set(event, q);
  }

  /**
   * Removes an exact listener from both the unsorted (lookup) and sorted (dispatch) maps.
   *
   * @param event - The event key.
   * @param listener - The listener function to remove.
   */
  private removeExactListener<K extends keyof E>(event: K, listener: Listener<any, E, any>): void {
    const entries = this.exactListeners.get(event);
    if (!entries) {
      return;
    }

    const index = entries.findIndex((e) => e.listener === listener);
    if (index === -1) {
      return;
    }

    const [entry] = entries.splice(index, 1);
    const sorted = this.exactListenersSorted.get(event);
    if (sorted) {
      const sortedIndex = sorted.indexOf(entry);
      if (sortedIndex !== -1) {
        sorted.splice(sortedIndex, 1);
      }
    }

    if (entries.length === 0) {
      this.exactListeners.delete(event);
      this.exactListenersSorted.delete(event);
    }
  }

  /**
   * Removes a compiled pattern listener entry from the array.
   *
   * @param entry - The entry to remove.
   */
  private removePatternEntry(entry: CompiledPatternListenerEntry<E>): void {
    const idx = this.patternListeners.indexOf(entry);
    if (idx !== -1) {
      this.patternListeners.splice(idx, 1);
    }
  }

  /**
   * Replays all stored sticky exact events for a given event key, optionally consuming
   * them based on their mode or an explicit override.
   *
   * @param event - The event key.
   * @param consumeOverride - Explicit consumption flag. If `undefined`, the event's mode determines behaviour.
   * @returns An array of payloads from the replayed events.
   */
  private replayExactStickyAll<K extends keyof E>(
    event: K,
    consumeOverride: boolean | undefined,
  ): unknown[] {
    const q = this.stickyExact.get(event);
    if (!q || q.length === 0) {
      return [];
    }

    const out: unknown[] = [];
    const remaining: StickyEvent[] = [];

    for (const item of q) {
      out.push(item.payload);
      if (!this.shouldConsumeSticky(consumeOverride, item.mode)) {
        remaining.push(item);
      }
    }

    if (remaining.length === 0) {
      this.stickyExact.delete(event);
    } else if (remaining.length !== q.length) {
      this.stickyExact.set(event, remaining);
    }

    return out;
  }

  /**
   * Replays the oldest stored sticky exact event for a given key. If the event's mode
   * (or an explicit override) indicates consumption, the event is removed from the queue.
   *
   * @param event - The event key.
   * @param consumeOverride - Explicit consumption flag.
   * @returns A result object indicating whether a replay occurred and the payload.
   */
  private replayExactStickyOne<K extends keyof E>(
    event: K,
    consumeOverride: boolean | undefined,
  ): ReplayOneResult {
    const q = this.stickyExact.get(event);
    if (!q || q.length === 0) {
      return NO_REPLAY;
    }

    const first = q[0];
    if (this.shouldConsumeSticky(consumeOverride, first.mode)) {
      q.shift();
      if (q.length === 0) {
        this.stickyExact.delete(event);
      }
    }

    return { found: true, payload: first.payload };
  }

  /**
   * Replays all matching sticky events for a newly registered pattern listener.
   * If the listener is `once` and fires, it is removed immediately after the first match,
   * and further sticky batches are skipped.
   *
   * @param entry - The compiled pattern listener entry.
   * @param consumeOverride - Explicit consumption flag.
   */
  private replayStickyForEntry(
    entry: CompiledPatternListenerEntry<E>,
    consumeOverride: boolean | undefined,
  ): void {
    for (const eventKey of this.stickyEventKeys.slice()) {
      const stickyBatch = this.stickyEvents.get(eventKey);
      if (!stickyBatch || stickyBatch.length === 0) {
        continue;
      }

      const params = entry.match(eventKey);
      if (!params) {
        continue;
      }

      for (let i = 0; i < stickyBatch.length; i++) {
        const item = stickyBatch[i];
        this.safeCall(() => entry.handler(eventKey, item.payload as any, params));

        if (this.shouldConsumeSticky(consumeOverride, item.mode)) {
          stickyBatch.splice(i, 1);
          i--;
        }

        if (entry.once) {
          this.removePatternEntry(entry);
          this.deleteStickyKeyIfEmpty(eventKey, stickyBatch);
          return;
        }
      }

      this.deleteStickyKeyIfEmpty(eventKey, stickyBatch);
    }
  }

  /**
   * Rethrows an error asynchronously using the best available mechanism
   * (`queueMicrotask`, `Promise.resolve().then`, or `setTimeout(0)`).
   * This prevents the error from being caught by synchronous try/catch blocks.
   *
   * @param err - The error to rethrow.
   */
  private rethrowAsync(err: unknown): void {
    if (typeof queueMicrotask !== 'undefined') {
      queueMicrotask(() => {
        throw err;
      });
      return;
    }

    if (typeof Promise !== 'undefined' && typeof Promise.resolve === 'function') {
      Promise.resolve().then(() => {
        throw err;
      });
      return;
    }

    setTimeout(() => {
      throw err;
    }, 0);
  }

  /**
   * Executes the middleware chain followed by dispatch, respecting `ctx.blocked` and
   * middleware match filters.
   *
   * @param ctx - The emit context.
   * @param rawMatches - Raw matched pattern entries (before freezing for `ctx.matched`).
   * @returns A Promise if any middleware is async, otherwise void.
   */
  private runMiddlewares<K extends keyof E>(
    ctx: EmitContext<E, K>,
    rawMatches: MatchedPattern<E>[],
  ): Promise<void> | void {
    const middlewares = this.middlewares.slice();
    let index = 0;

    const next = (): Promise<void> | void => {
      if (ctx.blocked) {
        return;
      }

      while (index < middlewares.length) {
        const entry = middlewares[index++];

        try {
          if (entry.match && !entry.match(ctx as EmitContext<E>)) {
            continue;
          }
        } catch (err) {
          this.handleMiddlewareError(err);
          throw err;
        }

        return this.invokeMiddleware(entry, ctx as EmitContext<E>, next);
      }

      this.invokeDispatch(ctx as EmitContext<E, keyof E>, rawMatches);
    };

    return next();
  }

  /**
   * Safely invokes a callback, catching and handling errors via `onError` or rethrowing
   * them asynchronously.
   *
   * @param fn - The callback to invoke.
   */
  private safeCall(fn: () => void): void {
    try {
      fn();
    } catch (err) {
      if (this.logErrors) {
        console.error('[EventBus] Listener error:', err);
      }

      if (this.onError) {
        try {
          this.onError(err);
        } catch (handlerErr) {
          this.rethrowAsync(handlerErr);
        }
      } else {
        this.rethrowAsync(err);
      }
    }
  }

  /**
   * Determines whether a sticky event should be consumed based on an explicit override
   * or its mode.
   *
   * @param override - Explicit boolean flag, or `undefined` to use the mode.
   * @param mode - The sticky mode of the event.
   * @returns `true` if the event should be consumed.
   */
  private shouldConsumeSticky(override: boolean | undefined, mode: StickyMode): boolean {
    return override ?? mode === 'consume';
  }
}
