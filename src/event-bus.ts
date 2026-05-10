import { clearCompileCache, compile, type Matcher } from '@dafengzhen/regex-derivative';

import type {
  CompiledPatternListenerEntry,
  EmitOptions,
  EventBusOptions,
  EventMap,
  Listener,
  ListenerContext,
  MatchedPattern,
  MiddlewareAsync,
  MiddlewareContext,
  MiddlewareEntry,
  MiddlewareSync,
  Off,
  OnOptions,
  PatternHandler,
  ReplayOneResult,
  StickyEvent,
  StickyMode,
  StoredExactListenerEntry,
  UseOptions,
} from './types.ts';

import { DispatcherRuntime } from './dispatcher-runtime.ts';
import { EventScope } from './event-scope.ts';

/**
 * Sentinel value returned when no sticky event is found for replay.
 * Frozen to prevent mutation.
 */
const NO_REPLAY: ReplayOneResult = { found: false };

/**
 * Frozen empty metadata object shared across replay contexts
 * to avoid unnecessary allocations.
 */
const EMPTY_META = Object.freeze({}) as Readonly<Record<string, unknown>>;

/**
 * A typed event bus providing dispatch, middleware, pattern matching,
 * sticky events, and scoped listener management.
 *
 * @typeParam E - The event map type mapping event keys to their payload types.
 *
 * @example
 * ```typescript
 * interface MyEvents {
 *   'user:login': { userId: string };
 *   'user:logout': { userId: string };
 * }
 *
 * const bus = new EventBus<MyEvents>();
 * bus.on('user:login', (payload, ctx) => {
 *   console.log(`User ${payload.userId} logged in`);
 * });
 * bus.emit('user:login', { userId: '123' });
 * ```
 *
 * @author dafengzhen
 */
export class EventBus<E extends EventMap> {
  /**
   * Optional error handler invoked when listener or middleware errors occur.
   * If not provided, errors are re-thrown asynchronously via `queueMicrotask`.
   */
  readonly onError?: ((e: unknown) => void) | undefined;

  /**
   * The dispatcher runtime managing scope-aware execution context.
   */
  readonly runtime: DispatcherRuntime<E>;

  /**
   * Whether to clear the global DFA compilation cache on disposal.
   */
  private readonly clearGlobalCacheOnDispose: boolean;

  /**
   * Cache of compiled DFA matchers keyed by pattern string.
   * Avoids recompilation of identical patterns.
   */
  private readonly dfaCache = new Map<string, Matcher>();

  /**
   * Flag indicating whether this bus instance has been disposed.
   * Once disposed, all operations throw an error.
   */
  private disposed = false;

  /**
   * Monotonically increasing sequence number for emit operations.
   * Used to assign unique IDs to emitted events.
   */
  private emitSequence = 0;

  /**
   * Map of exact event key listeners, keyed by event name.
   * Each value is a priority-sorted array of listener entries.
   */
  private readonly exactListeners = new Map<keyof E, Array<StoredExactListenerEntry<E>>>();

  /**
   * Monotonically increasing sequence number for listener registration.
   * Used to maintain insertion order for listeners with equal priority.
   */
  private listenerSequence = 0;

  /**
   * Whether to log errors to console.error in addition to calling onError.
   */
  private readonly logErrors: boolean;

  /**
   * Registered middleware entries in registration order.
   * Middleware are executed sequentially before listener dispatch.
   */
  private middlewares: Array<MiddlewareEntry<E>> = [];

  /**
   * Registered pattern listeners sorted by priority (high to low) and sequence.
   */
  private patternListeners: Array<CompiledPatternListenerEntry<E>> = [];

  /**
   * Sticky events keyed by exact event string (for pattern replay).
   * Each key maps to an array of sticky event payloads.
   */
  private stickyEvents = new Map<string, StickyEvent[]>();

  /**
   * Sticky events keyed by exact event key (for exact listener replay).
   * Each key maps to an array of sticky event payloads.
   */
  private stickyExact = new Map<keyof E, StickyEvent[]>();

  /**
   * Maximum number of sticky events retained per exact event key.
   */
  private readonly stickyExactMax: number;

  /**
   * Maximum number of sticky event keys retained globally.
   */
  private readonly stickyMax: number;

  /**
   * Maximum number of sticky events retained per pattern key.
   */
  private readonly stickyPatternMaxPerKey: number;

  /**
   * Creates a new EventBus instance.
   *
   * @param options - Configuration options for the event bus.
   * @param options.onError - Optional callback invoked when listener/middleware errors occur.
   * @param options.logErrors - Whether to log errors to console.error. Defaults to `true`.
   * @param options.clearGlobalCacheOnDispose - Whether to clear the global DFA cache on disposal. Defaults to `false`.
   * @param options.runtime - Custom dispatcher runtime for scope management. Defaults to a new `DispatcherRuntime`.
   * @param options.stickyMax - Maximum number of sticky event keys retained globally. Defaults to `200`.
   * @param options.stickyExactMax - Maximum sticky events retained per exact key. Defaults to `1`.
   * @param options.stickyPatternMaxPerKey - Maximum sticky events retained per pattern key. Defaults to `stickyMax`.
   */
  constructor(options?: EventBusOptions<E>) {
    this.onError = options?.onError;
    this.logErrors = options?.logErrors ?? true;
    this.clearGlobalCacheOnDispose = options?.clearGlobalCacheOnDispose ?? false;
    this.runtime = options?.runtime ?? new DispatcherRuntime<E>();
    this.stickyMax = this.normalizeLimit(options?.stickyMax ?? 200);
    this.stickyExactMax = this.normalizeLimit(options?.stickyExactMax ?? 1);
    this.stickyPatternMaxPerKey = this.normalizeLimit(
      options?.stickyPatternMaxPerKey ?? this.stickyMax,
    );
  }

  /**
   * Removes all listeners, middleware, and sticky events from the bus.
   * The bus remains usable after this call.
   */
  clearAll(): void {
    this.clearListeners();
    this.middlewares = [];
    this.stickyExact.clear();
    this.stickyEvents.clear();
  }

  /**
   * Removes all registered listeners (both exact and pattern-based).
   * Middleware and sticky events are preserved.
   */
  clearListeners(): void {
    this.exactListeners.clear();
    this.patternListeners = [];
  }

  /**
   * Creates a new EventScope that is a child of the provided parent scope
   * (or the current active scope if no parent is specified).
   *
   * Listeners registered within this scope are automatically cleaned up
   * when the scope is disposed.
   *
   * @param parent - Optional parent scope. If omitted, the current runtime scope is used.
   * @returns A new EventScope instance.
   * @throws If the bus has been disposed.
   */
  createScope(parent?: EventScope<E>): EventScope<E> {
    this.assertNotDisposed();
    return new EventScope<E>(this, parent);
  }

  /**
   * Disposes the event bus, releasing all resources.
   *
   * This removes all listeners, middleware, sticky events, clears the DFA cache,
   * and optionally clears the global compilation cache. After disposal, all
   * operations on this instance will throw an error.
   */
  dispose(): void {
    if (this.disposed) {
      return;
    }
    this.clearAll();
    this.dfaCache.clear();
    if (this.clearGlobalCacheOnDispose) {
      clearCompileCache();
    }
    this.disposed = true;
  }

  /**
   * Synchronously emits an event to all matching listeners and middleware.
   *
   * @typeParam K - The event key type (inferred from the `event` argument).
   * @param event - The event key to emit.
   * @param payload - The payload to pass to listeners and middleware.
   * @param options - Optional emit configuration.
   * @param options.sticky - If `true`, the event is stored for replay to future listeners.
   * @param options.stickyMode - Controls whether the event is consumed or replayed. Defaults to `'replay'`.
   * @param options.metaPatch - Additional metadata merged into the context's `meta` property.
   * @param options.origin - Optional origin identifier for the event.
   * @throws If the bus has been disposed.
   * @throws If an async middleware is encountered during synchronous emission.
   */
  emit<K extends keyof E>(event: K, payload: E[K], options?: EmitOptions): void {
    this.assertNotDisposed();
    const result = this.emitEvent(event, payload, options, false);
    if (this.isPromiseLike(result)) {
      throw new Error(
        '[EventBus] Async middleware detected in sync emit(). Use emitAsync() instead.',
      );
    }
  }

  /**
   * Asynchronously emits an event to all matching listeners and middleware.
   * Supports both sync and async middleware.
   *
   * @typeParam K - The event key type (inferred from the `event` argument).
   * @param event - The event key to emit.
   * @param payload - The payload to pass to listeners and middleware.
   * @param options - Optional emit configuration. See {@link emit} for details.
   * @returns A promise that resolves when all middleware and listeners have completed.
   * @throws If the bus has been disposed.
   */
  async emitAsync<K extends keyof E>(
    event: K,
    payload: E[K],
    options?: EmitOptions,
  ): Promise<void> {
    this.assertNotDisposed();
    await this.emitEvent(event, payload, options, true);
  }

  /**
   * Removes a previously registered exact listener for the given event key.
   *
   * @typeParam K - The event key type.
   * @param event - The event key from which to remove the listener.
   * @param listener - The listener function to remove.
   * @throws If the bus has been disposed.
   */
  off<K extends keyof E>(event: K, listener: Listener<E[K], E, K>): void {
    this.assertNotDisposed();
    this.removeExactListener(event, listener as Listener<any, E, any>);
  }

  /**
   * Registers a listener for the given event key.
   *
   * @typeParam K - The event key type.
   * @param event - The event key to listen for.
   * @param listener - The listener function invoked when the event is emitted.
   * @param options - Optional registration configuration.
   * @param options.priority - Listener priority. Higher values execute first. Defaults to `0`.
   * @param options.consumeSticky - Override for sticky event consumption behavior.
   * @returns An `Off` function that removes this listener when called.
   * @throws If the bus has been disposed.
   */
  on<K extends keyof E>(event: K, listener: Listener<E[K], E, K>, options?: OnOptions): Off {
    this.assertNotDisposed();
    return this.bindOffToCurrentScope(this.registerExactListener(false, event, listener, options));
  }

  /**
   * Registers a one-time listener for the given event key.
   * The listener is automatically removed after its first invocation.
   *
   * @typeParam K - The event key type.
   * @param event - The event key to listen for.
   * @param listener - The listener function invoked once when the event is emitted.
   * @param options - Optional registration configuration. See {@link on} for details.
   * @returns An `Off` function that removes this listener when called.
   * @throws If the bus has been disposed.
   */
  once<K extends keyof E>(event: K, listener: Listener<E[K], E, K>, options?: OnOptions): Off {
    this.assertNotDisposed();
    return this.bindOffToCurrentScope(this.registerExactListener(true, event, listener, options));
  }

  /**
   * Registers a one-time pattern-matching listener.
   * The listener is removed after its first match.
   *
   * @param pattern - A string pattern (compiled to DFA) or RegExp to match against event keys.
   * @param handler - The handler invoked when a matching event is emitted.
   * @param options - Optional registration configuration. See {@link on} for details.
   * @returns An `Off` function that removes this pattern handler when called.
   * @throws If the bus has been disposed.
   */
  onceMatch(pattern: RegExp | string, handler: PatternHandler<E>, options?: OnOptions): Off {
    this.assertNotDisposed();
    return this.bindOffToCurrentScope(
      this.registerPatternListener(true, pattern, handler, options),
    );
  }

  /**
   * Registers a pattern-matching listener that fires for all matching events.
   *
   * @param pattern - A string pattern (compiled to DFA) or RegExp to match against event keys.
   * @param handler - The handler invoked when a matching event is emitted.
   * @param options - Optional registration configuration. See {@link on} for details.
   * @returns An `Off` function that removes this pattern handler when called.
   * @throws If the bus has been disposed.
   */
  onMatch(pattern: RegExp | string, handler: PatternHandler<E>, options?: OnOptions): Off {
    this.assertNotDisposed();
    return this.bindOffToCurrentScope(
      this.registerPatternListener(false, pattern, handler, options),
    );
  }

  /**
   * Registers a synchronous middleware function.
   * Middleware execute before listeners and can stop propagation via `ctx.stop()` or `ctx.cancel()`.
   *
   * @param mw - The synchronous middleware function.
   * @param options - Optional middleware configuration.
   * @param options.pattern - A string pattern to conditionally apply this middleware.
   * @param options.match - A custom match function to conditionally apply this middleware.
   * @returns An `Off` function that removes this middleware when called.
   * @throws If the bus has been disposed.
   * @throws If the middleware returns a Promise (use `useAsync` instead).
   */
  use(mw: MiddlewareSync<E>, options?: UseOptions<E>): Off {
    this.assertNotDisposed();
    const entry: MiddlewareEntry<E> = {
      fn: ((ctx: MiddlewareContext<E>, next: () => void) => {
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
    return this.registerMiddlewareEntry(entry);
  }

  /**
   * Registers an asynchronous middleware function.
   * Async middleware must call `await next()` to continue the chain.
   *
   * @param mw - The asynchronous middleware function.
   * @param options - Optional middleware configuration. See {@link use} for details.
   * @returns An `Off` function that removes this middleware when called.
   * @throws If the bus has been disposed.
   */
  useAsync(mw: MiddlewareAsync<E>, options?: UseOptions<E>): Off {
    this.assertNotDisposed();
    const entry: MiddlewareEntry<E> = {
      fn: mw,
      isAsync: true,
      match: this.buildMiddlewareMatch(options),
    };
    return this.registerMiddlewareEntry(entry);
  }

  /**
   * Executes a function within a new scope, automatically cleaning up
   * any listeners registered during execution.
   *
   * @typeParam T - The return type of the function.
   * @param fn - The function to execute within the scope.
   * @param options - Optional configuration.
   * @param options.parent - Optional parent scope. If omitted, the current runtime scope is used.
   * @returns A promise resolving to the function's return value.
   * @throws If the bus has been disposed.
   */
  async withScope<T>(
    fn: (scope: EventScope<E>) => Promise<T> | T,
    options?: { parent?: EventScope<E> | undefined },
  ): Promise<T> {
    this.assertNotDisposed();
    const parent = options?.parent ?? this.runtime.getScope();
    const scope = this.createScope(parent);
    try {
      return await this.runtime.runWithScopeAsync(scope, () => fn(scope));
    } finally {
      scope.dispose();
    }
  }

  /**
   * Asserts that the bus has not been disposed, throwing an error if it has.
   *
   * @throws If the bus instance has been disposed.
   */
  private assertNotDisposed(): void {
    if (this.disposed) {
      throw new Error('EventBus instance has been disposed.');
    }
  }

  /**
   * Binds an `Off` function to the current active scope, if any.
   * When the scope is disposed, the `Off` function is automatically called.
   *
   * @param off - The `Off` function to bind.
   * @returns The original `Off` function (possibly wrapped by the scope).
   */
  private bindOffToCurrentScope(off: Off): Off {
    const scope = this.runtime.getScope();
    if (scope) {
      scope.registerOff(off);
    }
    return off;
  }

  /**
   * Builds a middleware match function from options.
   * Combines pattern-based DFA matching with a custom match function if both are provided.
   *
   * @param options - Middleware registration options.
   * @returns A match function or `undefined` if no filtering is configured.
   */
  private buildMiddlewareMatch(options?: UseOptions<E>): MiddlewareEntry<E>['match'] {
    const hasPattern = options?.pattern !== undefined;
    const customMatch = options?.match;
    if (!hasPattern && !customMatch) {
      return undefined;
    }

    const dfa = hasPattern ? this.getOrCompileDfa(options.pattern as string) : undefined;

    return (ctx: MiddlewareContext<E>) => {
      if (dfa && (typeof ctx.event !== 'string' || !dfa.match(ctx.event))) {
        return false;
      }
      return customMatch ? customMatch(ctx) : true;
    };
  }

  /**
   * Creates a compiled pattern listener entry from a pattern and handler.
   *
   * @param once - Whether this is a one-time listener.
   * @param pattern - The string pattern or RegExp to match event keys.
   * @param handler - The handler function to invoke on match.
   * @param options - Optional registration configuration.
   * @returns A compiled pattern listener entry.
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
        seq: ++this.listenerSequence,
      };
    }

    const regex = new RegExp(pattern.source, pattern.flags);
    return {
      handler,
      isNativeRegExp: true,
      match: (event: string) => {
        regex.lastIndex = 0;
        const result = regex.exec(event);
        regex.lastIndex = 0;
        if (!result) {
          return null;
        }
        return result.groups ? { ...result.groups } : {};
      },
      once,
      pattern: pattern.toString(),
      priority: options?.priority ?? 80,
      seq: ++this.listenerSequence,
    };
  }

  /**
   * Compares two listener entries for priority ordering.
   * Higher priority values are ordered first. Equal priorities use sequence ascending.
   *
   * @param a - First entry.
   * @param b - Second entry.
   * @returns Negative if `a` sorts before `b`, positive if `a` sorts after `b`, zero if equal.
   */
  private compareListenerPriority(
    a: { priority: number; seq: number },
    b: { priority: number; seq: number },
  ): number {
    if (b.priority !== a.priority) {
      return b.priority - a.priority;
    }
    return a.seq - b.seq;
  }

  /**
   * Creates a disposable `Off` function that invokes the cleanup function at most once.
   *
   * @param cleanupFn - The function to invoke on disposal.
   * @returns An `Off` function.
   */
  private createDisposableOff(cleanupFn: () => void): Off {
    let executed = false;
    return () => {
      if (executed) {
        return;
      }
      executed = true;
      cleanupFn();
    };
  }

  /**
   * Creates a frozen listener context object with reactive getters for lifecycle state.
   *
   * @typeParam K - The event key type.
   * @param baseData - The base context data (event, id, payload, timestamp, origin).
   * @param isCanceled - Getter function for canceled state.
   * @param isImmediateStopped - Getter function for immediate stop state.
   * @param isStopped - Getter function for stopped state.
   * @param getOrCreateMeta - Lazy initializer for the meta object.
   * @param phase - The current dispatch phase.
   * @returns A frozen listener context.
   */
  private createListenerContext<K extends keyof E>(
    baseData: Readonly<{
      event: K;
      id: number;
      origin?: string | undefined;
      payload: E[K];
      timestamp: number;
    }>,
    isCanceled: () => boolean,
    isImmediateStopped: () => boolean,
    isStopped: () => boolean,
    getOrCreateMeta: () => Record<string, unknown>,
    phase: 'exact' | 'pattern',
  ): ListenerContext<E, K> {
    return Object.freeze({
      ...baseData,
      get isCanceled() {
        return isCanceled();
      },
      get isImmediateStopped() {
        return isImmediateStopped();
      },
      get isStopped() {
        return isStopped();
      },
      get meta() {
        return getOrCreateMeta();
      },
      phase,
    } as ListenerContext<E, K>);
  }

  /**
   * Creates a lightweight frozen context for replaying sticky events.
   * Replay contexts always have `id: 0`, `timestamp: 0`, and frozen empty meta.
   *
   * @typeParam K - The event key type.
   * @param event - The event key.
   * @param payload - The event payload.
   * @param phase - The dispatch phase ('exact' or 'pattern').
   * @returns A frozen replay listener context.
   */
  private createReplayContext<K extends keyof E>(
    event: K,
    payload: E[K],
    phase: 'exact' | 'pattern',
  ): ListenerContext<E, K> {
    return Object.freeze({
      event,
      id: 0,
      isCanceled: false,
      isImmediateStopped: false,
      isStopped: false,
      meta: EMPTY_META,
      origin: undefined,
      payload,
      phase,
      timestamp: 0,
    } as ListenerContext<E, K>);
  }

  /**
   * Dispatches exact and pattern listeners after middleware execution completes.
   * If the middleware context is stopped or canceled, no listeners are invoked.
   *
   * @typeParam K - The event key type.
   * @param middlewareCtx - The middleware context (checked for stop/cancel state).
   * @param matchedPatterns - The matched pattern entries for this event.
   * @param createListenerContext - Factory function to create listener contexts.
   */
  private dispatchListeners<K extends keyof E>(
    middlewareCtx: MiddlewareContext<E, K>,
    matchedPatterns: MatchedPattern<E>[],
    createListenerContext: (phase: 'exact' | 'pattern') => ListenerContext<E, K>,
  ): void {
    if (!middlewareCtx.isStopped && !middlewareCtx.isCanceled) {
      this.invokeExactListeners(middlewareCtx.event, middlewareCtx.payload, createListenerContext);
    }

    if (!middlewareCtx.isStopped && !middlewareCtx.isCanceled) {
      for (const { entry, match } of matchedPatterns) {
        if (!this.patternListeners.includes(entry)) {
          continue;
        }

        const listenerCtx = createListenerContext('pattern');

        if (listenerCtx.isImmediateStopped || listenerCtx.isStopped || listenerCtx.isCanceled) {
          return;
        }

        if (entry.once) {
          this.removePatternEntry(entry);
        }

        this.safeCall(() =>
          entry.handler(middlewareCtx.event as string, middlewareCtx.payload, match, listenerCtx),
        );
      }
    }
  }

  /**
   * Core event emission logic shared by sync and async emit methods.
   *
   * Handles sticky event storage, pattern matching, context creation,
   * and middleware execution.
   *
   * @typeParam K - The event key type.
   * @param event - The event key to emit.
   * @param payload - The event payload.
   * @param options - Optional emit configuration.
   * @param allowAsyncMiddleware - Whether async middleware is permitted.
   * @returns A Promise if async middleware is encountered, otherwise void.
   */
  private emitEvent<K extends keyof E>(
    event: K,
    payload: E[K],
    options: EmitOptions | undefined,
    allowAsyncMiddleware: boolean,
  ): Promise<void> | void {
    if (options?.sticky) {
      const mode: StickyMode = options.stickyMode ?? 'replay';
      this.pushStickyExact(event, payload, mode);
      if (typeof event === 'string') {
        this.pushStickyEvent(event, payload, mode);
      }
    }

    const matchedPatterns: MatchedPattern<E>[] =
      typeof event === 'string' ? this.matchPatternListeners(event) : [];

    let stopped = false;
    let immediateStopped = false;
    let canceled = false;

    let meta: Record<string, unknown> | undefined;
    const getOrCreateMeta = (): Record<string, unknown> => {
      meta ??= options?.metaPatch ? { ...options.metaPatch } : {};
      return meta;
    };

    const emitId = ++this.emitSequence;
    const timestamp = Date.now();

    const baseData = Object.freeze({
      event,
      id: emitId,
      origin: options?.origin,
      payload,
      timestamp,
    } as const);

    const isStopped = () => stopped;
    const isImmediateStopped = () => immediateStopped;
    const isCanceled = () => canceled;

    const middlewareContext: MiddlewareContext<E, K> = {
      ...baseData,
      cancel() {
        canceled = true;
      },
      get isCanceled() {
        return canceled;
      },
      get isImmediateStopped() {
        return immediateStopped;
      },
      get isStopped() {
        return stopped;
      },
      get meta() {
        return getOrCreateMeta();
      },
      phase: 'middleware',
      stop() {
        stopped = true;
      },
      stopImmediate() {
        stopped = true;
        immediateStopped = true;
      },
    };

    const createListenerCtx = (phase: 'exact' | 'pattern'): ListenerContext<E, K> =>
      this.createListenerContext(
        baseData,
        isCanceled,
        isImmediateStopped,
        isStopped,
        getOrCreateMeta,
        phase,
      );

    return this.executeMiddlewares(
      middlewareContext,
      matchedPatterns,
      createListenerCtx,
      allowAsyncMiddleware,
    );
  }

  /**
   * Executes the middleware chain sequentially, then dispatches listeners.
   *
   * @typeParam K - The event key type.
   * @param middlewareCtx - The middleware context.
   * @param rawMatches - Matched pattern entries.
   * @param createListenerContext - Factory for listener contexts.
   * @param allowAsyncMiddleware - Whether async middleware is allowed.
   * @returns A Promise if async middleware is present, otherwise void.
   */
  private executeMiddlewares<K extends keyof E>(
    middlewareCtx: MiddlewareContext<E, K>,
    rawMatches: MatchedPattern<E>[],
    createListenerContext: (phase: 'exact' | 'pattern') => ListenerContext<E, K>,
    allowAsyncMiddleware: boolean,
  ): Promise<void> | void {
    const middlewareList = this.middlewares.slice();
    let index = 0;

    const processNext = (): Promise<void> | void => {
      if (middlewareCtx.isStopped || middlewareCtx.isCanceled) {
        return;
      }

      while (index < middlewareList.length) {
        const entry = middlewareList[index++]!;
        try {
          if (entry.match && !entry.match(middlewareCtx)) {
            continue;
          }
        } catch (err) {
          this.handleMiddlewareError(err);
          throw err;
        }
        if (!allowAsyncMiddleware && entry.isAsync) {
          throw new Error(
            '[EventBus] Async middleware detected in sync emit(). Use emitAsync() instead.',
          );
        }

        return this.invokeMiddleware(entry, middlewareCtx, processNext);
      }

      this.dispatchListeners(middlewareCtx, rawMatches, createListenerContext);
    };

    return processNext();
  }

  /**
   * Retrieves or compiles a DFA matcher for the given pattern string.
   * Compiled matchers are cached for reuse.
   *
   * @param pattern - The pattern string to compile.
   * @returns A compiled DFA Matcher.
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
   * Handles an error thrown by a listener.
   * Logs the error if `logErrors` is enabled, and calls `onError` if configured.
   *
   * @param err - The error thrown by the listener.
   */
  private handleListenerError(err: unknown): void {
    if (this.logErrors) {
      console.error('[EventBus] Listener error:', err);
    }
    if (this.onError) {
      try {
        this.onError(err);
      } catch (handlerErr) {
        this.scheduleAsyncError(handlerErr);
      }
    } else {
      this.scheduleAsyncError(err);
    }
  }

  /**
   * Handles an error thrown by middleware.
   * Logs the error if `logErrors` is enabled, and calls `onError` if configured.
   *
   * @param err - The error thrown by the middleware.
   */
  private handleMiddlewareError(err: unknown): void {
    if (this.logErrors) {
      console.error('[EventBus] Middleware error:', err);
    }
    if (this.onError) {
      try {
        this.onError(err);
      } catch (handlerErr) {
        this.scheduleAsyncError(handlerErr);
      }
    }
  }

  /**
   * Inserts an exact listener entry into the appropriate priority-sorted bucket.
   *
   * @typeParam K - The event key type.
   * @param event - The event key.
   * @param entry - The listener entry to insert.
   */
  private insertExactListenerEntry<K extends keyof E>(
    event: K,
    entry: StoredExactListenerEntry<E>,
  ): void {
    let entries = this.exactListeners.get(event);
    if (!entries) {
      entries = [];
      this.exactListeners.set(event, entries);
    }
    this.insertSortedByPriority(entries, entry);
  }

  /**
   * Inserts an entry into a sorted array using binary search based on priority and sequence.
   * Higher priority inserts earlier. Equal priority orders by sequence ascending.
   *
   * @typeParam T - The entry type (must have `priority` and `seq` properties).
   * @param bucket - The sorted array to insert into.
   * @param entry - The entry to insert.
   */
  private insertSortedByPriority<T extends { priority: number; seq: number }>(
    bucket: T[],
    entry: T,
  ): void {
    let lo = 0;
    let hi = bucket.length;
    while (lo < hi) {
      const mid = (lo + hi) >>> 1;
      if (this.compareListenerPriority(entry, bucket[mid]) < 0) {
        hi = mid;
      } else {
        lo = mid + 1;
      }
    }
    bucket.splice(lo, 0, entry);
  }

  /**
   * Invokes all exact listeners for the given event key.
   *
   * @typeParam K - The event key type.
   * @param event - The event key.
   * @param payload - The event payload.
   * @param createListenerContext - Factory to create listener contexts.
   */
  private invokeExactListeners<K extends keyof E>(
    event: K,
    payload: E[K],
    createListenerContext: (phase: 'exact' | 'pattern') => ListenerContext<E, K>,
  ): void {
    const entries = this.exactListeners.get(event);
    if (!entries || entries.length === 0) {
      return;
    }

    for (const { listener } of entries.slice()) {
      const listenerCtx = createListenerContext('exact');

      if (listenerCtx.isImmediateStopped || listenerCtx.isStopped || listenerCtx.isCanceled) {
        return;
      }

      this.safeCall(() => (listener as Listener<E[K], E, K>)(payload, listenerCtx));
    }
  }

  /**
   * Invokes a single middleware entry, handling both sync and async middleware,
   * and enforcing that `next()` is called exactly once.
   *
   * @param entry - The middleware entry to invoke.
   * @param ctx - The middleware context.
   * @param next - The callback to continue to the next middleware or dispatch.
   * @returns A Promise if async, otherwise void.
   * @throws If `next()` is not called or is called multiple times.
   */
  private invokeMiddleware(
    entry: MiddlewareEntry<E>,
    ctx: MiddlewareContext<E>,
    next: () => Promise<void> | void,
  ): Promise<void> | void {
    let nextCalled = false;
    let nextResult: Promise<void> | void = undefined;

    const assertNextCalled = () => {
      if (!nextCalled && !ctx.isStopped && !ctx.isCanceled) {
        throw new Error(
          'Middleware: next() was not called. Call next() to continue, or ctx.stop()/ctx.cancel() to stop dispatch.',
        );
      }
    };

    const handleNext = () => {
      if (nextCalled) {
        throw new Error('Middleware: next() called multiple times.');
      }
      nextCalled = true;
      nextResult = next();
      return entry.isAsync ? Promise.resolve(nextResult) : nextResult;
    };

    let middlewareResult: Promise<void> | void;
    try {
      middlewareResult = (entry.fn as any)(ctx, handleNext);
    } catch (err) {
      this.handleMiddlewareError(err);
      throw err;
    }

    if (this.isPromiseLike(middlewareResult)) {
      return Promise.resolve(middlewareResult)
        .then(() => {
          assertNextCalled();
          return this.isPromiseLike(nextResult) ? nextResult : undefined;
        })
        .catch((err) => {
          this.handleMiddlewareError(err);
          throw err;
        });
    }

    assertNextCalled();

    if (this.isPromiseLike(nextResult)) {
      return Promise.resolve(nextResult).catch((err) => {
        this.handleMiddlewareError(err);
        throw err;
      }) as Promise<void>;
    }
  }

  /**
   * Type guard checking whether a value is Promise-like (has a `.then` method).
   *
   * @typeParam T - The resolved type of the potential promise.
   * @param value - The value to check.
   * @returns `true` if the value is Promise-like, `false` otherwise.
   */
  private isPromiseLike<T = unknown>(value: unknown): value is PromiseLike<T> {
    return (
      value !== null &&
      (typeof value === 'object' || typeof value === 'function') &&
      typeof (value as { then?: unknown }).then === 'function'
    );
  }

  /**
   * Matches an event string against all registered pattern listeners.
   *
   * @param event - The event string to match.
   * @returns An array of matched pattern entries with their match params.
   */
  private matchPatternListeners(event: string): MatchedPattern<E>[] {
    const matches: MatchedPattern<E>[] = [];
    for (const entry of this.patternListeners) {
      const params = entry.match(event);
      if (params) {
        matches.push({ entry, match: params });
      }
    }
    return matches;
  }

  /**
   * Normalizes a limit value to a non-negative finite integer.
   *
   * @param value - The raw limit value.
   * @returns A normalized non-negative integer.
   */
  private normalizeLimit(value: number): number {
    return Number.isFinite(value) ? Math.max(0, Math.floor(value)) : 0;
  }

  /**
   * Stores a sticky event for pattern replay.
   * Respects `stickyMax` and `stickyPatternMaxPerKey` limits by trimming oldest entries.
   *
   * @param eventKey - The event string key.
   * @param payload - The event payload.
   * @param mode - The sticky mode ('replay' or 'consume').
   */
  private pushStickyEvent(eventKey: string, payload: unknown, mode: StickyMode): void {
    if (this.stickyMax <= 0 || this.stickyPatternMaxPerKey <= 0) {
      return;
    }

    let stickyBatch = this.stickyEvents.get(eventKey);
    if (!stickyBatch) {
      stickyBatch = [];
      this.stickyEvents.set(eventKey, stickyBatch);
    }
    stickyBatch.push({ mode, payload });

    const overflow = stickyBatch.length - this.stickyPatternMaxPerKey;
    if (overflow > 0) {
      stickyBatch.splice(0, overflow);
    }

    while (this.stickyEvents.size > this.stickyMax) {
      const oldestKey = this.stickyEvents.keys().next().value as string | undefined;
      if (oldestKey === undefined) {
        break;
      }
      this.stickyEvents.delete(oldestKey);
    }
  }

  /**
   * Stores a sticky event for exact listener replay.
   * Respects `stickyExactMax` limit by trimming oldest entries.
   *
   * @typeParam K - The event key type.
   * @param event - The exact event key.
   * @param payload - The event payload.
   * @param mode - The sticky mode ('replay' or 'consume').
   */
  private pushStickyExact<K extends keyof E>(event: K, payload: unknown, mode: StickyMode): void {
    if (this.stickyExactMax <= 0) {
      return;
    }

    const queue = this.stickyExact.get(event) ?? [];
    queue.push({ mode, payload });

    const overflow = queue.length - this.stickyExactMax;
    if (overflow > 0) {
      queue.splice(0, overflow);
    }
    this.stickyExact.set(event, queue);
  }

  /**
   * Registers an exact listener, handling sticky replay and priority ordering.
   *
   * @typeParam K - The event key type.
   * @param once - Whether this is a one-time listener.
   * @param event - The event key.
   * @param listener - The listener function.
   * @param options - Optional registration configuration.
   * @returns An `Off` function to remove this listener.
   */
  private registerExactListener<K extends keyof E>(
    once: boolean,
    event: K,
    listener: Listener<E[K], E, K>,
    options?: OnOptions,
  ): Off {
    const consumeStickyOverride = options?.consumeSticky;
    const priority = options?.priority ?? 0;
    const seq = ++this.listenerSequence;
    let registeredListener: Listener<E[K], E, K>;

    if (once) {
      registeredListener = ((payload: E[K], ctx: ListenerContext<E, K>) => {
        this.removeExactListener(event, registeredListener as Listener<any, E, any>);
        return listener(payload, ctx);
      }) as Listener<E[K], E, K>;
    } else {
      registeredListener = listener;
    }

    this.insertExactListenerEntry(event, {
      listener: registeredListener,
      originalListener: listener as Listener<any, E, any>,
      priority,
      seq,
    });

    if (once) {
      const replay = this.replayExactStickyOne(event, consumeStickyOverride);
      if (replay.found) {
        const replayPayload = replay.payload as E[K];
        const replayCtx = this.createReplayContext(event, replayPayload, 'exact');
        this.safeCall(() => registeredListener(replayPayload, replayCtx));
      }
    } else {
      for (const replayPayload of this.replayExactStickyAll(event, consumeStickyOverride)) {
        const payloadForEvent = replayPayload as E[K];
        const replayCtx = this.createReplayContext(event, payloadForEvent, 'exact');
        this.safeCall(() => registeredListener(payloadForEvent, replayCtx));
      }
    }

    return this.createDisposableOff(() =>
      this.removeExactListener(event, registeredListener as Listener<any, E, any>),
    );
  }

  /**
   * Registers a middleware entry and returns an `Off` function for removal.
   *
   * @param entry - The middleware entry to register.
   * @returns An `Off` function to remove this middleware.
   */
  private registerMiddlewareEntry(entry: MiddlewareEntry<E>): Off {
    this.middlewares.push(entry);
    return this.createDisposableOff(() => {
      const idx = this.middlewares.indexOf(entry);
      if (idx !== -1) {
        this.middlewares.splice(idx, 1);
      }
    });
  }

  /**
   * Registers a pattern listener, inserting it into the priority-sorted list
   * and replaying any matching sticky events.
   *
   * @param once - Whether this is a one-time listener.
   * @param pattern - The string pattern or RegExp.
   * @param handler - The pattern handler function.
   * @param options - Optional registration configuration.
   * @returns An `Off` function to remove this pattern listener.
   */
  private registerPatternListener(
    once: boolean,
    pattern: RegExp | string,
    handler: PatternHandler<E>,
    options?: OnOptions,
  ): Off {
    const entry = this.buildPatternEntry(once, pattern, handler, options);
    this.insertSortedByPriority(this.patternListeners, entry);
    this.replayStickyForEntry(entry, options?.consumeSticky);
    if (once && !this.patternListeners.includes(entry)) {
      return this.createDisposableOff(() => {});
    }
    return this.createDisposableOff(() => this.removePatternEntry(entry));
  }

  /**
   * Removes an exact listener matching the given listener function reference.
   *
   * @typeParam K - The event key type.
   * @param event - The event key.
   * @param listener - The listener function reference to remove.
   */
  private removeExactListener<K extends keyof E>(event: K, listener: Listener<any, E, any>): void {
    const entries = this.exactListeners.get(event);
    if (!entries) {
      return;
    }

    const index = entries.findIndex(
      (e) => e.listener === listener || e.originalListener === listener,
    );
    if (index === -1) {
      return;
    }

    entries.splice(index, 1);

    if (entries.length === 0) {
      this.exactListeners.delete(event);
    }
  }

  /**
   * Removes a compiled pattern listener entry from the registration list.
   *
   * @param entry - The pattern listener entry to remove.
   */
  private removePatternEntry(entry: CompiledPatternListenerEntry<E>): void {
    const idx = this.patternListeners.indexOf(entry);
    if (idx !== -1) {
      this.patternListeners.splice(idx, 1);
    }
  }

  /**
   * Deletes a sticky event key from the map if its value array is empty.
   *
   * @param eventKey - The event key to check and possibly delete.
   * @param stickyBatch - Optional pre-fetched batch array.
   */
  private removeStickyEventKeyIfEmpty(eventKey: string, stickyBatch?: StickyEvent[]): void {
    const batch = stickyBatch ?? this.stickyEvents.get(eventKey);
    if (!batch || batch.length === 0) {
      this.stickyEvents.delete(eventKey);
    }
  }

  /**
   * Replays all sticky events for an exact event key, consuming or preserving
   * based on the sticky mode.
   *
   * @typeParam K - The event key type.
   * @param event - The event key.
   * @param consumeOverride - Override for consumption behavior.
   * @returns An array of replay payloads.
   */
  private replayExactStickyAll<K extends keyof E>(
    event: K,
    consumeOverride: boolean | undefined,
  ): unknown[] {
    const queue = this.stickyExact.get(event);
    if (!queue || queue.length === 0) {
      return [];
    }

    const replayed: unknown[] = [];
    const remaining: StickyEvent[] = [];

    for (const item of queue) {
      replayed.push(item.payload);
      if (!this.shouldConsumeSticky(consumeOverride, item.mode)) {
        remaining.push(item);
      }
    }

    if (remaining.length === 0) {
      this.stickyExact.delete(event);
    } else if (remaining.length !== queue.length) {
      this.stickyExact.set(event, remaining);
    }

    return replayed;
  }

  /**
   * Replays the first sticky event for an exact key (used by once listeners).
   *
   * @typeParam K - The event key type.
   * @param event - The event key.
   * @param consumeOverride - Override for consumption behavior.
   * @returns A replay result with the payload if found.
   */
  private replayExactStickyOne<K extends keyof E>(
    event: K,
    consumeOverride: boolean | undefined,
  ): ReplayOneResult {
    const queue = this.stickyExact.get(event);
    if (!queue || queue.length === 0) {
      return NO_REPLAY;
    }

    const first = queue[0]!;
    if (this.shouldConsumeSticky(consumeOverride, first.mode)) {
      queue.shift();
      if (queue.length === 0) {
        this.stickyExact.delete(event);
      }
    }

    return { found: true, payload: first.payload };
  }

  /**
   * Replays matching sticky events for a newly registered pattern listener.
   *
   * @param entry - The compiled pattern listener entry.
   * @param consumeOverride - Override for consumption behavior.
   */
  private replayStickyForEntry(
    entry: CompiledPatternListenerEntry<E>,
    consumeOverride: boolean | undefined,
  ): void {
    for (const eventKey of Array.from(this.stickyEvents.keys())) {
      const stickyBatch = this.stickyEvents.get(eventKey);
      if (!stickyBatch || stickyBatch.length === 0) {
        continue;
      }

      const match = entry.match(eventKey);
      if (!match) {
        continue;
      }

      for (let i = 0; i < stickyBatch.length; i++) {
        const stickyItem = stickyBatch[i]!;

        const replayCtx = this.createReplayContext(
          eventKey as keyof E,
          stickyItem.payload as E[keyof E],
          'pattern',
        ) as ListenerContext<E>;

        this.safeCall(() => entry.handler(eventKey, stickyItem.payload as any, match, replayCtx));

        if (this.shouldConsumeSticky(consumeOverride, stickyItem.mode)) {
          stickyBatch.splice(i, 1);
          i--;
        }

        if (entry.once) {
          this.removePatternEntry(entry);
          this.removeStickyEventKeyIfEmpty(eventKey, stickyBatch);
          return;
        }
      }

      this.removeStickyEventKeyIfEmpty(eventKey, stickyBatch);
    }
  }

  /**
   * Safely invokes a function, catching synchronous errors and handling
   * rejected promises from async listener return values.
   *
   * @param fn - The function to invoke safely.
   */
  private safeCall(fn: () => unknown): void {
    try {
      const result = fn();
      if (this.isPromiseLike(result)) {
        Promise.resolve(result).catch((err) => this.handleListenerError(err));
      }
    } catch (err) {
      this.handleListenerError(err);
    }
  }

  /**
   * Schedules an error to be thrown asynchronously using the best available mechanism.
   * Tries `queueMicrotask`, falls back to `Promise.resolve().then()`, then `setTimeout`.
   *
   * @param err - The error to throw asynchronously.
   */
  private scheduleAsyncError(err: unknown): void {
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
   * Determines whether a sticky event should be consumed based on mode and override.
   *
   * @param override - Optional explicit override for consumption.
   * @param mode - The sticky event's mode.
   * @returns `true` if the event should be consumed.
   */
  private shouldConsumeSticky(override: boolean | undefined, mode: StickyMode): boolean {
    return override ?? mode === 'consume';
  }
}
