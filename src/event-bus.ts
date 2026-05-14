import {
  clearPatternMatcherCache,
  createPatternMatcher,
  type PatternMatcher,
} from '@dafengzhen/derivative-matcher';

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

/** Default replay result indicating no sticky event was found. */
const NO_REPLAY: ReplayOneResult = { found: false };

/** Frozen empty object used as default metadata for replay contexts. */
const EMPTY_META = Object.freeze({}) as Readonly<Record<string, unknown>>;

/** Cached empty array returned when no pattern listeners match. */
const NO_PATTERN_MATCHES: MatchedPattern<any>[] = [];

/** Symbol used to tag versioned matched pattern arrays for invalidation tracking. */
const MATCHED_PATTERN_VERSION = Symbol('EventBus.matchedPatternVersion');

/**
 * Extended array type that carries a version stamp used to detect stale
 * pattern listener snapshots during dispatch.
 */
type VersionedMatchedPatterns<E extends EventMap> = MatchedPattern<E>[] & {
  [MATCHED_PATTERN_VERSION]?: number;
};

/**
 * A type-safe, high-performance event bus with support for exact and
 * pattern-based listeners, middleware pipelines, sticky events, scoped
 * listener lifecycles, and asynchronous dispatch.
 *
 * @typeParam E - A mapping from event keys to their payload types.
 *
 * @example
 * ```ts
 * type MyEvents = {
 *   'user:login': { userId: string };
 *   'user:logout': { userId: string };
 * };
 *
 * const bus = new EventBus<MyEvents>({ logErrors: true });
 *
 * bus.on('user:login', (payload, ctx) => {
 *   console.log(`User ${payload.userId} logged in`);
 * });
 *
 * bus.emit('user:login', { userId: 'abc123' });
 * ```
 *
 * @author dafengzhen
 */
export class EventBus<E extends EventMap> {
  /**
   * Optional callback invoked when a listener or middleware throws an error.
   * If not provided, errors are re-thrown asynchronously via `queueMicrotask`
   * or `setTimeout` to avoid disrupting the dispatch loop.
   */
  readonly onError?: ((e: unknown) => void) | undefined;

  /**
   * The dispatcher runtime responsible for managing scoped execution contexts
   * and asynchronous dispatching.
   */
  readonly runtime: DispatcherRuntime<E>;

  /**
   * Whether to clear the global regex-derivative compile cache when the
   * EventBus is disposed.
   */
  private readonly clearGlobalCacheOnDispose: boolean;

  /** Internal cache of compiled DFA matchers keyed by pattern string. */
  private readonly dfaCache = new Map<string, PatternMatcher>();

  /** Whether the EventBus has been disposed and should no longer be used. */
  private disposed = false;

  /** Monotonically increasing sequence number for emit calls. */
  private emitSequence = 0;

  /** Map of exact event listeners keyed by event name. */
  private readonly exactListeners = new Map<keyof E, Array<StoredExactListenerEntry<E>>>();

  /** Monotonically increasing sequence number for listener registration order. */
  private listenerSequence = 0;

  /** Whether to log errors to the console before invoking onError. */
  private readonly logErrors: boolean;

  /** Ordered list of registered middleware entries. */
  private middlewares: Array<MiddlewareEntry<E>> = [];

  /** Ordered list of compiled pattern listener entries. */
  private patternListeners: Array<CompiledPatternListenerEntry<E>> = [];

  /** Version counter incremented whenever pattern listeners are added or removed. */
  private patternListenerVersion = 0;

  /** Map of sticky events for pattern-based replay, keyed by event string. */
  private stickyEvents = new Map<string, StickyEvent[]>();

  /** Map of sticky events for exact-match replay, keyed by event key. */
  private stickyExact = new Map<keyof E, StickyEvent[]>();

  /** Maximum number of sticky events retained per exact event key. */
  private readonly stickyExactMax: number;

  /** Maximum total number of sticky event keys retained for pattern replay. */
  private readonly stickyMax: number;

  /** Maximum sticky events retained per pattern-matched event key. */
  private readonly stickyPatternMaxPerKey: number;

  /**
   * Creates a new EventBus instance.
   *
   * @param options - Configuration options for the EventBus.
   * @param options.onError - Optional error handler for listener/middleware errors.
   * @param options.logErrors - Whether to log errors to console. Defaults to `true`.
   * @param options.clearGlobalCacheOnDispose - Whether to clear the global regex
   *   compile cache on disposal. Defaults to `false`.
   * @param options.runtime - Custom dispatcher runtime. Defaults to a new
   *   `DispatcherRuntime`.
   * @param options.stickyMax - Maximum number of sticky event keys for pattern
   *   replay. Defaults to `200`.
   * @param options.stickyExactMax - Maximum sticky events per exact event key.
   *   Defaults to `1`.
   * @param options.stickyPatternMaxPerKey - Maximum sticky events per pattern-
   *   matched key. Defaults to `stickyMax`.
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
   * Removes all listeners, middleware, and sticky events from the EventBus.
   * The instance remains usable after this call.
   */
  clearAll(): void {
    this.clearListeners();
    this.middlewares = [];
    this.stickyExact.clear();
    this.stickyEvents.clear();
  }

  /**
   * Removes all exact and pattern listeners. Middleware and sticky events
   * are preserved.
   */
  clearListeners(): void {
    this.exactListeners.clear();
    this.patternListeners = [];
    this.patternListenerVersion++;
  }

  /**
   * Creates a new {@link EventScope} that automatically unbinds listeners
   * registered within it when disposed.
   *
   * @param parent - Optional parent scope to nest within.
   * @returns A new EventScope instance.
   */
  createScope(parent?: EventScope<E>): EventScope<E> {
    this.assertNotDisposed();
    return new EventScope<E>(this, parent);
  }

  /**
   * Disposes the EventBus, clearing all listeners, middleware, sticky events,
   * and optionally the global regex compile cache. Once disposed, the instance
   * cannot be used again.
   */
  dispose(): void {
    if (this.disposed) {
      return;
    }
    this.clearAll();
    this.dfaCache.clear();
    if (this.clearGlobalCacheOnDispose) {
      clearPatternMatcherCache();
    }
    this.disposed = true;
  }

  /**
   * Synchronously emits an event to all registered listeners and middleware.
   *
   * @typeParam K - The event key type.
   * @param event - The event key to emit.
   * @param payload - The payload to pass to listeners.
   * @param options - Optional emit configuration.
   * @param options.sticky - Whether to store this event for sticky replay.
   * @param options.stickyMode - Sticky consumption mode (replay or consume).
   * @param options.origin - Origin marker passed to listener contexts.
   * @param options.metaPatch - Metadata merged into listener context meta.
   * @throws If async middleware is encountered. Use {@link emitAsync} instead.
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
   * Asynchronously emits an event, supporting both sync and async middleware.
   *
   * @typeParam K - The event key type.
   * @param event - The event key to emit.
   * @param payload - The payload to pass to listeners.
   * @param options - Optional emit configuration.
   * @returns A promise that resolves when all middleware and listeners have
   *   completed.
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
   * Unregisters an exact listener for the specified event.
   *
   * @typeParam K - The event key type.
   * @param event - The event key to unregister from.
   * @param listener - The listener function to remove.
   */
  off<K extends keyof E>(event: K, listener: Listener<E[K], E, K>): void {
    this.assertNotDisposed();
    this.removeExactListener(event, listener as Listener<any, E, any>);
  }

  /**
   * Registers a listener for the exact event key. The listener will be called
   * every time the event is emitted.
   *
   * @typeParam K - The event key type.
   * @param event - The event key to listen for.
   * @param listener - The listener function.
   * @param options - Optional registration configuration.
   * @param options.priority - Higher priority listeners execute first. Defaults to `0`.
   * @param options.consumeSticky - Whether to consume sticky events on replay.
   * @returns A function that unregisters the listener when called.
   */
  on<K extends keyof E>(event: K, listener: Listener<E[K], E, K>, options?: OnOptions): Off {
    this.assertNotDisposed();
    return this.bindOffToCurrentScope(this.registerExactListener(false, event, listener, options));
  }

  /**
   * Registers a one-time listener for the exact event key. The listener is
   * automatically removed after its first invocation.
   *
   * @typeParam K - The event key type.
   * @param event - The event key to listen for.
   * @param listener - The listener function.
   * @param options - Optional registration configuration.
   * @returns A function that unregisters the listener when called.
   */
  once<K extends keyof E>(event: K, listener: Listener<E[K], E, K>, options?: OnOptions): Off {
    this.assertNotDisposed();
    return this.bindOffToCurrentScope(this.registerExactListener(true, event, listener, options));
  }

  /**
   * Registers a one-time pattern-based listener. The handler is called when an
   * event whose string key matches the pattern is emitted, then automatically
   * removed.
   *
   * @param pattern - A RegExp or string pattern to match event keys against.
   * @param handler - The handler function, receiving the event string, payload,
   *   regex match groups, and listener context.
   * @param options - Optional registration configuration.
   * @returns A function that unregisters the handler when called.
   */
  onceMatch(pattern: RegExp | string, handler: PatternHandler<E>, options?: OnOptions): Off {
    this.assertNotDisposed();
    return this.bindOffToCurrentScope(
      this.registerPatternListener(true, pattern, handler, options),
    );
  }

  /**
   * Registers a pattern-based listener. The handler is called whenever an event
   * whose string key matches the pattern is emitted.
   *
   * @param pattern - A RegExp or string pattern to match event keys against.
   * @param handler - The handler function, receiving the event string, payload,
   *   regex match groups, and listener context.
   * @param options - Optional registration configuration.
   * @returns A function that unregisters the handler when called.
   */
  onMatch(pattern: RegExp | string, handler: PatternHandler<E>, options?: OnOptions): Off {
    this.assertNotDisposed();
    return this.bindOffToCurrentScope(
      this.registerPatternListener(false, pattern, handler, options),
    );
  }

  /**
   * Registers synchronous middleware that intercepts events before they reach
   * listeners.
   *
   * @param mw - The middleware function.
   * @param options - Optional middleware configuration.
   * @param options.pattern - Event key pattern to restrict when the middleware runs.
   * @param options.match - Custom predicate to determine if the middleware runs.
   * @returns A function that unregisters the middleware when called.
   * @throws If the middleware returns a Promise. Use {@link useAsync} instead.
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
   * Registers asynchronous middleware that intercepts events before they reach
   * listeners.
   *
   * @param mw - The async middleware function.
   * @param options - Optional middleware configuration.
   * @param options.pattern - Event key pattern to restrict when the middleware runs.
   * @param options.match - Custom predicate to determine if the middleware runs.
   * @returns A function that unregisters the middleware when called.
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
   * Executes a function within a temporary {@link EventScope}. All listeners
   * registered during the function's execution are automatically removed when
   * the scope is disposed.
   *
   * @typeParam T - The return type of the function.
   * @param fn - The function to execute within the scope.
   * @param options - Optional configuration.
   * @param options.parent - Optional parent scope to nest within.
   * @returns A promise resolving to the function's return value.
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
   * Throws if the EventBus has been disposed.
   * @throws If the instance is disposed.
   */
  private assertNotDisposed(): void {
    if (this.disposed) {
      throw new Error('EventBus instance has been disposed.');
    }
  }

  /**
   * Binds an unregister function to the current scope, if one exists.
   * This ensures listeners are cleaned up when the scope is disposed.
   *
   * @param off - The unregister function.
   * @returns The same unregister function.
   */
  private bindOffToCurrentScope(off: Off): Off {
    const scope = this.runtime.getScope();
    if (scope) {
      scope.registerOff(off);
    }
    return off;
  }

  /**
   * Builds a middleware match function from the given options.
   * Combines pattern-based and custom matching into a single predicate.
   *
   * @param options - The middleware use options.
   * @returns A match function, or `undefined` if no matching is specified.
   */
  private buildMiddlewareMatch(options?: UseOptions<E>): MiddlewareEntry<E>['match'] {
    const hasPattern = options?.pattern !== undefined;
    const customMatch = options?.match;
    if (!hasPattern && !customMatch) {
      return undefined;
    }

    const dfa = hasPattern ? this.getOrCompileDfa(options.pattern as string) : undefined;

    return (ctx: MiddlewareContext<E>) => {
      if (dfa && (typeof ctx.event !== 'string' || !dfa.test(ctx.event))) {
        return false;
      }
      return customMatch ? customMatch(ctx) : true;
    };
  }

  /**
   * Builds a compiled pattern listener entry from registration parameters.
   * Handles both native RegExp and DFA-based string patterns.
   *
   * @param once - Whether the listener fires only once.
   * @param pattern - The pattern to match against event keys.
   * @param handler - The handler function.
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
        match: (event: string) => (dfa.test(event) ? {} : null),
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
   * Compares two listener/middleware entries by priority (descending) and
   * registration sequence (ascending) for stable insertion ordering.
   *
   * @param a - First entry.
   * @param b - Second entry.
   * @returns Negative if `a` sorts before `b`, positive if after.
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
   * Creates a disposable unregister function that invokes the given cleanup
   * function at most once.
   *
   * @param cleanupFn - The function to call on unregister.
   * @returns A disposer function.
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
   * Creates a frozen listener context object for the given dispatch phase.
   *
   * @typeParam K - The event key type.
   * @param baseData - Base event data.
   * @param isCanceled - Getter for the canceled state.
   * @param isImmediateStopped - Getter for the immediate-stop state.
   * @param isStopped - Getter for the stopped state.
   * @param getOrCreateMeta - Lazy metadata factory.
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
   * Creates a listener context for sticky replay dispatching.
   *
   * @typeParam K - The event key type.
   * @param event - The event key.
   * @param payload - The event payload.
   * @param phase - The dispatch phase.
   * @returns A frozen replay listener context with zeroed timestamps and ID.
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
   * Dispatches an event to exact and pattern listeners after middleware has
   * completed.
   *
   * @typeParam K - The event key type.
   * @param middlewareCtx - The middleware context (shared state).
   * @param matchedPatterns - Pre-computed pattern matches.
   * @param createListenerContext - Factory for listener contexts.
   */
  private dispatchListeners<K extends keyof E>(
    middlewareCtx: MiddlewareContext<E, K>,
    matchedPatterns: VersionedMatchedPatterns<E>,
    createListenerContext: (phase: 'exact' | 'pattern') => ListenerContext<E, K>,
  ): void {
    if (!middlewareCtx.isStopped && !middlewareCtx.isCanceled) {
      this.invokeExactListeners(middlewareCtx.event, middlewareCtx.payload, createListenerContext);
    }

    if (!middlewareCtx.isStopped && !middlewareCtx.isCanceled && matchedPatterns.length > 0) {
      let activePatternSet: Set<CompiledPatternListenerEntry<E>> | undefined;
      let activePatternVersion = matchedPatterns[MATCHED_PATTERN_VERSION];

      for (let i = 0; i < matchedPatterns.length; i++) {
        const { entry, match } = matchedPatterns[i]!;

        if (activePatternVersion !== this.patternListenerVersion) {
          activePatternSet = new Set(this.patternListeners);
          activePatternVersion = this.patternListenerVersion;
        }

        if (activePatternSet && !activePatternSet.has(entry)) {
          continue;
        }

        const listenerCtx = createListenerContext('pattern');

        if (listenerCtx.isImmediateStopped || listenerCtx.isStopped || listenerCtx.isCanceled) {
          return;
        }

        if (entry.once) {
          this.removePatternEntry(entry);
        }

        this.safeCallPatternHandler(
          entry.handler,
          middlewareCtx.event as string,
          middlewareCtx.payload,
          match,
          listenerCtx,
        );
      }
    }
  }

  /**
   * Core emit logic shared by {@link emit} and {@link emitAsync}.
   * Handles sticky storage, pattern matching, and middleware execution.
   *
   * @typeParam K - The event key type.
   * @param event - The event key.
   * @param payload - The event payload.
   * @param options - Emit options.
   * @param allowAsyncMiddleware - Whether async middleware is permitted.
   * @returns A Promise if async middleware is involved, otherwise void.
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

    const matchedPatterns: VersionedMatchedPatterns<E> =
      typeof event === 'string' && this.patternListeners.length > 0
        ? this.matchPatternListeners(event)
        : (NO_PATTERN_MATCHES as VersionedMatchedPatterns<E>);

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

    let exactListenerCtx: ListenerContext<E, K> | undefined;
    let patternListenerCtx: ListenerContext<E, K> | undefined;
    const createListenerCtx = (phase: 'exact' | 'pattern'): ListenerContext<E, K> => {
      if (phase === 'exact') {
        exactListenerCtx ??= this.createListenerContext(
          baseData,
          isCanceled,
          isImmediateStopped,
          isStopped,
          getOrCreateMeta,
          'exact',
        );
        return exactListenerCtx;
      }

      patternListenerCtx ??= this.createListenerContext(
        baseData,
        isCanceled,
        isImmediateStopped,
        isStopped,
        getOrCreateMeta,
        'pattern',
      );
      return patternListenerCtx;
    };

    if (this.middlewares.length === 0) {
      this.dispatchListeners(middlewareContext, matchedPatterns, createListenerCtx);
      return;
    }

    return this.executeMiddlewares(
      middlewareContext,
      matchedPatterns,
      createListenerCtx,
      allowAsyncMiddleware,
    );
  }

  /**
   * Executes the middleware chain sequentially, then dispatches to listeners.
   *
   * @typeParam K - The event key type.
   * @param middlewareCtx - The middleware context.
   * @param rawMatches - Pre-computed pattern matches.
   * @param createListenerContext - Factory for listener contexts.
   * @param allowAsyncMiddleware - Whether async middleware is permitted.
   * @returns A Promise if any middleware is async, otherwise void.
   */
  private executeMiddlewares<K extends keyof E>(
    middlewareCtx: MiddlewareContext<E, K>,
    rawMatches: VersionedMatchedPatterns<E>,
    createListenerContext: (phase: 'exact' | 'pattern') => ListenerContext<E, K>,
    allowAsyncMiddleware: boolean,
  ): Promise<void> | void {
    const middlewareList = this.middlewares;
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
   * Retrieves a compiled DFA matcher from cache, or compiles and caches a new
   * one for the given pattern string.
   *
   * @param pattern - The pattern string to compile.
   * @returns A compiled Matcher instance.
   */
  private getOrCompileDfa(pattern: string): PatternMatcher {
    let cached = this.dfaCache.get(pattern);
    if (!cached) {
      cached = createPatternMatcher(pattern);
      this.dfaCache.set(pattern, cached);
    }
    return cached;
  }

  /**
   * Handles errors thrown by listener functions.
   * Logs and/or forwards to the configured error handler.
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
   * Checks if a listener returned a Promise, and attaches error handling to it.
   * This prevents unhandled promise rejections from async listeners.
   *
   * @param result - The return value from a listener call.
   */
  private handleMaybeAsyncListenerResult(result: unknown): void {
    if (this.isPromiseLike(result)) {
      Promise.resolve(result).catch((err) => this.handleListenerError(err));
    }
  }

  /**
   * Handles errors thrown by middleware functions.
   * Logs and/or forwards to the configured error handler.
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
   * Inserts an exact listener entry into the appropriate bucket, maintaining
   * priority and sequence ordering via a copy-on-write strategy.
   *
   * @typeParam K - The event key type.
   * @param event - The event key.
   * @param entry - The listener entry to insert.
   */
  private insertExactListenerEntry<K extends keyof E>(
    event: K,
    entry: StoredExactListenerEntry<E>,
  ): void {
    const entries = this.exactListeners.get(event);
    const nextEntries = entries ? this.insertSortedByPriorityCopy(entries, entry) : [entry];
    this.exactListeners.set(event, nextEntries);
  }

  /**
   * Inserts an entry into an array in place, maintaining priority (descending)
   * and sequence (ascending) order using binary search.
   *
   * @param bucket - The array to insert into.
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
   * Returns a new array with the entry inserted in sorted order. Used for
   * copy-on-write immutability during dispatch.
   *
   * @param bucket - The source array.
   * @param entry - The entry to insert.
   * @returns A new sorted array.
   */
  private insertSortedByPriorityCopy<T extends { priority: number; seq: number }>(
    bucket: T[],
    entry: T,
  ): T[] {
    const nextBucket = bucket.slice();
    this.insertSortedByPriority(nextBucket, entry);
    return nextBucket;
  }

  /**
   * Invokes all exact listeners registered for the given event key.
   * Respects stop/immediate-stop/cancel signals.
   *
   * @typeParam K - The event key type.
   * @param event - The event key.
   * @param payload - The event payload.
   * @param createListenerContext - Factory for listener contexts.
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

    for (let i = 0; i < entries.length; i++) {
      const { listener } = entries[i]!;
      const listenerCtx = createListenerContext('exact');

      if (listenerCtx.isImmediateStopped || listenerCtx.isStopped || listenerCtx.isCanceled) {
        return;
      }

      this.safeCallListener(listener as Listener<E[K], E, K>, payload, listenerCtx);
    }
  }

  /**
   * Invokes a single middleware entry, managing the `next()` callback semantics
   * and enforcing that `next()` is called exactly once when required.
   *
   * @param entry - The middleware entry to invoke.
   * @param ctx - The middleware context.
   * @param next - The callback to proceed to the next middleware.
   * @returns A Promise if the middleware or its next callback is async.
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
   * Type guard that checks if a value is Promise-like (has a `.then` method).
   *
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
   * Matches an event string against all registered pattern listeners and
   * returns the matching entries.
   *
   * @param event - The event string to match.
   * @returns A versioned array of matched pattern entries.
   */
  private matchPatternListeners(event: string): VersionedMatchedPatterns<E> {
    const patternListeners = this.patternListeners;
    if (patternListeners.length === 0) {
      return NO_PATTERN_MATCHES as VersionedMatchedPatterns<E>;
    }

    const matches = [] as VersionedMatchedPatterns<E>;
    for (let i = 0; i < patternListeners.length; i++) {
      const entry = patternListeners[i]!;
      const params = entry.match(event);
      if (params) {
        matches.push({ entry, match: params });
      }
    }
    matches[MATCHED_PATTERN_VERSION] = this.patternListenerVersion;
    return matches;
  }

  /**
   * Normalizes a limit value to a non-negative finite integer.
   *
   * @param value - The raw limit value.
   * @returns A non-negative integer.
   */
  private normalizeLimit(value: number): number {
    return Number.isFinite(value) ? Math.max(0, Math.floor(value)) : 0;
  }

  /**
   * Pushes a sticky event for pattern-based replay, respecting configured
   * capacity limits.
   *
   * @param eventKey - The event key string.
   * @param payload - The event payload.
   * @param mode - The sticky mode (replay or consume).
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
   * Pushes a sticky event for exact-key replay, respecting the per-key limit.
   *
   * @typeParam K - The event key type.
   * @param event - The event key.
   * @param payload - The event payload.
   * @param mode - The sticky mode (replay or consume).
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
   * Registers an exact listener, handles sticky replay for the event, and
   * returns an unregister function.
   *
   * @typeParam K - The event key type.
   * @param once - Whether the listener is one-time.
   * @param event - The event key.
   * @param listener - The listener function.
   * @param options - Optional registration configuration.
   * @returns An unregister function.
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
        this.safeCallListener(registeredListener, replayPayload, replayCtx);
      }
    } else {
      for (const replayPayload of this.replayExactStickyAll(event, consumeStickyOverride)) {
        const payloadForEvent = replayPayload as E[K];
        const replayCtx = this.createReplayContext(event, payloadForEvent, 'exact');
        this.safeCallListener(registeredListener, payloadForEvent, replayCtx);
      }
    }

    return this.createDisposableOff(() =>
      this.removeExactListener(event, registeredListener as Listener<any, E, any>),
    );
  }

  /**
   * Registers a middleware entry and returns an unregister function.
   *
   * @param entry - The middleware entry to register.
   * @returns An unregister function.
   */
  private registerMiddlewareEntry(entry: MiddlewareEntry<E>): Off {
    this.middlewares = this.middlewares.concat(entry);
    return this.createDisposableOff(() => {
      const middlewares = this.middlewares;
      const idx = middlewares.indexOf(entry);
      if (idx !== -1) {
        const nextMiddlewares = middlewares.slice();
        nextMiddlewares.splice(idx, 1);
        this.middlewares = nextMiddlewares;
      }
    });
  }

  /**
   * Registers a pattern listener, handles sticky replay, and returns an
   * unregister function.
   *
   * @param once - Whether the listener is one-time.
   * @param pattern - The pattern to match event keys against.
   * @param handler - The pattern handler function.
   * @param options - Optional registration configuration.
   * @returns An unregister function.
   */
  private registerPatternListener(
    once: boolean,
    pattern: RegExp | string,
    handler: PatternHandler<E>,
    options?: OnOptions,
  ): Off {
    const entry = this.buildPatternEntry(once, pattern, handler, options);
    this.patternListeners = this.insertSortedByPriorityCopy(this.patternListeners, entry);
    this.patternListenerVersion++;
    this.replayStickyForEntry(entry, options?.consumeSticky);
    if (once && !this.patternListeners.includes(entry)) {
      return this.createDisposableOff(() => {});
    }
    return this.createDisposableOff(() => this.removePatternEntry(entry));
  }

  /**
   * Removes an exact listener by reference.
   * Matches both the active wrapper and the original listener function.
   *
   * @typeParam K - The event key type.
   * @param event - The event key.
   * @param listener - The listener function to remove.
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

    if (entries.length === 1) {
      this.exactListeners.delete(event);
      return;
    }

    const nextEntries = entries.slice();
    nextEntries.splice(index, 1);
    this.exactListeners.set(event, nextEntries);
  }

  /**
   * Removes a pattern listener entry and bumps the pattern version for
   * invalidation.
   *
   * @param entry - The pattern listener entry to remove.
   */
  private removePatternEntry(entry: CompiledPatternListenerEntry<E>): void {
    const patternListeners = this.patternListeners;
    const idx = patternListeners.indexOf(entry);
    if (idx !== -1) {
      const nextPatternListeners = patternListeners.slice();
      nextPatternListeners.splice(idx, 1);
      this.patternListeners = nextPatternListeners;
      this.patternListenerVersion++;
    }
  }

  /**
   * Removes a sticky event key from the pattern replay map if its batch is
   * empty.
   *
   * @param eventKey - The event key string.
   * @param stickyBatch - Optional pre-fetched batch to check.
   */
  private removeStickyEventKeyIfEmpty(eventKey: string, stickyBatch?: StickyEvent[]): void {
    const batch = stickyBatch ?? this.stickyEvents.get(eventKey);
    if (!batch || batch.length === 0) {
      this.stickyEvents.delete(eventKey);
    }
  }

  /**
   * Replays all sticky events for an exact key, returning their payloads and
   * optionally consuming them based on mode and override.
   *
   * @typeParam K - The event key type.
   * @param event - The event key.
   * @param consumeOverride - Override for sticky consumption behavior.
   * @returns An array of payloads.
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
   * Replays the first sticky event for an exact key (for once() listeners),
   * returning its payload and optionally consuming it.
   *
   * @typeParam K - The event key type.
   * @param event - The event key.
   * @param consumeOverride - Override for sticky consumption behavior.
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
   * Replays sticky events for a newly registered pattern listener entry.
   * Iterates over all stored sticky event keys, matches them against the pattern,
   * and invokes the handler for each matching event.
   *
   * @param entry - The pattern listener entry.
   * @param consumeOverride - Override for sticky consumption behavior.
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

        this.safeCallPatternHandler(entry.handler, eventKey, stickyItem.payload, match, replayCtx);

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
   * Safely invokes a listener, catching synchronous errors and attaching error
   * handling to returned Promises.
   *
   * @typeParam K - The event key type.
   * @param listener - The listener function.
   * @param payload - The event payload.
   * @param ctx - The listener context.
   */
  private safeCallListener<K extends keyof E>(
    listener: Listener<E[K], E, K>,
    payload: E[K],
    ctx: ListenerContext<E, K>,
  ): void {
    try {
      this.handleMaybeAsyncListenerResult(listener(payload, ctx));
    } catch (err) {
      this.handleListenerError(err);
    }
  }

  /**
   * Safely invokes a pattern handler, catching synchronous errors and attaching
   * error handling to returned Promises.
   *
   * @param handler - The pattern handler function.
   * @param event - The matched event string.
   * @param payload - The event payload.
   * @param match - The regex match result or empty object.
   * @param ctx - The listener context.
   */
  private safeCallPatternHandler(
    handler: PatternHandler<E>,
    event: string,
    payload: unknown,
    match: unknown,
    ctx: ListenerContext<E>,
  ): void {
    try {
      this.handleMaybeAsyncListenerResult(handler(event, payload as any, match as any, ctx));
    } catch (err) {
      this.handleListenerError(err);
    }
  }

  /**
   * Schedules an error to be thrown asynchronously to avoid disrupting the
   * current synchronous dispatch. Uses `queueMicrotask` when available,
   * falling back to `Promise.resolve().then()`, and finally `setTimeout`.
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
   * Determines whether a sticky event should be consumed based on its mode
   * and an optional override flag.
   *
   * @param override - Explicit consumption flag, or `undefined` to use mode-based logic.
   * @param mode - The sticky event's mode.
   * @returns `true` if the event should be consumed, `false` if it should be replayed.
   */
  private shouldConsumeSticky(override: boolean | undefined, mode: StickyMode): boolean {
    return override ?? mode === 'consume';
  }
}
