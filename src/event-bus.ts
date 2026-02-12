import type {
  CompiledPatternListener,
  CompiledSeg,
  EmitContext,
  EmitOptions,
  EventMap,
  Listener,
  Middleware,
  MiddlewareEntry,
  OnOptions,
  PatternHandler,
  PatternListenerInfo,
  StickyMode,
  TrieNode,
  UseOptions,
} from './types.ts';

import { DispatcherRuntime } from './dispatcher-runtime.ts';
import { EventScope } from './event-scope.ts';

/**
 * Monotonic id generator for trie nodes.
 *
 * Used to assign each {@link TrieNode} a unique numeric identifier so we can build
 * collision-resistant “visited” keys during DFS matching without storing object references.
 *
 * @internal
 */
let TRIE_NODE_ID = 0;

/**
 * Multiplier used to pack two integers into one "visited key" for DFS/backtracking.
 *
 * We frequently need to remember whether a state `(nodeId, segmentIndex)` (or `(patternIndex, eventIndex)`)
 * has already been expanded, to avoid infinite loops—especially with deep wildcards (`**`).
 *
 * The key is computed as:
 * - `key = a * VISITED_KEY_MULT + b`
 *
 * Requirements:
 * - `VISITED_KEY_MULT` must be larger than the maximum expected value of `b` (the second coordinate),
 *   so that `(a1, b1)` and `(a2, b2)` do not collide.
 *
 * @internal
 */
const VISITED_KEY_MULT = 1_000_000;

/**
 * A lightweight, strongly-typed event bus with:
 * - Exact event listeners (`on`, `once`, `off`)
 * - Pattern listeners with params/globs/wildcards (e.g. `user:{id}:**`, `order:*`, `foo[ab]`)
 * - Middleware pipeline (`use`) with optional matching filters
 * - Scoped listener lifetimes via {@link EventScope}
 * - “Sticky” events that are replayed to future listeners (bounded by `stickyMax`)
 *
 * Pattern syntax (per segment, split by `separator`, default `:`):
 * - `**` deep wildcard: matches zero or more segments
 * - `*`  segment wildcard: matches exactly one segment
 * - `{name}` param: captures a segment into `params.name`
 * - glob segment: supports `*`, `?`, and character classes like `[abc]` / `[!abc]`
 *
 * Notes:
 * - Pattern listeners are stored in a trie for efficient matching.
 * - When emitting, middlewares run first; they can call `ctx.block()` to stop dispatch.
 * - Listener exceptions are caught and rethrown asynchronously to avoid breaking dispatch.
 *
 * @author dafengzhen
 */
export class EventBus<E extends EventMap> {
  /**
   * Global error handler for listener exceptions.
   *
   * If provided, this handler will be called synchronously whenever a listener
   * or pattern handler throws an error. If omitted, errors are logged to console.error
   * and rethrown asynchronously via queueMicrotask.
   *
   * This handler is invoked during {@link safeCall} and applies to:
   * - Exact event listeners
   * - Pattern handlers
   * - Sticky event replays
   *
   * @example
   * ```ts
   * const bus = new EventBus({
   *   onError: (err) => {
   *     Sentry.captureException(err);
   *   }
   * });
   * ```
   */
  readonly onError?: (e: unknown) => void;

  /**
   * Runtime that provides scope tracking and `runWithScope` execution context.
   * Manages the current active scope and execution context for listeners.
   */
  readonly runtime: DispatcherRuntime<E>;

  /**
   * Indicates whether this EventBus instance has been destroyed.
   *
   * When destroyed:
   * - All subsequent method calls (on, emit, use, etc.) will throw an error
   * - Listeners, middlewares, and sticky state are cleared
   * - Pattern compilation cache is cleared
   *
   * @see {@link destroy}
   * @see {@link assertNotDestroyed}
   */
  private destroyed = false;

  /**
   * Exact listeners keyed by event name.
   * Maps event names to Sets of listener functions registered for that exact event.
   */
  private listenersByEvent = new Map<keyof E, Set<Listener<any>>>();

  /**
   * Registered middlewares in insertion order.
   * Each entry contains the middleware function and optional match predicates.
   */
  private middlewares: MiddlewareEntry<E>[] = [];

  /**
   * Cache for compiled patterns, keyed by `${pattern}|${separator}`.
   * Prevents redundant compilation of identical patterns with the same separator.
   */
  private patternCache = new Map<string, ReturnType<EventBus<E>['compilePattern']>>();

  /**
   * A trie per separator (e.g. `:` or `/`) for pattern listeners.
   * Each separator gets its own root trie node for efficient pattern matching.
   */
  private patternTries = new Map<string, TrieNode<E>>();

  /**
   * Monotonic sequence used to stable-sort pattern listeners.
   * Incremented for each new pattern listener to preserve registration order.
   */
  private seq = 0;

  /**
   * @deprecated Use {@link stickyExact} instead. Retained for backward compatibility.
   * Sticky payload queue for string events (FIFO). Bounded by {@link stickyMax}.
   */
  private stickyEvents: Array<{ event: string; mode: StickyMode; payload: unknown }> = [];

  /**
   * Sticky payload queue for exact (typed) events (FIFO). Bounded by {@link stickyExactMax}.
   * Maps event keys to arrays of sticky payloads and their consumption modes.
   */
  private stickyExact = new Map<keyof E, Array<{ mode: StickyMode; payload: unknown }>>();

  /**
   * Max number of sticky payloads retained per exact event key. Default 1 (replay-last).
   * Controls how many historical emissions are stored for exact event replay.
   */
  private readonly stickyExactMax: number;

  /**
   * Maximum number of sticky string events retained for pattern replay.
   * Controls the size of the FIFO queue for string event pattern matching.
   */
  private readonly stickyMax: number;

  /**
   * Create an EventBus instance.
   *
   * @param options.onError - Global error handler for listener exceptions.
   *                          If not provided, errors are logged and rethrown asynchronously.
   * @param options.runtime - Optional custom runtime for scope management.
   *                          Defaults to a new {@link DispatcherRuntime} instance.
   * @param options.stickyExactMax - Maximum number of sticky payloads to retain per exact event key.
   *                                 Default: `1`.
   * @param options.stickyMax - Maximum number of sticky string events retained
   *                            for pattern replay. Default: `200`.
   *
   * @throws Never throws directly; errors in listeners are handled via onError or async rethrow.
   *
   * @example
   * ```ts
   * // Basic usage
   * const bus = new EventBus();
   *
   * // With error handling
   * const bus = new EventBus({
   *   onError: (err) => console.error('EventBus error:', err),
   *   stickyMax: 100,
   *   stickyExactMax: 5
   * });
   * ```
   */
  constructor(options?: {
    onError?: (e: unknown) => void;
    runtime?: DispatcherRuntime<E>;
    stickyExactMax?: number;
    stickyMax?: number;
  }) {
    this.onError = options?.onError;
    this.runtime = options?.runtime ?? new DispatcherRuntime<E>();
    this.stickyMax = options?.stickyMax ?? 200;
    this.stickyExactMax = options?.stickyExactMax ?? 1;
  }

  /**
   * Remove all listeners and pattern tries (exact and pattern listeners).
   * Middlewares and sticky storage are not affected (use {@link reset} to clear everything).
   */
  clearListeners(): void {
    this.listenersByEvent.clear();
    this.patternTries.clear();
  }

  /**
   * Create a new {@link EventScope} for managing listener lifetimes.
   * Listeners registered while a scope is active can be automatically disposed by destroying the scope.
   *
   * @param parent - Optional parent scope. If omitted, uses the runtime's current scope.
   * @returns A new EventScope instance bound to this EventBus.
   */
  createScope(parent?: EventScope<E>): EventScope<E> {
    this.assertNotDestroyed();
    return new EventScope(this, parent);
  }

  /**
   * Destroy this EventBus instance.
   * After calling this, all APIs will throw if used again.
   * This also clears listeners, middlewares, sticky state, and pattern cache.
   */
  destroy(): void {
    if (this.destroyed) {
      return;
    }
    this.reset();
    this.patternCache.clear();
    this.destroyed = true;
  }

  /**
   * Emit an event synchronously (fire-and-forget).
   *
   * Middlewares run asynchronously; errors are rethrown on a microtask.
   *
   * @param event - Event key.
   * @param payload - Event payload.
   * @param options - Emit options (e.g. sticky, metaPatch).
   */
  emit<K extends keyof E>(event: K, payload?: E[K], options?: EmitOptions): void;
  /**
   * Emit an event with `(event, options)` signature (no payload).
   *
   * @param event - Event key.
   * @param options - Emit options (e.g. sticky, metaPatch).
   */
  emit<K extends keyof E>(event: K, options: EmitOptions): void;
  /**
   * Emit an event with overloaded signature.
   *
   * @param event - Event key.
   * @param payloadOrOptions - Either the event payload or EmitOptions.
   * @param options - Emit options when payload is provided separately.
   */
  emit<K extends keyof E>(event: K, payloadOrOptions?: E[K] | EmitOptions, options?: EmitOptions) {
    this.assertNotDestroyed();
    const [payload, opts] = this.parseEmitArgs<E[K]>(payloadOrOptions, options);
    this._emit(event, payload as any, opts).catch((e) => this.rethrowAsync(e));
  }

  /**
   * Emit an event and await the full middleware + dispatch pipeline.
   *
   * @param event - Event key.
   * @param payload - Event payload.
   * @param options - Emit options.
   * @returns Promise that resolves when all middlewares and listeners have completed.
   */
  async emitAsync<K extends keyof E>(
    event: K,
    payload?: E[K],
    options?: EmitOptions,
  ): Promise<void>;
  /**
   * Emit an event with `(event, options)` signature (no payload) and await completion.
   *
   * @param event - Event key.
   * @param options - Emit options.
   * @returns Promise that resolves when all middlewares and listeners have completed.
   */
  async emitAsync<K extends keyof E>(event: K, options: EmitOptions): Promise<void>;
  /**
   * Emit an event asynchronously with overloaded signature.
   *
   * @param event - Event key.
   * @param payloadOrOptions - Either the event payload or EmitOptions.
   * @param options - Emit options when payload is provided separately.
   * @returns Promise that resolves when all middlewares and listeners have completed.
   */
  async emitAsync<K extends keyof E>(
    event: K,
    payloadOrOptions?: E[K] | EmitOptions,
    options?: EmitOptions,
  ): Promise<void> {
    this.assertNotDestroyed();
    const [payload, opts] = this.parseEmitArgs<E[K]>(payloadOrOptions, options);
    await this._emit(event, payload as any, opts);
  }

  /**
   * Remove an exact listener from an event.
   *
   * @param event - Event key.
   * @param listener - Listener function to remove.
   */
  off<K extends keyof E>(event: K, listener: Listener<E[K]>): void {
    this.assertNotDestroyed();
    const set = this.listenersByEvent.get(event);
    if (!set) {
      return;
    }

    set.delete(listener);
    if (set.size === 0) {
      this.listenersByEvent.delete(event);
    }
  }

  /**
   * Register an exact listener.
   *
   * If the event has been emitted as sticky before, the listener is invoked immediately (safely).
   *
   * @param event - Event key.
   * @param listener - Listener invoked with `(payload)`.
   * @param options - Options such as `separator` (used only if treated as pattern), `priority`, etc.
   * @returns An `off()` function to remove this listener.
   */
  on<K extends keyof E>(event: K, listener: Listener<E[K]>, options?: OnOptions): () => void;
  /**
   * Register a pattern listener.
   *
   * The handler signature is `(event, payload, params)`.
   * Patterns can include wildcards, params, and globs (see class doc).
   *
   * If there are matching sticky string events, the handler is replayed immediately for those matches.
   *
   * @param pattern - Pattern string.
   * @param handler - Pattern handler.
   * @param options - Pattern options (`separator`, `priority`, ...).
   * @returns An `off()` function to remove this listener.
   */
  on(pattern: string, handler: PatternHandler<E>, options?: OnOptions): () => void;
  /**
   * Register an event listener with overloaded signature.
   *
   * @param eventOrPattern - Either an exact event key or a pattern string.
   * @param handler - Either an exact listener or a pattern handler.
   * @param options - Registration options.
   * @returns An `off()` function to remove this listener.
   */
  on(eventOrPattern: any, handler: any, options?: OnOptions): () => void {
    this.assertNotDestroyed();
    return this.add(false, eventOrPattern, handler, options, this.runtime.getScope());
  }

  /**
   * Register a one-time listener (exact event or pattern).
   * The listener is removed after the first invocation.
   *
   * Sticky behavior:
   * - Exact event: if already sticky, it will fire immediately and unregister.
   * - Pattern: if any sticky string event matches, it will fire and then unregister.
   *
   * @param event - Exact event key.
   * @param listener - Listener invoked with `(payload)`.
   * @param options - Registration options.
   * @returns An `off()` function to remove this listener early.
   */
  once<K extends keyof E>(event: K, listener: Listener<E[K]>, options?: OnOptions): () => void;
  /**
   * Register a one-time pattern listener.
   *
   * @param pattern - Pattern string.
   * @param handler - Pattern handler.
   * @param options - Pattern options.
   * @returns An `off()` function to remove this listener early.
   */
  once(pattern: string, handler: PatternHandler<E>, options?: OnOptions): () => void;
  /**
   * Register a one-time listener with overloaded signature.
   *
   * @param eventOrPattern - Either an exact event key or a pattern string.
   * @param handler - Either an exact listener or a pattern handler.
   * @param options - Registration options.
   * @returns An `off()` function to remove this listener early.
   */
  once(eventOrPattern: any, handler: any, options?: OnOptions): () => void {
    this.assertNotDestroyed();
    return this.add(true, eventOrPattern, handler, options, this.runtime.getScope());
  }

  /**
   * Reset this EventBus:
   * - clears exact listeners and pattern listeners
   * - removes all middlewares
   * - clears sticky state
   *
   * Does not destroy the instance or clear the pattern compilation cache (use {@link destroy}).
   */
  reset(): void {
    this.clearListeners();
    this.middlewares.length = 0;
    this.stickyExact.clear();
    this.stickyEvents.length = 0;
  }

  /**
   * Register a middleware.
   *
   * Middleware can optionally be restricted by:
   * - `options.pattern`: only run when emitted string event matches the pattern
   * - `options.onlyWhenPatternListenerMatched`: only run when any pattern listener would match the event
   * - `options.match`: custom predicate
   *
   * @param mw - Middleware function `(ctx, next) => ...`.
   * @param options - Matching options controlling when this middleware runs.
   * @returns An `off()` function to remove this middleware.
   */
  use(mw: Middleware<E>, options?: UseOptions<E>): () => void {
    this.assertNotDestroyed();

    const matchers: NonNullable<MiddlewareEntry<E>['match']>[] = [];

    if (options?.pattern) {
      const sep = options.separator ?? ':';
      const compiled = this.compilePatternCached(options.pattern, sep);
      matchers.push((ctx) => typeof ctx.event === 'string' && !!compiled.match(ctx.event));
    }

    if (options?.onlyWhenPatternListenerMatched) {
      matchers.push((ctx) => typeof ctx.event === 'string' && this.hasAnyPatternMatch(ctx.event));
    }

    if (options?.match) {
      matchers.push(options.match);
    }

    const match =
      matchers.length === 0
        ? undefined
        : (ctx: EmitContext<E, keyof E>) => matchers.every((m) => m(ctx));

    const entry: MiddlewareEntry<E> = { fn: mw, match };
    this.middlewares.push(entry);

    return this.makeOff(() => this.removeFromArray(this.middlewares, entry));
  }

  /**
   * Run `fn` inside a fresh scope, then destroy the scope automatically.
   *
   * Useful for temporary listeners that must be cleaned up even if `fn` throws.
   *
   * @param fn - Function executed with the created scope.
   * @param options options
   * @param options.parent - Optional parent scope (defaults to runtime current scope).
   * @returns Promise resolving to the return value of `fn`.
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
      return ret instanceof Promise ? await ret : (ret as T);
    } finally {
      scope.destroy();
    }
  }

  /**
   * Internal emit implementation:
   * - optionally stores sticky payload
   * - matches pattern listeners (string events only)
   * - builds {@link EmitContext}
   * - runs middlewares then dispatch
   *
   * @param event - Event key.
   * @param payload - Event payload.
   * @param options - Emit options.
   * @internal
   */
  private async _emit<K extends keyof E>(event: K, payload: E[K], options?: EmitOptions) {
    let blocked = false;

    if (options?.sticky) {
      const mode: StickyMode = options.stickyMode ?? 'replay';
      this.pushStickyExact(event, payload as unknown, mode);
      if (typeof event === 'string') {
        this.pushStickyEvent(event, payload, mode);
      }
    }

    const matchedRaw =
      typeof event === 'string' ? this.matchPatternListeners(event) : ([] as const);

    const ctx: EmitContext<E, K> = {
      /** Stop the remaining middleware/dispatch pipeline. */
      block() {
        blocked = true;
      },
      /** Whether the pipeline has been blocked. */
      get blocked() {
        return blocked;
      },
      event,
      /**
       * Frozen list of matched pattern listeners (sorted by priority/sequence),
       * including captured `params` for each match.
       */
      matched: Object.freeze(
        matchedRaw.map(
          ({ entry, params }): PatternListenerInfo<E> => ({
            handler: entry.handler,
            once: entry.once,
            params: Object.freeze({ ...params }),
            pattern: entry.pattern,
            priority: entry.priority,
          }),
        ),
      ),
      /** Metadata bag; starts from `options.metaPatch` shallow-cloned. */
      meta: { ...options?.metaPatch },
      params: {},
      payload,
    };

    await this.runMiddlewares(ctx, matchedRaw as any);
  }

  /**
   * Add either an exact listener or a pattern listener, depending on inputs.
   *
   * A string `eventOrPattern` is treated as a pattern if:
   * - handler arity suggests pattern handler (`handler.length >= 2`), OR
   * - the string “looks like” a pattern (contains wildcard/param/glob syntax)
   *
   * @param once - Whether this is a one-time listener.
   * @param eventOrPattern - Event key or pattern string.
   * @param handler - Listener function or pattern handler.
   * @param options - Registration options.
   * @param scope - Current event scope for automatic cleanup.
   * @returns An `off()` function to remove the listener.
   * @internal
   */
  private add(
    once: boolean,
    eventOrPattern: any | keyof E | string,
    handler: any,
    options?: OnOptions,
    scope?: EventScope<E>,
  ): () => void {
    const sep = options?.separator ?? ':';

    const treatAsPattern =
      typeof eventOrPattern === 'string' && this.looksLikePattern(eventOrPattern, sep);

    if (treatAsPattern) {
      const off = this.addPatternListener(once, String(eventOrPattern), handler, options);
      if (scope) {
        scope.registerOff(off);
      }
      return off;
    }

    const event = eventOrPattern as keyof E;
    const consumeStickyOverride = options?.consumeSticky; // true/false/undefined

    if (!once) {
      this.getListenerSet(event).add(handler);

      const off = this.makeOff(() => this.off(event, handler));
      if (scope) {
        scope.registerOff(off);
      }

      // Replay sticky (possibly multiple, but default max is 1).
      const payloads = this.replayExactStickyAll(event, consumeStickyOverride);
      for (const p of payloads) {
        this.safeCall(() => handler(p));
      }

      return off;
    }

    const wrapper = ((p: any) => {
      try {
        handler(p);
      } finally {
        this.off(event, wrapper);
      }
    }) as Listener<any>;

    this.getListenerSet(event).add(wrapper);

    const off = this.makeOff(() => this.off(event, wrapper));
    if (scope) {
      scope.registerOff(off);
    }

    // For once: replay only one sticky payload (FIFO).
    const p = this.replayExactStickyOne(event, consumeStickyOverride);
    if (p !== undefined) {
      this.safeCall(() => wrapper(p));
    }

    return off;
  }

  /**
   * Register a pattern listener and insert it into the separator-specific trie.
   * Also replays matching sticky string events immediately.
   *
   * @param once - Whether this is a one-time pattern listener.
   * @param pattern - Pattern string.
   * @param handler - Pattern handler function.
   * @param options - Registration options.
   * @returns An `off()` function to remove this pattern listener.
   * @internal
   */
  private addPatternListener(once: boolean, pattern: string, handler: any, options?: OnOptions) {
    const sep = options?.separator ?? ':';
    const compiled = this.compilePatternCached(pattern, sep);
    const consumeStickyOverride = options?.consumeSticky; // true/false/undefined

    const entry: CompiledPatternListener<E> = {
      handler,
      match: compiled.match,
      once,
      pattern,
      priority: options?.priority ?? compiled.priority,
      separator: sep,
      seq: ++this.seq,
    };

    this.trieInsert(entry, compiled.compiledSegs);

    // Replay sticky string events that match this pattern.
    // If consumeSticky=true, remove matched sticky events as they are replayed.
    for (let i = 0; i < this.stickyEvents.length; ) {
      const s = this.stickyEvents[i];
      const params = entry.match(s.event);
      if (!params) {
        i++;
        continue;
      }

      this.safeCall(() => handler(s.event as any, s.payload as any, params));

      const consume = this.shouldConsumeSticky(consumeStickyOverride, s.mode);
      if (consume) {
        this.stickyEvents.splice(i, 1);
      } else {
        i++;
      }

      if (once) {
        this.trieRemove(entry, compiled.compiledSegs);
        break;
      }
    }

    return this.makeOff(() => this.trieRemove(entry, compiled.compiledSegs));
  }

  /**
   * Throw if this instance has been destroyed.
   *
   * @throws {Error} If the EventBus instance has been destroyed.
   * @internal
   */
  private assertNotDestroyed() {
    if (this.destroyed) {
      throw new Error('EventBus instance has been destroyed.');
    }
  }

  /**
   * Compile a pattern into:
   * - `compiledSegs`: normalized segments for trie insertion and matching
   * - `match(event)`: returns params if matched, otherwise `null`
   * - `priority`: a score where higher means “more specific” (sorted first)
   *
   * @param pattern - Pattern string.
   * @param sep - Segment separator (e.g. `:`). If empty, the whole string is one segment.
   * @returns Compiled pattern object with match function, segments, and priority.
   * @internal
   */
  private compilePattern(pattern: string, sep: string) {
    if (pattern === '**') {
      return {
        compiledSegs: [{ type: 'deepWildcard' as const }] as CompiledSeg[],
        match: () => ({}) as Record<string, string>,
        priority: -100,
      };
    }

    const pSegs = sep ? pattern.split(sep) : [pattern];
    let score = 0;

    const compiledSegs: CompiledSeg[] = pSegs.map((seg) => {
      if (seg === '**') {
        score += 0;
        return { type: 'deepWildcard' as const };
      }
      if (seg === '*') {
        score += 10;
        return { type: 'segWildcard' as const };
      }
      if (seg.startsWith('{') && seg.endsWith('}') && seg.length > 2) {
        score += 80;
        return { key: seg.slice(1, -1), type: 'param' as const };
      }
      if (seg.includes('*') || seg.includes('?') || seg.includes('[')) {
        score += 70;
        return { re: this.globToRegExp(seg), src: seg, type: 'glob' as const };
      }

      score += 100;
      return { type: 'exact' as const, value: seg };
    });

    /**
     * Match an emitted string event against this compiled pattern.
     *
     * Implementation uses an explicit stack (DFS) to support `**` backtracking
     * without recursion. Returns a params object on success, otherwise `null`.
     *
     * @param event - String event to match against.
     * @returns Params object if matched, null otherwise.
     */
    const match = (event: string) => {
      const eSegs = sep ? event.split(sep) : [event];

      type State = { i: number; j: number; params: Record<string, string> };
      const stack: State[] = [{ i: 0, j: 0, params: {} }];

      const expanded = new Set<number>();
      const keyOf = (i: number, j: number) => i * VISITED_KEY_MULT + j;

      while (stack.length) {
        const st = stack.pop()!;
        let i = st.i;
        const j = st.j;
        const curParams = st.params;

        if (j === eSegs.length) {
          while (compiledSegs[i]?.type === 'deepWildcard') {
            i++;
          }
          if (i === compiledSegs.length) {
            return curParams;
          }
          continue;
        }

        const p = compiledSegs[i];
        const seg = eSegs[j];
        if (!p) {
          continue;
        }

        if (p.type === 'deepWildcard') {
          const k0 = keyOf(i + 1, j);
          if (!expanded.has(k0)) {
            expanded.add(k0);
            stack.push({ i: i + 1, j, params: curParams });
          }
          stack.push({ i, j: j + 1, params: curParams });
          continue;
        }

        if (p.type === 'segWildcard') {
          stack.push({ i: i + 1, j: j + 1, params: curParams });
          continue;
        }

        if (p.type === 'exact') {
          if (p.value === seg) {
            stack.push({ i: i + 1, j: j + 1, params: curParams });
          }
          continue;
        }

        if (p.type === 'glob') {
          if (p.re.test(seg)) {
            stack.push({ i: i + 1, j: j + 1, params: curParams });
          }
          continue;
        }

        // param
        stack.push({ i: i + 1, j: j + 1, params: { ...curParams, [p.key]: seg } });
      }

      return null;
    };

    return { compiledSegs, match, priority: score };
  }

  /**
   * Compile a pattern and cache the result by `(pattern, separator)`.
   *
   * @param pattern - Pattern string.
   * @param sep - Segment separator.
   * @returns Compiled pattern object (cached or newly compiled).
   * @internal
   */
  private compilePatternCached(pattern: string, sep: string) {
    const cacheKey = `${pattern}|${sep}`;
    let compiled = this.patternCache.get(cacheKey);
    if (!compiled) {
      compiled = this.compilePattern(pattern, sep);
      this.patternCache.set(cacheKey, compiled);
    }
    return compiled;
  }

  /**
   * Create a new trie node with a unique id.
   *
   * @returns A new TrieNode instance.
   * @internal
   */
  private createNode<T extends EventMap>(): TrieNode<T> {
    return { end: [], exact: new Map(), id: ++TRIE_NODE_ID };
  }

  /**
   * Get or create the Set that stores exact listeners for `event`.
   *
   * @param event - Event key.
   * @returns Set of listener functions for this event.
   * @internal
   */
  private getListenerSet<K extends keyof E>(event: K): Set<Listener<E[K]>> {
    let set = this.listenersByEvent.get(event) as Set<Listener<E[K]>> | undefined;
    if (!set) {
      set = new Set();
      this.listenersByEvent.set(event, set as any);
    }
    return set;
  }

  /**
   * Convert a glob segment into a RegExp.
   * Supported:
   * - `*` any substring
   * - `?` any single char
   * - `[abc]` character class
   * - `[!abc]` negated class
   *
   * @param seg - Glob segment string.
   * @returns RegExp for matching this glob pattern.
   * @internal
   */
  private globToRegExp(seg: string): RegExp {
    let re = '^';
    for (let i = 0; i < seg.length; i++) {
      const ch = seg[i];

      if (ch === '*') {
        re += '.*';
        continue;
      }
      if (ch === '?') {
        re += '.';
        continue;
      }
      if (ch === '[') {
        const j = seg.indexOf(']', i + 1);
        if (j === -1) {
          re += '\\[';
          continue;
        }

        const content = seg.slice(i + 1, j);
        if (content.length === 0) {
          re += '\\[\\]';
          i = j;
          continue;
        }

        let cls = content;
        if (cls[0] === '!') {
          cls = '^' + cls.slice(1);
        }
        cls = cls.replace(/\\/g, '\\\\').replace(/]/g, '\\]');
        re += `[${cls}]`;
        i = j;
        continue;
      }

      if (/[$()*+.?[\\\]^{|}]/.test(ch)) {
        re += '\\' + ch;
      } else {
        re += ch;
      }
    }
    re += '$';
    return new RegExp(re);
  }

  /**
   * Check whether any registered pattern listener (for any separator)
   * would match the given string event.
   *
   * @param event - String event to check.
   * @returns True if at least one pattern listener matches the event.
   * @internal
   */
  private hasAnyPatternMatch(event: string): boolean {
    for (const [sep, root] of this.patternTries) {
      const eSegs = sep ? event.split(sep) : [event];
      if (this.trieHasAnyMatch(root, eSegs)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Invoke exact listeners first, then matched pattern listeners (in sorted order).
   * Pattern listeners marked `once` are removed after invocation.
   *
   * @param ctx - Emit context.
   * @param matched - Array of matched pattern listeners with their captured params.
   * @internal
   */
  private async invokeDispatch(
    ctx: EmitContext<E, keyof E>,
    matched: Array<{ entry: CompiledPatternListener<E>; params: Record<string, string> }>,
  ) {
    this.invokeExactListeners(ctx.event, ctx.payload, ctx);
    for (const { entry, params } of matched) {
      if (ctx.blocked) {
        return;
      }

      ctx.params = params;
      this.safeCall(() => (entry.handler as any)(ctx.event, ctx.payload, params, ctx));

      if (entry.once) {
        const compiled = this.compilePatternCached(entry.pattern, entry.separator);
        this.trieRemove(entry, compiled.compiledSegs);
      }
    }
  }

  /**
   * Invoke all exact listeners registered for `event`.
   *
   * @param event - Event key.
   * @param payload - Event payload.
   * @param ctx - Emit context.
   * @internal
   */
  private invokeExactListeners<K extends keyof E>(event: K, payload: E[K], ctx: EmitContext<E, K>) {
    const set = this.listenersByEvent.get(event);
    if (!set || set.size === 0) {
      return;
    }

    for (const fn of Array.from(set)) {
      ctx.params = {};
      this.safeCall(() => (fn as any)(payload, ctx));
    }
  }

  /**
   * Heuristic for distinguishing payload vs options in `emit(...)`.
   * Treats objects containing `sticky` or `metaPatch` as {@link EmitOptions}.
   *
   * @param x - Value to check.
   * @returns True if the value appears to be EmitOptions.
   * @internal
   */
  private looksLikeEmitOptions(x: any): x is EmitOptions {
    return !!x && typeof x === 'object' && ('sticky' in x || 'metaPatch' in x);
  }

  /**
   * Heuristic for determining whether a string “looks like” a pattern.
   *
   * @param s - String to check.
   * @param sep - Segment separator.
   * @returns True if the string contains pattern syntax.
   * @internal
   */
  private looksLikePattern(s: string, sep: string): boolean {
    const segs = sep ? s.split(sep) : [s];
    for (const seg of segs) {
      if (seg === '*' || seg === '**') {
        return true;
      }
      if (seg.startsWith('{') && seg.endsWith('}') && seg.length > 2) {
        return true;
      }
      if (this.segmentHasGlobMeta(seg)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Wrap a cleanup function as an idempotent `off()` callback.
   *
   * @param fn - Cleanup function to wrap.
   * @returns Idempotent off function that calls fn at most once.
   * @internal
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
   * Match all pattern listeners against a string event, deduplicate by `(seq, paramsKey)`,
   * then sort:
   * - higher priority first
   * - for equal priority, earlier registered first (lower seq)
   *
   * @param event - String event to match.
   * @returns Array of matched entries with their captured params.
   * @internal
   */
  private matchPatternListeners(event: string) {
    const raw: Array<{
      entry: CompiledPatternListener<E>;
      params: Record<string, string>;
      paramsKey: string;
    }> = [];

    for (const [sep, root] of this.patternTries) {
      const eSegs = sep ? event.split(sep) : [event];
      this.trieMatchCollect(root, eSegs, raw);
    }

    const uniq = new Map<
      string,
      { entry: CompiledPatternListener<E>; params: Record<string, string> }
    >();

    for (const m of raw) {
      const key = `${m.entry.seq}|${m.paramsKey}`;
      if (!uniq.has(key)) {
        uniq.set(key, { entry: m.entry, params: m.params });
      }
    }

    const out = Array.from(uniq.values());

    out.sort((a, b) => {
      if (b.entry.priority !== a.entry.priority) {
        return b.entry.priority - a.entry.priority;
      }
      return a.entry.seq - b.entry.seq;
    });

    return out;
  }

  /**
   * Parse overloaded `emit` arguments:
   * - `(event, payload?, options?)`
   * - `(event, options)` (when second arg looks like EmitOptions)
   *
   * @param payloadOrOptions - Either payload or EmitOptions.
   * @param options - Explicit EmitOptions (if provided separately).
   * @returns Tuple of [payload, options].
   * @internal
   */
  private parseEmitArgs<P>(
    payloadOrOptions?: EmitOptions | P,
    options?: EmitOptions,
  ): [P | undefined, EmitOptions | undefined] {
    return this.looksLikeEmitOptions(payloadOrOptions)
      ? [undefined, payloadOrOptions]
      : [payloadOrOptions as P | undefined, options];
  }

  /**
   * Append a sticky string event for pattern replay, enforcing {@link stickyMax}.
   *
   * @param event - String event.
   * @param payload - Event payload.
   * @param mode - Sticky consumption mode.
   * @internal
   */
  private pushStickyEvent(event: string, payload: unknown, mode: StickyMode) {
    this.stickyEvents.push({ event, mode, payload });
    const overflow = this.stickyEvents.length - this.stickyMax;
    if (overflow > 0) {
      this.stickyEvents.splice(0, overflow);
    }
  }

  /**
   * Append a sticky payload for an exact event key, enforcing {@link stickyExactMax}.
   *
   * @param event - Exact event key.
   * @param payload - Event payload.
   * @param mode - Sticky consumption mode.
   * @internal
   */
  private pushStickyExact<K extends keyof E>(event: K, payload: unknown, mode: StickyMode) {
    const q = this.stickyExact.get(event) ?? [];
    q.push({ mode, payload });
    const overflow = q.length - this.stickyExactMax;
    if (overflow > 0) {
      q.splice(0, overflow);
    }
    this.stickyExact.set(event, q);
  }

  /**
   * Remove the first occurrence of `item` from `arr` if present.
   *
   * @param arr - Array to modify.
   * @param item - Item to remove.
   * @internal
   */
  private removeFromArray<T>(arr: T[], item: T) {
    const i = arr.indexOf(item);
    if (i >= 0) {
      arr.splice(i, 1);
    }
  }

  /**
   * Replay all sticky payloads for an exact event key.
   * Consumption is decided per entry (unless overridden).
   *
   * @param event - Exact event key.
   * @param consumeOverride - Override for sticky consumption behavior.
   * @returns Array of payloads that were replayed.
   * @internal
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
    const remaining: typeof q = [];

    for (const it of q) {
      out.push(it.payload);
      const consume = this.shouldConsumeSticky(consumeOverride, it.mode);
      if (!consume) {
        remaining.push(it);
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
   * Replay a single sticky payload for an exact event key (FIFO).
   * Consumption is decided by override or entry.mode.
   *
   * @param event - Exact event key.
   * @param consumeOverride - Override for sticky consumption behavior.
   * @returns The replayed payload, or undefined if no sticky event exists.
   * @internal
   */
  private replayExactStickyOne<K extends keyof E>(
    event: K,
    consumeOverride: boolean | undefined,
  ): undefined | unknown {
    const q = this.stickyExact.get(event);
    if (!q || q.length === 0) {
      return undefined;
    }

    const first = q[0];
    const consume = this.shouldConsumeSticky(consumeOverride, first.mode);
    if (consume) {
      q.shift();
      if (q.length === 0) {
        this.stickyExact.delete(event);
      } else {
        this.stickyExact.set(event, q);
      }
    }
    return first.payload;
  }

  /**
   * Rethrow an error asynchronously (microtask), so synchronous flows keep going.
   *
   * @param err - Error to rethrow.
   * @internal
   */
  private rethrowAsync(err: unknown) {
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
   * Run middlewares in order and finally dispatch listeners.
   *
   * Middleware must call `next()` exactly once to continue.
   * If `ctx.block()` is called, the pipeline stops immediately.
   *
   * @param ctx - Emit context.
   * @param rawMatches - Array of matched pattern listeners.
   * @internal
   */
  private async runMiddlewares<K extends keyof E>(
    ctx: EmitContext<E, K>,
    rawMatches: Array<{ entry: CompiledPatternListener<E>; params: Record<string, string> }>,
  ): Promise<void> {
    const mws = this.middlewares.slice();
    let i = 0;

    const next = async (): Promise<void> => {
      if (ctx.blocked) {
        return;
      }

      if (i >= mws.length) {
        return this.invokeDispatch(ctx as any, rawMatches);
      }

      const entry = mws[i++];

      if (entry.match && !entry.match(ctx as any)) {
        return next();
      }

      let called = false;
      await entry.fn(ctx as any, async () => {
        if (called) {
          throw new Error('next() called multiple times.');
        }
        called = true;
        await next();
      });
    };

    await next();
  }

  /**
   * Safely invoke a listener/handler.
   * Errors are logged and rethrown asynchronously.
   *
   * @param fn - Function to invoke safely.
   * @internal
   */
  private safeCall(fn: () => void) {
    try {
      fn();
    } catch (err) {
      console.error('[EventBus] Listener error:', err);
      if (this.onError) {
        this.onError(err);
      } else {
        this.rethrowAsync(err);
      }
    }
  }

  /**
   * Whether a segment contains glob meta characters.
   * This is used only as a heuristic for pattern detection.
   *
   * @param seg - Segment to check.
   * @returns True if the segment contains glob syntax.
   * @internal
   */
  private segmentHasGlobMeta(seg: string): boolean {
    return seg.includes('*') || seg.includes('?') || seg.includes('[') || seg.includes(']');
  }

  /**
   * Resolve whether to consume sticky for a given entry.
   * - if override is true/false, it wins
   * - otherwise follow entry.mode
   *
   * @param override - Explicit override value.
   * @param mode - Sticky consumption mode.
   * @returns True if the sticky event should be consumed.
   * @internal
   */
  private shouldConsumeSticky(override: boolean | undefined, mode: StickyMode): boolean {
    return override ?? mode === 'consume';
  }

  /**
   * Fast existence check: returns `true` if any pattern listener can match `eSegs`.
   * Used by middleware `onlyWhenPatternListenerMatched`.
   *
   * @param root - Root trie node.
   * @param eSegs - Split event segments.
   * @returns True if any pattern listener matches the segments.
   * @internal
   */
  private trieHasAnyMatch(root: TrieNode<E>, eSegs: string[]): boolean {
    type State = { i: number; node: TrieNode<E> };
    const stack: State[] = [{ i: 0, node: root }];

    const expanded = new Set<number>();
    const keyOf = (nodeId: number, i: number) => nodeId * VISITED_KEY_MULT + i;

    while (stack.length) {
      const st = stack.pop()!;
      const node = st.node;
      const i = st.i;

      if (i === eSegs.length) {
        if (node.end.length) {
          return true;
        }

        if (node.deep) {
          const k = keyOf(node.deep.id, i);
          if (!expanded.has(k)) {
            expanded.add(k);
            stack.push({ i, node: node.deep });
          }
        }
        continue;
      }

      const seg = eSegs[i];

      if (node.deep) {
        const k0 = keyOf(node.deep.id, i);
        if (!expanded.has(k0)) {
          expanded.add(k0);
          stack.push({ i, node: node.deep });
        }
        stack.push({ i: i + 1, node: node.deep });
      }

      const exactNext = node.exact.get(seg);
      if (exactNext) {
        stack.push({ i: i + 1, node: exactNext });
      }

      if (node.star) {
        stack.push({ i: i + 1, node: node.star });
      }

      if (node.globs?.length) {
        for (const g of node.globs) {
          if (g.re.test(seg)) {
            stack.push({ i: i + 1, node: g.node });
          }
        }
      }

      if (node.params?.length) {
        for (const p of node.params) {
          stack.push({ i: i + 1, node: p.node });
        }
      }
    }

    return false;
  }

  /**
   * Insert a compiled pattern listener into the separator-specific trie.
   * Leaf `end` arrays are kept ordered by `(priority desc, seq asc)` to speed up collection.
   *
   * @param entry - Compiled pattern listener to insert.
   * @param segs - Compiled segments from the pattern.
   * @internal
   */
  private trieInsert(entry: CompiledPatternListener<E>, segs: CompiledSeg[]) {
    const sep = entry.separator;
    let root = this.patternTries.get(sep);
    if (!root) {
      root = this.createNode<E>();
      this.patternTries.set(sep, root);
    }

    let node = root;
    for (const s of segs) {
      if (s.type === 'exact') {
        let next = node.exact.get(s.value);
        if (!next) {
          next = this.createNode<E>();
          node.exact.set(s.value, next);
        }
        node = next;
        continue;
      }

      if (s.type === 'segWildcard') {
        node.star ??= this.createNode<E>();
        node = node.star;
        continue;
      }

      if (s.type === 'deepWildcard') {
        if (!node.deep) {
          const deepNode = this.createNode<E>();
          deepNode.deep = deepNode;
          node.deep = deepNode;
        }
        node = node.deep;
        continue;
      }

      if (s.type === 'glob') {
        node.globs ??= [];
        let found = node.globs.find((x) => x.src === s.src);
        if (!found) {
          found = { node: this.createNode<E>(), re: s.re, src: s.src };
          node.globs.push(found);
        }
        node = found.node;
        continue;
      }

      node.params ??= [];
      let found = node.params.find((x) => x.key === s.key);
      if (!found) {
        found = { key: s.key, node: this.createNode<E>() };
        node.params.push(found);
      }
      node = found.node;
    }

    const arr = node.end;
    let lo = 0;
    let hi = arr.length;

    while (lo < hi) {
      const mid = (lo + hi) >>> 1;
      const m = arr[mid];

      if (m.priority > entry.priority) {
        lo = mid + 1;
      } else if (m.priority < entry.priority) {
        hi = mid;
      } else {
        if (m.seq <= entry.seq) {
          lo = mid + 1;
        } else {
          hi = mid;
        }
      }
    }

    arr.splice(lo, 0, entry);
  }

  /**
   * Collect all pattern listener matches for `eSegs` into `out`.
   * Also produces a `paramsKey` used for deduplication.
   *
   * @param root - Root trie node.
   * @param eSegs - Split event segments.
   * @param out - Output array to collect matches.
   * @internal
   */
  private trieMatchCollect(
    root: TrieNode<E>,
    eSegs: string[],
    out: Array<{
      entry: CompiledPatternListener<E>;
      params: Record<string, string>;
      paramsKey: string;
    }>,
  ) {
    type State = {
      i: number;
      node: TrieNode<E>;
      params: Record<string, string>;
      paramsKey: string;
    };

    const stack: State[] = [{ i: 0, node: root, params: {}, paramsKey: '' }];

    const expanded = new Set<number>();
    const keyOf = (nodeId: number, i: number) => nodeId * VISITED_KEY_MULT + i;

    while (stack.length) {
      const st = stack.pop()!;
      const node = st.node;
      const i = st.i;

      if (i === eSegs.length) {
        if (node.end.length) {
          for (const entry of node.end) {
            out.push({ entry, params: st.params, paramsKey: st.paramsKey });
          }
        }

        if (node.deep) {
          const k = keyOf(node.deep.id, i);
          if (!expanded.has(k)) {
            expanded.add(k);
            stack.push({ i, node: node.deep, params: st.params, paramsKey: st.paramsKey });
          }
        }
        continue;
      }

      const seg = eSegs[i];

      if (node.deep) {
        const k0 = keyOf(node.deep.id, i);
        if (!expanded.has(k0)) {
          expanded.add(k0);
          stack.push({ i, node: node.deep, params: st.params, paramsKey: st.paramsKey });
        }
        stack.push({ i: i + 1, node: node.deep, params: st.params, paramsKey: st.paramsKey });
      }

      const exactNext = node.exact.get(seg);
      if (exactNext) {
        stack.push({ i: i + 1, node: exactNext, params: st.params, paramsKey: st.paramsKey });
      }

      if (node.star) {
        stack.push({ i: i + 1, node: node.star, params: st.params, paramsKey: st.paramsKey });
      }

      if (node.globs?.length) {
        for (const g of node.globs) {
          if (g.re.test(seg)) {
            stack.push({ i: i + 1, node: g.node, params: st.params, paramsKey: st.paramsKey });
          }
        }
      }

      if (node.params?.length) {
        for (const p of node.params) {
          const nextParams = { ...st.params, [p.key]: seg };

          const nextKey = st.paramsKey + '\u0000' + p.key + '=' + seg;
          stack.push({ i: i + 1, node: p.node, params: nextParams, paramsKey: nextKey });
        }
      }
    }
  }

  /**
   * Remove a pattern listener from the trie.
   * This does not currently prune empty nodes.
   *
   * @param entry - Compiled pattern listener to remove.
   * @param segs - Compiled segments from the pattern.
   * @internal
   */
  private trieRemove(entry: CompiledPatternListener<E>, segs: CompiledSeg[]) {
    const root = this.patternTries.get(entry.separator);
    if (!root) {
      return;
    }

    let node: TrieNode<E> | undefined = root;

    for (const s of segs) {
      if (!node) {
        return;
      }

      if (s.type === 'exact') {
        node = node.exact.get(s.value);
        continue;
      }
      if (s.type === 'segWildcard') {
        node = node.star;
        continue;
      }
      if (s.type === 'deepWildcard') {
        node = node.deep;
        continue;
      }
      if (s.type === 'glob') {
        const found = node.globs?.find((x) => x.src === s.src);
        node = found?.node;
        continue;
      }
      const found = node.params?.find((x) => x.key === s.key);
      node = found?.node;
    }

    if (!node) {
      return;
    }

    const idx = node.end.indexOf(entry);
    if (idx >= 0) {
      node.end.splice(idx, 1);
    }
  }
}
