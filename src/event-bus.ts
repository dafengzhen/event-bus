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
  ParamNode,
  PatternHandler,
  PatternListenerInfo,
  StickyMode,
  TrieNode,
  UseOptions,
} from './types.ts';

import { DispatcherRuntime } from './dispatcher-runtime.ts';
import { EventScope } from './event-scope.ts';

let TRIE_NODE_ID = 0;

const VISITED_KEY_MULT = 1_000_000;

/**
 * A typed event bus with support for:
 * - **Exact events** (`on('ready', ...)`)
 * - **Pattern events** (`on('user:{id}:*', ...)`) using a segment separator (default `:`)
 * - **Middlewares** (`use(...)`) with optional matching rules
 * - **Sticky events** (replay/consume semantics for late subscribers)
 * - **Scoped subscriptions** via {@link EventScope} to auto-unsubscribe on scope destroy
 *
 * Pattern syntax (per segment):
 * - `*` matches **exactly one** segment
 * - `**` matches **zero or more** segments
 * - `{name}` captures a segment into `params.name`
 * - Glob-like segment patterns are supported: `a*`, `?`, `[abc]`, `[!abc]`, etc.
 *
 * Notes:
 * - `emit()` is fire-and-forget and will rethrow listener errors asynchronously.
 * - `emitAsync()` awaits middleware/dispatch completion.
 * - For non-string events (typed keys that are not `string`), pattern matching and pattern-sticky are skipped.
 *
 * @author dafengzhen
 */
export class EventBus<E extends EventMap> {
  /** Optional error hook for listener/middleware failures. If absent, errors are rethrown asynchronously. */
  readonly onError?: (e: unknown) => void;

  /** Runtime holder used to manage current {@link EventScope} and scope propagation. */
  readonly runtime: DispatcherRuntime<E>;

  private destroyed = false;

  private listenersByEvent = new Map<keyof E, Set<Listener<any>>>();

  private middlewares: ReadonlyArray<MiddlewareEntry<E>> = [];

  /** Cache for compiled pattern matchers keyed by `pattern|separator`. */
  private patternCache = new Map<string, ReturnType<EventBus<E>['compilePattern']>>();

  /** One trie per separator for fast pattern lookups. */
  private patternTries = new Map<string, TrieNode<E>>();

  /** Monotonic sequence used to preserve insertion order among same-priority listeners. */
  private seq = 0;

  /**
   * Sticky storage for *string events* to enable replay for future pattern listeners.
   * (Exact sticky is stored separately in `stickyExact`.)
   */
  private stickyEvents: Array<{ event: string; mode: StickyMode; payload: unknown }> = [];

  /**
   * Sticky storage for exact event keys.
   * Each key keeps a queue (bounded by {@link stickyExactMax}).
   */
  private stickyExact = new Map<keyof E, Array<{ mode: StickyMode; payload: unknown }>>();

  /** Max sticky items kept per exact event key. */
  private readonly stickyExactMax: number;

  /** Max sticky items kept for string events (pattern replay). */
  private readonly stickyMax: number;

  /**
   * Create an {@link EventBus}.
   *
   * @param options.onError Optional error handler invoked when a listener throws.
   * @param options.runtime Optional runtime instance. Useful for sharing scope management.
   * @param options.stickyExactMax Max sticky items per exact event (default `1`).
   * @param options.stickyMax Max sticky items for pattern replay (default `200`).
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
   * Remove all registered listeners (both exact and pattern).
   * Middlewares and sticky buffers are not cleared here—use {@link reset} for full reset.
   */
  clearListeners(): void {
    this.listenersByEvent.clear();
    this.patternTries.clear();
  }

  /**
   * Create a new {@link EventScope} that can auto-unsubscribe handlers registered within it.
   *
   * @param parent Optional parent scope. If provided, destroying the parent will destroy the child.
   */
  createScope(parent?: EventScope<E>): EventScope<E> {
    this.assertNotDestroyed();
    return new EventScope(this, parent);
  }

  /**
   * Destroy this bus instance. After destroy, all methods that mutate/emit will throw.
   * This clears listeners, middlewares, and sticky state and drops pattern cache.
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
   * Emit an event (fire-and-forget).
   *
   * Overloads:
   * - `emit(event, payload?, options?)`
   * - `emit(event, options)` (when you want options without payload)
   *
   * If a listener/middleware throws and {@link onError} is not provided,
   * the error is rethrown asynchronously (microtask/Promise/setTimeout fallback).
   */
  emit<K extends keyof E>(event: K, payload?: E[K], options?: EmitOptions): void;
  emit<K extends keyof E>(event: K, options: EmitOptions): void;
  emit<K extends keyof E>(event: K, payloadOrOptions?: E[K] | EmitOptions, options?: EmitOptions) {
    this.assertNotDestroyed();
    const [payload, opts] = this.parseEmitArgs<E[K]>(payloadOrOptions, options);
    this._emit(event, payload as any, opts).catch((e) => this.rethrowAsync(e));
  }

  /**
   * Emit an event and await completion of middleware + dispatch.
   *
   * Overloads:
   * - `emitAsync(event, payload?, options?)`
   * - `emitAsync(event, options)` (when you want options without payload)
   */
  async emitAsync<K extends keyof E>(
    event: K,
    payload?: E[K],
    options?: EmitOptions,
  ): Promise<void>;
  async emitAsync<K extends keyof E>(event: K, options: EmitOptions): Promise<void>;
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
   * Unsubscribe an exact event listener.
   *
   * @param event Event key
   * @param listener The previously registered listener function
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
   * Subscribe to either an exact event or a pattern.
   *
   * - If `eventOrPattern` is a string that "looks like" a pattern (contains `*`, `**`, `{param}`, or glob meta),
   *   it is treated as a pattern listener.
   * - Otherwise it's treated as an exact event key.
   *
   * Returns an `off()` function for convenience.
   */
  on<K extends keyof E>(event: K, listener: Listener<E[K]>, options?: OnOptions): () => void;
  on(pattern: string, handler: PatternHandler<E>, options?: OnOptions): () => void;
  on(eventOrPattern: any, handler: any, options?: OnOptions): () => void {
    this.assertNotDestroyed();
    return this.add(false, eventOrPattern, handler, options, this.runtime.getScope());
  }

  /**
   * Subscribe once to either an exact event or a pattern.
   * Automatically unsubscribes after the first matched dispatch.
   *
   * Returns an `off()` function (safe to call multiple times).
   */
  once<K extends keyof E>(event: K, listener: Listener<E[K]>, options?: OnOptions): () => void;
  once(pattern: string, handler: PatternHandler<E>, options?: OnOptions): () => void;
  once(eventOrPattern: any, handler: any, options?: OnOptions): () => void {
    this.assertNotDestroyed();
    return this.add(true, eventOrPattern, handler, options, this.runtime.getScope());
  }

  /**
   * Subscribe once to an exact event key (never treated as pattern).
   * Convenience wrapper around internal subscription logic.
   */
  onceEvent<K extends keyof E>(
    event: K,
    listener: Listener<E[K]>,
    options?: OnOptions,
  ): () => void {
    this.assertNotDestroyed();
    return this.add(true, event, listener, options, this.runtime.getScope());
  }

  /**
   * Subscribe once to a pattern (never treated as exact).
   * Convenience wrapper that also registers the off() into the current scope (if any).
   */
  oncePattern(pattern: string, handler: PatternHandler<E>, options?: OnOptions): () => void {
    this.assertNotDestroyed();
    const off = this.addPatternListener(true, pattern, handler, options);

    const scope = this.runtime.getScope();
    if (scope) {
      scope.registerOff(off);
    }
    return off;
  }

  /**
   * Subscribe to an exact event key (never treated as pattern).
   * Convenience wrapper around {@link on}.
   */
  onEvent<K extends keyof E>(event: K, listener: Listener<E[K]>, options?: OnOptions): () => void {
    this.assertNotDestroyed();
    return this.add(false, event, listener, options, this.runtime.getScope());
  }

  /**
   * Subscribe to a pattern (never treated as exact).
   * Convenience wrapper that also registers the off() into the current scope (if any).
   */
  onPattern(pattern: string, handler: PatternHandler<E>, options?: OnOptions): () => void {
    this.assertNotDestroyed();
    const off = this.addPatternListener(false, pattern, handler, options);

    const scope = this.runtime.getScope();
    if (scope) {
      scope.registerOff(off);
    }
    return off;
  }

  /**
   * Reset bus state:
   * - clears listeners (exact + pattern)
   * - removes all middlewares
   * - clears sticky buffers
   *
   * Pattern compilation cache is kept; use {@link destroy} to clear it too.
   */
  reset(): void {
    this.clearListeners();
    this.middlewares = [];
    this.stickyExact.clear();
    this.stickyEvents.length = 0;
  }

  /**
   * Register a middleware.
   *
   * Middleware can be conditionally applied via {@link UseOptions}:
   * - `pattern`: only run for string events matching the pattern
   * - `onlyWhenPatternListenerMatched`: only run when the emitted string event matches *any* pattern listener
   * - `match`: custom predicate
   *
   * Returns an `off()` function to remove the middleware.
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
    this.middlewares = [...this.middlewares, entry];

    return this.makeOff(() => {
      const next = this.middlewares.filter((x) => x !== entry);
      if (next.length !== this.middlewares.length) {
        this.middlewares = next;
      }
    });
  }

  /**
   * Run a function within a temporary {@link EventScope}.
   * The created scope will be destroyed (thus unsubscribing any registered handlers) in a `finally` block.
   *
   * @param fn Function to run under the new scope.
   * @param options options
   * @param options.parent Optional parent scope. Defaults to the current runtime scope.
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
   * Core emit implementation:
   * - optionally records sticky payload
   * - resolves matching pattern listeners (string events only)
   * - builds {@link EmitContext}
   * - runs middleware chain, then dispatches to exact + matched pattern listeners
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
      /** Prevent further dispatch (pattern listeners and remaining middlewares). */
      block() {
        blocked = true;
      },

      /** Whether dispatch has been blocked by middleware/listener. */
      get blocked() {
        return blocked;
      },

      /** The emitted event key. */
      event,

      /**
       * Immutable view of matched pattern listeners (string events only), in actual dispatch order.
       * `params` is frozen per entry.
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

      /** User-defined metadata; starts from `options.metaPatch` and may be modified by middleware. */
      meta: { ...options?.metaPatch },

      /**
       * The current params for the listener being invoked.
       * For exact listeners it's `{}`; for pattern listeners it's the matched params.
       */
      params: {},

      /** The emitted payload. */
      payload,
    };

    await this.runMiddlewares(ctx, matchedRaw as any);
  }

  /**
   * Internal subscription helper for both exact and pattern:
   * - Determines whether a string should be treated as a pattern
   * - Registers into scope if provided
   * - Replays sticky payloads to newly added listeners based on consume rules
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
    const consumeStickyOverride = options?.consumeSticky;

    if (!once) {
      this.getListenerSet(event).add(handler);

      const off = this.makeOff(() => this.off(event, handler));
      if (scope) {
        scope.registerOff(off);
      }

      // Replay all exact sticky payloads for this event to the new listener.
      const payloads = this.replayExactStickyAll(event, consumeStickyOverride);
      for (const p of payloads) {
        this.safeCall(() => handler(p));
      }

      return off;
    }

    // Once wrapper for exact listeners.
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

    // Replay at most one exact sticky payload for once-listeners.
    const p = this.replayExactStickyOne(event, consumeStickyOverride);
    if (p !== undefined) {
      this.safeCall(() => wrapper(p));
    }

    return off;
  }

  /**
   * Add a pattern listener into the trie and optionally replay sticky string events that match it.
   *
   * Sticky replay behavior:
   * - For each stored sticky string event that matches this pattern, handler is invoked immediately.
   * - If sticky mode is `consume` (or overridden by `options.consumeSticky`), the sticky item is removed.
   * - If `once`, the listener is removed after the first matched sticky replay.
   */
  private addPatternListener(once: boolean, pattern: string, handler: any, options?: OnOptions) {
    const sep = options?.separator ?? ':';
    const compiled = this.compilePatternCached(pattern, sep);
    const consumeStickyOverride = options?.consumeSticky;

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

  /** Throw if this bus has been destroyed. */
  private assertNotDestroyed() {
    if (this.destroyed) {
      throw new Error('EventBus instance has been destroyed.');
    }
  }

  /**
   * Compile a pattern into:
   * - `compiledSegs`: per-segment matcher representation
   * - `match(event)`: returns params map on match, else `null`
   * - `priority`: a heuristic score used for dispatch ordering (higher first)
   *
   * Special case: `**` alone matches everything with very low priority.
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

    const materializeParams = (node?: ParamNode): Record<string, string> => {
      const out: Record<string, string> = Object.create(null);
      for (let p = node; p; p = p.prev) {
        out[p.k] = p.v;
      }
      return out;
    };

    /**
     * Match an event string against this pattern.
     * Returns a params object on success, otherwise `null`.
     */
    const match = (event: string) => {
      const eSegs = sep ? event.split(sep) : [event];

      type State = { i: number; j: number; params?: ParamNode };
      const stack: State[] = [{ i: 0, j: 0, params: undefined }];

      // Tracks expanded deep-wildcard states to avoid infinite expansion.
      const expanded = new Set<number>();
      const keyOf = (i: number, j: number) => i * VISITED_KEY_MULT + j;

      while (stack.length) {
        const st = stack.pop()!;
        let i = st.i;
        const j = st.j;

        if (j === eSegs.length) {
          // Only deep wildcards may remain.
          while (compiledSegs[i]?.type === 'deepWildcard') {
            i++;
          }
          if (i === compiledSegs.length) {
            return materializeParams(st.params);
          }
          continue;
        }

        const p = compiledSegs[i];
        const seg = eSegs[j];
        if (!p) {
          continue;
        }

        if (p.type === 'deepWildcard') {
          // Branch: consume 0 segments or consume 1 and stay.
          const k0 = keyOf(i + 1, j);
          if (!expanded.has(k0)) {
            expanded.add(k0);
            stack.push({ i: i + 1, j, params: st.params });
          }
          stack.push({ i, j: j + 1, params: st.params });
          continue;
        }

        if (p.type === 'segWildcard') {
          stack.push({ i: i + 1, j: j + 1, params: st.params });
          continue;
        }

        if (p.type === 'exact') {
          if (p.value === seg) {
            stack.push({ i: i + 1, j: j + 1, params: st.params });
          }
          continue;
        }

        if (p.type === 'glob') {
          if (p.re.test(seg)) {
            stack.push({ i: i + 1, j: j + 1, params: st.params });
          }
          continue;
        }

        // param
        stack.push({
          i: i + 1,
          j: j + 1,
          params: { k: p.key, prev: st.params, v: seg },
        });
      }

      return null;
    };

    return { compiledSegs, match, priority: score };
  }

  /**
   * Compile pattern with memoization keyed by `pattern|sep`.
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

  /** Create a fresh trie node. */
  private createNode<T extends EventMap>(): TrieNode<T> {
    return { end: [], exact: new Map(), id: ++TRIE_NODE_ID };
  }

  /** Get or create the listener set for an exact event key. */
  private getListenerSet<K extends keyof E>(event: K): Set<Listener<E[K]>> {
    let set = this.listenersByEvent.get(event) as Set<Listener<E[K]>> | undefined;
    if (!set) {
      set = new Set();
      this.listenersByEvent.set(event, set as any);
    }
    return set;
  }

  /**
   * Convert a glob-like segment into a RegExp.
   * Supports `*`, `?`, character classes `[abc]`, and negated classes `[!abc]`.
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

      // Escape regex meta.
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
   * Check if a given string event matches *any* registered pattern listener (across all separators).
   * Used for conditional middlewares.
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
   * Dispatch in final order:
   * 1) exact listeners
   * 2) matched pattern listeners (by priority desc, seq asc)
   *
   * Pattern listeners can be `once`; they are removed after dispatch.
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

  /** Invoke listeners registered for an exact event key. */
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

  /** Type guard for the alternate emit signature `emit(event, options)` (no payload). */
  private looksLikeEmitOptions(x: any): x is EmitOptions {
    return !!x && typeof x === 'object' && ('sticky' in x || 'metaPatch' in x);
  }

  /**
   * Quick heuristic to decide whether a string should be treated as a pattern.
   * If any segment is `*`, `**`, `{param}`, or contains glob meta, it is considered a pattern.
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
   * Wrap an `fn` into an idempotent `off()` function.
   * Calling `off()` multiple times is safe.
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
   * Find all pattern listeners that match the string event, deduplicate by (seq, params),
   * then sort by:
   * 1) priority desc
   * 2) seq asc (older first)
   */
  private matchPatternListeners(event: string) {
    const uniq = new Map<
      string,
      { entry: CompiledPatternListener<E>; params: Record<string, string> }
    >();

    for (const [sep, root] of this.patternTries) {
      const eSegs = sep ? event.split(sep) : [event];
      this.trieMatchCollectUniq(root, eSegs, uniq);
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
   * Normalize emit overload arguments.
   * If the second argument "looks like" options, treat it as options and payload as undefined.
   */
  private parseEmitArgs<P>(
    payloadOrOptions?: EmitOptions | P,
    options?: EmitOptions,
  ): [P | undefined, EmitOptions | undefined] {
    return this.looksLikeEmitOptions(payloadOrOptions)
      ? [undefined, payloadOrOptions]
      : [payloadOrOptions as P | undefined, options];
  }

  /** Push a sticky item for string events (pattern replay). Bounded by {@link stickyMax}. */
  private pushStickyEvent(event: string, payload: unknown, mode: StickyMode) {
    this.stickyEvents.push({ event, mode, payload });
    const overflow = this.stickyEvents.length - this.stickyMax;
    if (overflow > 0) {
      this.stickyEvents.splice(0, overflow);
    }
  }

  /** Push a sticky item for exact events (exact replay). Bounded by {@link stickyExactMax}. */
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
   * Replay all sticky payloads for an exact event to a newly subscribed listener.
   * Items are consumed depending on their sticky mode or `consumeOverride`.
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
   * Replay a single sticky payload (oldest) for an exact event to a newly subscribed once-listener.
   * The returned payload may be consumed depending on mode/override.
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
   * Rethrow an error asynchronously to avoid breaking the current call stack.
   * Prefers `queueMicrotask`, then Promise microtasks, then `setTimeout`.
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
   * Execute middleware chain in registration order.
   * Each middleware must call `next()` at most once to continue dispatch.
   * When the chain completes, dispatch is performed.
   */
  private async runMiddlewares<K extends keyof E>(
    ctx: EmitContext<E, K>,
    rawMatches: Array<{ entry: CompiledPatternListener<E>; params: Record<string, string> }>,
  ): Promise<void> {
    const mws = this.middlewares;
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
   * Invoke a function and handle errors consistently.
   * If {@link onError} exists, it is called; otherwise the error is rethrown asynchronously.
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

  /** Whether a segment contains glob meta characters. */
  private segmentHasGlobMeta(seg: string): boolean {
    return seg.includes('*') || seg.includes('?') || seg.includes('[') || seg.includes(']');
  }

  /**
   * Determine whether a sticky item should be consumed after replay.
   * `consumeOverride` (from subscription options) wins; otherwise consume when mode is `'consume'`.
   */
  private shouldConsumeSticky(override: boolean | undefined, mode: StickyMode): boolean {
    return override ?? mode === 'consume';
  }

  /**
   * Trie-only check: return true if there exists any pattern listener that matches `eSegs`.
   * Used for `onlyWhenPatternListenerMatched`.
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
   * Insert a compiled pattern listener into the trie, keeping `node.end` sorted by:
   * - priority desc
   * - seq asc
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
   * Traverse trie and collect matches, deduplicating by (listener seq + paramsKey).
   * `paramsKey` is a stable serialization of captured params for uniqueness.
   */
  private trieMatchCollectUniq(
    root: TrieNode<E>,
    eSegs: string[],
    uniq: Map<string, { entry: CompiledPatternListener<E>; params: Record<string, string> }>,
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
            const key = `${entry.seq}|${st.paramsKey}`;
            if (!uniq.has(key)) {
              uniq.set(key, { entry, params: st.params });
            }
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

  /** Remove a specific compiled pattern listener from the trie. */
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
