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
  /** Runtime that provides scope tracking and `runWithScope` execution context. */
  readonly runtime: DispatcherRuntime<E>;

  private destroyed = false;

  /** Exact listeners keyed by event name. */
  private listenersByEvent = new Map<keyof E, Set<Listener<any>>>();

  /** Registered middlewares in insertion order. */
  private middlewares: MiddlewareEntry<E>[] = [];

  /** Cache for compiled patterns, keyed by `${pattern}|${separator}`. */
  private patternCache = new Map<string, ReturnType<EventBus<E>['compilePattern']>>();

  /** A trie per separator (e.g. `:` or `/`) for pattern listeners. */
  private patternTries = new Map<string, TrieNode<E>>();

  /** Monotonic sequence used to stable-sort pattern listeners. */
  private seq = 0;

  /**
   * Sticky events for string events, used to “replay” to newly-added pattern listeners.
   * This list is bounded by {@link stickyMax}.
   */
  private stickyEvents: Array<{ event: string; payload: unknown }> = [];

  /** Sticky payloads for exact (typed) events. */
  private stickyExact = new Map<keyof E, unknown>();

  /** Maximum number of sticky string events retained for pattern replay. */
  private readonly stickyMax: number;

  /**
   * Create an EventBus.
   *
   * @param options.runtime - Optional custom runtime. Defaults to a new {@link DispatcherRuntime}.
   * @param options.stickyMax - Maximum number of sticky string events retained for pattern replay. Default: `200`.
   */
  constructor(options?: { runtime?: DispatcherRuntime<E>; stickyMax?: number }) {
    this.runtime = options?.runtime ?? new DispatcherRuntime<E>();
    this.stickyMax = options?.stickyMax ?? 200;
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
   * @param parent - Optional parent scope. If omitted, uses the runtime’s current scope.
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
   */
  async emitAsync<K extends keyof E>(
    event: K,
    payload?: E[K],
    options?: EmitOptions,
  ): Promise<void>;
  /**
   * Emit an event with `(event, options)` signature (no payload) and await completion.
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
   * @returns An `off()` function to remove this listener early.
   */
  once<K extends keyof E>(event: K, listener: Listener<E[K]>, options?: OnOptions): () => void;
  once(pattern: string, handler: PatternHandler<E>, options?: OnOptions): () => void;
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
   */
  private async _emit<K extends keyof E>(event: K, payload: E[K], options?: EmitOptions) {
    let blocked = false;

    if (options?.sticky) {
      this.stickyExact.set(event, payload as unknown);
      if (typeof event === 'string') {
        this.pushStickyEvent(event, payload);
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
      typeof eventOrPattern === 'string' &&
      (handler?.length >= 2 || this.looksLikePattern(eventOrPattern, sep));

    if (treatAsPattern) {
      const off = this.addPatternListener(once, String(eventOrPattern), handler, options);
      if (scope) {
        scope.registerOff(off);
      }
      return off;
    }

    const event = eventOrPattern as keyof E;

    if (!once) {
      this.getListenerSet(event).add(handler);

      const off = this.makeOff(() => this.off(event, handler));
      if (scope) {
        scope.registerOff(off);
      }

      if (this.stickyExact.has(event)) {
        this.safeCall(() => handler(this.stickyExact.get(event)));
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

    if (this.stickyExact.has(event)) {
      this.safeCall(() => wrapper(this.stickyExact.get(event)));
    }

    return off;
  }

  /**
   * Register a pattern listener and insert it into the separator-specific trie.
   * Also replays matching sticky string events immediately.
   */
  private addPatternListener(once: boolean, pattern: string, handler: any, options?: OnOptions) {
    const sep = options?.separator ?? ':';
    const compiled = this.compilePatternCached(pattern, sep);

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

    for (const s of this.stickyEvents) {
      const params = entry.match(s.event);
      if (params) {
        this.safeCall(() => handler(s.event as any, s.payload as any, params));
        if (once) {
          this.trieRemove(entry, compiled.compiledSegs);
          break;
        }
      }
    }

    return this.makeOff(() => this.trieRemove(entry, compiled.compiledSegs));
  }

  /** Throw if this instance has been destroyed. */
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

  /** Create a new trie node with a unique id. */
  private createNode<T extends EventMap>(): TrieNode<T> {
    return { end: [], exact: new Map(), id: ++TRIE_NODE_ID };
  }

  /**
   * Get or create the Set that stores exact listeners for `event`.
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
   */
  private async invokeDispatch(
    ctx: EmitContext<E, keyof E>,
    matched: Array<{ entry: CompiledPatternListener<E>; params: Record<string, string> }>,
  ) {
    this.invokeExactListeners(ctx.event, ctx.payload);

    for (const { entry, params } of matched) {
      if (ctx.blocked) {
        return;
      }

      this.safeCall(() => entry.handler(ctx.event, ctx.payload, params));

      if (entry.once) {
        const compiled = this.compilePatternCached(entry.pattern, entry.separator);
        this.trieRemove(entry, compiled.compiledSegs);
      }
    }
  }

  /**
   * Invoke all exact listeners registered for `event`.
   */
  private invokeExactListeners<K extends keyof E>(event: K, payload: E[K]) {
    const set = this.listenersByEvent.get(event);
    if (!set || set.size === 0) {
      return;
    }

    for (const fn of Array.from(set)) {
      this.safeCall(() => fn(payload));
    }
  }

  /**
   * Heuristic for distinguishing payload vs options in `emit(...)`.
   * Treats objects containing `sticky` or `metaPatch` as {@link EmitOptions}.
   */
  private looksLikeEmitOptions(x: any): x is EmitOptions {
    return !!x && typeof x === 'object' && ('sticky' in x || 'metaPatch' in x);
  }

  /**
   * Heuristic for determining whether a string “looks like” a pattern.
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
   */
  private pushStickyEvent(event: string, payload: unknown) {
    this.stickyEvents.push({ event, payload });
    const overflow = this.stickyEvents.length - this.stickyMax;
    if (overflow > 0) {
      this.stickyEvents.splice(0, overflow);
    }
  }

  /** Remove the first occurrence of `item` from `arr` if present. */
  private removeFromArray<T>(arr: T[], item: T) {
    const i = arr.indexOf(item);
    if (i >= 0) {
      arr.splice(i, 1);
    }
  }

  /**
   * Rethrow an error asynchronously (microtask), so synchronous flows keep going.
   */
  private rethrowAsync(err: unknown) {
    queueMicrotask(() => {
      throw err;
    });
  }

  /**
   * Run middlewares in order and finally dispatch listeners.
   *
   * Middleware must call `next()` exactly once to continue.
   * If `ctx.block()` is called, the pipeline stops immediately.
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
   */
  private safeCall(fn: () => void) {
    try {
      fn();
    } catch (err) {
      console.error('[EventBus] Listener error:', err);
      this.rethrowAsync(err);
    }
  }

  /**
   * Whether a segment contains glob meta characters.
   * This is used only as a heuristic for pattern detection.
   */
  private segmentHasGlobMeta(seg: string): boolean {
    return seg.includes('*') || seg.includes('?') || seg.includes('[') || seg.includes(']');
  }

  /**
   * Fast existence check: returns `true` if any pattern listener can match `eSegs`.
   * Used by middleware `onlyWhenPatternListenerMatched`.
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
