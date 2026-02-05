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
  PatternKind,
  PatternListenerInfo,
  TrieNode,
  UseOptions,
} from './types.ts';

import { DispatcherRuntime } from './dispatcher-runtime.ts';
import { EventScope } from './event-scope.ts';

let TRIE_NODE_ID = 0;

/**
 * A typed EventBus with:
 * - Exact event listeners (`on/once` with an event key)
 * - Pattern listeners for string events (`on/once` with `{ pattern: true }`)
 * - Middleware pipeline (`use`) that can block dispatching
 * - Sticky events (replay last payload per exact event, and a bounded history for pattern matches)
 * - Scoped subscriptions via `EventScope` / `DispatcherRuntime`
 *
 * Pattern syntax (when `options.pattern === true`):
 * - `a:b:c` segments (separator defaults to `:`; configurable via `separator`)
 * - `*` matches exactly one segment
 * - `**` matches zero or more segments (deep wildcard)
 * - `{name}` captures one segment into `params.name`
 *
 * Priority:
 * - More specific patterns get higher priority (exact segments score higher).
 * - For equal priority, earlier registration order runs first.
 *
 * @typeParam E - Event map where keys are event names and values are payload types.
 *
 * @author dafengzhen
 */
export class EventBus<E extends EventMap> {
  /** Runtime used to obtain and run with scopes. */
  readonly runtime: DispatcherRuntime<E>;

  /** Whether this instance has been destroyed. */
  private destroyed = false;

  /** Exact listeners keyed by event name. */
  private listenersByEvent = new Map<keyof E, Set<Listener<any>>>();

  /** Middleware pipeline, applied in registration order (snapshot per emit). */
  private middlewares: MiddlewareEntry<E>[] = [];

  /** Cache for compiled patterns, keyed by `${pattern}|${separator}`. */
  private patternCache = new Map<string, ReturnType<EventBus<E>['compilePattern']>>();

  /** Pattern tries keyed by separator string. */
  private patternTries = new Map<string, TrieNode<E>>();

  /** Monotonic counter used for stable ordering among pattern listeners. */
  private seq = 0;

  /**
   * Sticky history for string events to support immediate replay for pattern listeners.
   * Bounded by `stickyMax`.
   */
  private stickyEvents: Array<{ event: string; payload: unknown }> = [];

  /**
   * Sticky payloads for exact events:
   * - When emitting with `{ sticky: true }`, the last payload is stored here.
   * - When adding an exact listener, it is invoked immediately if a sticky payload exists.
   */
  private stickyExact = new Map<keyof E, unknown>();

  /** Maximum number of sticky history entries for pattern replay. */
  private readonly stickyMax: number;

  /**
   * Create a new EventBus.
   *
   * @param options - Construction options.
   * @param options.runtime - Custom dispatcher runtime (defaults to a new `DispatcherRuntime`).
   * @param options.stickyMax - Max history size for sticky string events used by pattern listeners (default `200`).
   */
  constructor(options?: { runtime?: DispatcherRuntime<E>; stickyMax?: number }) {
    this.runtime = options?.runtime ?? new DispatcherRuntime<E>();
    this.stickyMax = options?.stickyMax ?? 200;
  }

  /**
   * Remove all listeners (exact + pattern) but keep middleware and sticky state.
   */
  clearListeners(): void {
    this.listenersByEvent.clear();
    this.patternTries.clear();
  }

  /**
   * Create a new `EventScope`.
   *
   * A scope can register multiple `off()` functions and dispose them together.
   *
   * @param parent - Optional parent scope (defaults to current runtime scope when used via `withScope`).
   * @returns A new scope.
   * @throws If the EventBus has been destroyed.
   */
  createScope(parent?: EventScope<E>): EventScope<E> {
    this.assertNotDestroyed();
    return new EventScope(this, parent);
  }

  /**
   * Destroy this EventBus instance.
   *
   * After destruction, all public APIs throw if called.
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
   * This method schedules errors to be rethrown asynchronously.
   * If you need a rejected promise instead, use `emitAsync`.
   *
   * Payload vs options overload:
   * - `emit(event, payload, options)`
   * - `emit(event, options)` where `options` looks like `EmitOptions`
   *
   * Sticky:
   * - When `options.sticky === true`, stores the payload:
   *   - for exact listeners: last payload by event key
   *   - for pattern listeners (string events): also stored in bounded history for replay
   *
   * @typeParam K - Exact event key.
   * @param event - Exact event name.
   * @param payload - Payload for the event.
   * @param options - Emit options.
   * @throws If the EventBus has been destroyed.
   */
  emit<K extends keyof E>(event: K, payload?: E[K], options?: EmitOptions): void;
  emit<K extends keyof E>(event: K, payloadOrOptions?: E[K] | EmitOptions, options?: EmitOptions) {
    this.assertNotDestroyed();
    const [payload, opts] = this.parseEmitArgs<E[K]>(payloadOrOptions, options);
    this._emit(event, payload as any, opts).catch((e) => this.rethrowAsync(e));
  }

  /**
   * Emit an event and await completion of middleware + dispatch.
   *
   * Payload vs options overload:
   * - `emitAsync(event, payload, options)`
   * - `emitAsync(event, options)` where `options` looks like `EmitOptions`
   *
   * @typeParam K - Exact event key.
   * @param event - Exact event name.
   * @param payload - Payload for the event.
   * @param options - Emit options.
   * @returns A promise that resolves when dispatch completes.
   * @throws If the EventBus has been destroyed.
   */
  async emitAsync<K extends keyof E>(
    event: K,
    payload?: E[K],
    options?: EmitOptions,
  ): Promise<void>;
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
   * Unregister an exact event listener.
   *
   * @typeParam K - Exact event key.
   * @param event - Exact event name.
   * @param listener - Listener to remove.
   * @throws If the EventBus has been destroyed.
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
   * Register an exact event listener.
   *
   * If the event has a sticky payload, the listener is invoked immediately (synchronously, guarded).
   *
   * @typeParam K - Exact event key.
   * @param event - Exact event name.
   * @param listener - Listener invoked with the event payload.
   * @param options - Listener options.
   * @returns An `off()` function to unregister this listener.
   * @throws If the EventBus has been destroyed.
   */
  on<K extends keyof E>(event: K, listener: Listener<E[K]>, options?: OnOptions): () => void;

  /**
   * Register a pattern listener (string events only).
   *
   * If there are sticky history entries, any matching past events are replayed immediately.
   *
   * @param pattern - Pattern string.
   * @param handler - Handler invoked with `(event, payload, params)`.
   * @param options - Options with `pattern: true`.
   * @returns An `off()` function to unregister this listener.
   * @throws If the EventBus has been destroyed.
   */
  on(
    pattern: string,
    handler: PatternHandler<E>,
    options: OnOptions & { pattern: true },
  ): () => void;
  on(eventOrPattern: any, handler: any, options?: OnOptions): () => void {
    this.assertNotDestroyed();
    return this.add(false, eventOrPattern, handler, options, this.runtime.getScope());
  }

  /**
   * Register a one-time exact event listener.
   *
   * If the event has a sticky payload, the listener is invoked immediately and then removed.
   *
   * @typeParam K - Exact event key.
   * @param event - Exact event name.
   * @param listener - Listener invoked with the event payload.
   * @param options - Listener options.
   * @returns An `off()` function to unregister this listener.
   * @throws If the EventBus has been destroyed.
   */
  once<K extends keyof E>(event: K, listener: Listener<E[K]>, options?: OnOptions): () => void;

  /**
   * Register a one-time pattern listener (string events only).
   *
   * If there are sticky history entries, the first matching replay triggers the handler and then removes it.
   *
   * @param pattern - Pattern string.
   * @param handler - Handler invoked with `(event, payload, params)`.
   * @param options - Options with `pattern: true`.
   * @returns An `off()` function to unregister this listener.
   * @throws If the EventBus has been destroyed.
   */
  once(
    pattern: string,
    handler: PatternHandler<E>,
    options: OnOptions & { pattern: true },
  ): () => void;
  once(eventOrPattern: any, handler: any, options?: OnOptions): () => void {
    this.assertNotDestroyed();
    return this.add(true, eventOrPattern, handler, options, this.runtime.getScope());
  }

  /**
   * Reset the EventBus:
   * - clears listeners (exact + pattern)
   * - clears middleware
   * - clears sticky state
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
   * Middlewares are invoked in registration order for each `emit/emitAsync`.
   * A middleware can:
   * - observe/modify `ctx.meta`
   * - call `ctx.block()` to stop dispatching
   * - decide whether to call `next()` (exactly once) to continue the pipeline
   *
   * Matching:
   * - `options.pattern`: only run when `ctx.event` (string) matches the given pattern
   * - `options.onlyWhenPatternListenerMatched`: only run when the current emitted string event has at least one pattern listener match
   * - `options.match`: custom predicate
   *
   * @param mw - Middleware function.
   * @param options - Matching options.
   * @returns An `off()` function to unregister this middleware.
   * @throws If the EventBus has been destroyed.
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
   * Run a function within a temporary scope, then destroy that scope afterwards.
   *
   * Any subscriptions created via `on/once` inside the callback are automatically registered to
   * the current runtime scope and will be disposed when the scope is destroyed.
   *
   * @typeParam T - Return type.
   * @param fn - Function to run with the created scope.
   * @param options - Scope options.
   * @param options.parent - Parent scope (defaults to current runtime scope).
   * @returns The return value of `fn`.
   * @throws If the EventBus has been destroyed.
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
   * @internal Core emit implementation: builds context, runs middleware, then dispatches.
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
      block() {
        blocked = true;
      },
      get blocked() {
        return blocked;
      },
      event,
      matched: Object.freeze(
        matchedRaw.map(
          ({ entry, params }): PatternListenerInfo<E> => ({
            handler: entry.handler,
            kind: entry.kind,
            once: entry.once,
            params: Object.freeze({ ...params }),
            pattern: entry.pattern,
            priority: entry.priority,
          }),
        ),
      ),
      meta: { ...options?.metaPatch },
      payload,
    };

    await this.runMiddlewares(ctx, matchedRaw as any);
  }

  /**
   * @internal Register either an exact listener or a pattern listener depending on options.
   *
   * If a scope is provided, the resulting `off()` is registered into it.
   */
  private add(
    once: boolean,
    eventOrPattern: keyof E | string,
    handler: any,
    options?: OnOptions,
    scope?: EventScope<E>,
  ): () => void {
    if (options?.pattern) {
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
   * @internal Add a compiled pattern listener into the trie.
   *
   * This also replays matching sticky history entries immediately.
   *
   * @param once - Whether the handler should auto-remove after the first match.
   * @param pattern - Pattern string.
   * @param handler - Handler function.
   * @param options - Listener options (must include `pattern: true` at call site).
   * @returns An `off()` function.
   */
  private addPatternListener(once: boolean, pattern: string, handler: any, options: OnOptions) {
    const sep = options.separator ?? ':';
    const compiled = this.compilePatternCached(pattern, sep);

    const entry: CompiledPatternListener<E> = {
      handler,
      kind: compiled.kind,
      match: compiled.match,
      once,
      pattern,
      priority: options.priority ?? compiled.priority,
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

    return this.makeOff(() => {
      this.trieRemove(entry, compiled.compiledSegs);
    });
  }

  /** @internal Throw if this EventBus has been destroyed. */
  private assertNotDestroyed() {
    if (this.destroyed) {
      throw new Error('EventBus instance has been destroyed.');
    }
  }

  /**
   * @internal Compile a pattern into segments + a match function.
   *
   * Special case:
   * - Pattern `"**"` matches any string event and returns empty params.
   *
   * The returned `priority` is higher for more specific patterns (more exact segments).
   *
   * @param pattern - Pattern string (supports `*`, `**`, `{param}`).
   * @param sep - Segment separator (e.g. `:`).
   * @returns Compiled pattern info.
   */
  private compilePattern(pattern: string, sep: string) {
    if (pattern === '**') {
      return {
        compiledSegs: [{ type: 'deepWildcard' as const }] as CompiledSeg[],
        kind: 'wildcard' as const,
        match: () => ({}) as Record<string, string>,
        priority: -100,
      };
    }

    const pSegs = sep ? pattern.split(sep) : [pattern];
    let score = 0;
    let hasStar = false;
    let hasParam = false;

    const compiledSegs: CompiledSeg[] = pSegs.map((seg) => {
      if (seg === '**') {
        hasStar = true;
        return { type: 'deepWildcard' as const };
      }
      if (seg === '*') {
        hasStar = true;
        return { type: 'wildcard' as const };
      }
      if (seg.startsWith('{') && seg.endsWith('}')) {
        hasParam = true;
        return { key: seg.slice(1, -1), type: 'param' as const };
      }

      score += 100;
      return { type: 'exact' as const, value: seg };
    });

    const kind: PatternKind = hasStar ? 'wildcard' : hasParam ? 'param' : 'exact';

    const match = (event: string) => {
      const eSegs = sep ? event.split(sep) : [event];
      const params: Record<string, string> = {};

      let i = 0;
      let j = 0;
      let starI = -1;
      let starJ = -1;

      while (j < eSegs.length) {
        const p = compiledSegs[i];

        if (p) {
          if (p.type === 'deepWildcard') {
            starI = i++;
            starJ = j;
            continue;
          }
          if (p.type === 'wildcard') {
            i++;
            j++;
            continue;
          }
          if (p.type === 'exact') {
            if (p.value === eSegs[j]) {
              i++;
              j++;
              continue;
            }
          } else if (p.type === 'param') {
            params[p.key] = eSegs[j];
            i++;
            j++;
            continue;
          }
        }

        if (starI !== -1) {
          i = starI + 1;
          j = ++starJ;
          continue;
        }

        return null;
      }

      while (compiledSegs[i]?.type === 'deepWildcard') {
        i++;
      }
      return i === compiledSegs.length ? params : null;
    };

    return { compiledSegs, kind, match, priority: score };
  }

  /** @internal Compile and cache patterns per `(pattern, separator)` pair. */
  private compilePatternCached(pattern: string, sep: string) {
    const cacheKey = `${pattern}|${sep}`;
    let compiled = this.patternCache.get(cacheKey);
    if (!compiled) {
      compiled = this.compilePattern(pattern, sep);
      this.patternCache.set(cacheKey, compiled);
    }
    return compiled;
  }

  /** @internal Create a new trie node. */
  private createNode<T extends EventMap>(): TrieNode<T> {
    return {
      end: [],
      exact: new Map(),
      id: ++TRIE_NODE_ID,
    };
  }

  /** @internal Get (or create) the listener set for an exact event key. */
  private getListenerSet<K extends keyof E>(event: K): Set<Listener<E[K]>> {
    let set = this.listenersByEvent.get(event) as Set<Listener<E[K]>> | undefined;
    if (!set) {
      set = new Set();
      this.listenersByEvent.set(event, set as any);
    }
    return set;
  }

  /** @internal Check whether any pattern listener matches the given string event. */
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
   * @internal Dispatch to exact listeners first, then matched pattern listeners in priority order.
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
   * @internal Invoke exact listeners for the given event.
   *
   * Listeners are invoked on a snapshot to avoid issues if listeners mutate the set during dispatch.
   */
  private invokeExactListeners<K extends keyof E>(event: K, payload: E[K]) {
    const set = this.listenersByEvent.get(event);
    if (!set || set.size === 0) {
      return;
    }

    const snapshot = Array.from(set);
    for (const fn of snapshot) {
      this.safeCall(() => fn(payload));
    }
  }

  /** @internal Heuristic to detect `EmitOptions` in overloaded `emit` calls. */
  private looksLikeEmitOptions(x: any): x is EmitOptions {
    return !!x && typeof x === 'object' && ('sticky' in x || 'metaPatch' in x);
  }

  /**
   * Wraps a cleanup function so it can only be executed once.
   *
   * @param fn - Cleanup function to run once.
   * @returns A function that runs `fn` at most once.
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
   * @internal Collect all matching pattern listeners for a string event across all separators.
   *
   * Results are sorted by:
   * - descending priority
   * - ascending registration sequence
   */
  private matchPatternListeners(event: string) {
    const out: Array<{ entry: CompiledPatternListener<E>; params: Record<string, string> }> = [];

    for (const [sep, root] of this.patternTries) {
      const eSegs = sep ? event.split(sep) : [event];
      this.trieMatchCollect(root, eSegs, out);
    }

    out.sort((a, b) => {
      if (b.entry.priority !== a.entry.priority) {
        return b.entry.priority - a.entry.priority;
      }
      return a.entry.seq - b.entry.seq;
    });

    return out;
  }

  /** @internal Parse overloaded `emit` arguments into `[payload, options]`. */
  private parseEmitArgs<P>(
    payloadOrOptions?: EmitOptions | P,
    options?: EmitOptions,
  ): [P | undefined, EmitOptions | undefined] {
    return this.looksLikeEmitOptions(payloadOrOptions)
      ? [undefined, payloadOrOptions]
      : [payloadOrOptions as P | undefined, options];
  }

  /**
   * @internal Push a sticky string event into bounded history.
   */
  private pushStickyEvent(event: string, payload: unknown) {
    this.stickyEvents.push({ event, payload });
    const overflow = this.stickyEvents.length - this.stickyMax;
    if (overflow > 0) {
      this.stickyEvents.splice(0, overflow);
    }
  }

  /** @internal Remove the first occurrence of `item` from `arr` if present. */
  private removeFromArray<T>(arr: T[], item: T) {
    const i = arr.indexOf(item);
    if (i >= 0) {
      arr.splice(i, 1);
    }
  }

  /** @internal Rethrow an error on a microtask so it is not swallowed by async chains. */
  private rethrowAsync(err: unknown) {
    queueMicrotask(() => {
      throw err;
    });
  }

  /**
   * @internal Run middleware pipeline, then dispatch to exact listeners and matched pattern listeners.
   *
   * `next()` must be called at most once per middleware.
   */
  private async runMiddlewares<K extends keyof E>(
    ctx: EmitContext<E, K>,
    rawMatches: Array<{ entry: CompiledPatternListener<E>; params: Record<string, string> }>,
  ): Promise<void> {
    const mws = this.middlewares.slice(); // snapshot
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
   * @internal Safely invoke a user callback and rethrow errors asynchronously.
   *
   * This prevents one listener from breaking the whole dispatch loop while still surfacing errors.
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
   * @internal Fast existence check: return true if any match exists in the trie.
   *
   * Uses the same deep-wildcard expansion guard as `trieMatchCollect`.
   */
  private trieHasAnyMatch(root: TrieNode<E>, eSegs: string[]): boolean {
    type State = { i: number; node: TrieNode<E> };

    const stack: State[] = [{ i: 0, node: root }];

    const expanded = new Set<number>();
    const keyOf = (nodeId: number, i: number) => nodeId * 1_000_000 + i;

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

      if (node.params?.length) {
        for (const p of node.params) {
          stack.push({ i: i + 1, node: p.node });
        }
      }
    }

    return false;
  }

  /**
   * @internal Insert a compiled pattern listener into the trie.
   *
   * This maintains `node.end` sorted by:
   * - descending priority
   * - ascending registration sequence
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

      if (s.type === 'wildcard') {
        node.star ??= this.createNode<E>();
        node = node.star;
        continue;
      }

      if (s.type === 'deepWildcard') {
        node.deep ??= this.createNode<E>();
        node = node.deep;
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
   * @internal Collect matches in the trie using an explicit stack (non-recursive).
   *
   * Deep wildcard (`**`) can match zero or more segments, so this keeps an `expanded` set to avoid
   * repeatedly expanding the same `(node, index)` state via the "match zero segments" path.
   */
  private trieMatchCollect(
    root: TrieNode<E>,
    eSegs: string[],
    out: Array<{ entry: CompiledPatternListener<E>; params: Record<string, string> }>,
  ) {
    type State = { i: number; node: TrieNode<E>; params: Record<string, string> };

    const stack: State[] = [{ i: 0, node: root, params: {} }];

    const expanded = new Set<number>();
    const keyOf = (nodeId: number, i: number) => nodeId * 1_000_000 + i;

    while (stack.length) {
      const st = stack.pop()!;
      const node = st.node;
      const i = st.i;

      if (i === eSegs.length) {
        for (const entry of node.end) {
          out.push({ entry, params: st.params });
        }

        if (node.deep) {
          const k = keyOf(node.deep.id, i);
          if (!expanded.has(k)) {
            expanded.add(k);
            stack.push({ i, node: node.deep, params: st.params });
          }
        }
        continue;
      }

      const seg = eSegs[i];

      if (node.deep) {
        const k0 = keyOf(node.deep.id, i);
        if (!expanded.has(k0)) {
          expanded.add(k0);
          stack.push({ i, node: node.deep, params: st.params });
        }
        stack.push({ i: i + 1, node: node.deep, params: st.params });
      }

      const exactNext = node.exact.get(seg);
      if (exactNext) {
        stack.push({ i: i + 1, node: exactNext, params: st.params });
      }

      if (node.star) {
        stack.push({ i: i + 1, node: node.star, params: st.params });
      }

      if (node.params?.length) {
        for (const p of node.params) {
          stack.push({
            i: i + 1,
            node: p.node,
            params: { ...st.params, [p.key]: seg },
          });
        }
      }
    }
  }

  /**
   * @internal Remove a compiled pattern listener from the trie.
   *
   * Note: this does not currently prune empty nodes.
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
      if (s.type === 'wildcard') {
        node = node.star;
        continue;
      }
      if (s.type === 'deepWildcard') {
        node = node.deep;
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
