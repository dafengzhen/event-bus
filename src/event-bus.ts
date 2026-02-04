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
  UseOptions,
} from './types.ts';

/**
 * EventBus - A lightweight event emitter with support for:
 *
 * - Exact event listeners (`on`, `once`, `off`)
 * - Pattern-based listeners (`*`, `**`, `{param}`)
 * - Sticky events (replay last emitted payload for new subscribers)
 * - Middleware pipeline with blocking support
 *
 * Pattern syntax (split by separator, default `:`):
 * - `*` matches exactly one segment
 * - `**` matches zero or more segments
 * - `{name}` captures one segment into params
 *
 * Dispatch order:
 * 1) Middleware chain (in registration order)
 * 2) Exact listeners for the event
 * 3) Matched pattern listeners (in priority order)
 *
 * Notes:
 * - Sticky exact events are stored per exact event key.
 * - Sticky pattern replay stores recent string events in a ring buffer.
 * - Exceptions thrown by listeners are rethrown asynchronously (microtask) to avoid breaking dispatch.
 *
 * @typeParam E - Event map type defining event names and payload types.
 *
 * @example
 * ```ts
 * type Events = {
 *   'user:login': { id: string };
 *   'user:logout': void;
 * };
 *
 * const bus = new EventBus<Events>();
 *
 * bus.on('user:login', (payload) => console.log(payload.id));
 * bus.emit('user:login', { id: '42' });
 *
 * bus.on(
 *   'user:{action}',
 *   (event, payload, params) => console.log(event, params.action),
 *   { pattern: true }
 * );
 * ```
 *
 * @author dafengzhen
 */
export class EventBus<E extends EventMap> {
  /** Exact listeners mapped by event key. */
  private listenersByEvent = new Map<keyof E, Set<Listener<any>>>();

  /** Registered middleware entries (executed before dispatch). */
  private middlewares: MiddlewareEntry<E>[] = [];

  /**
   * Pattern listeners sorted by priority (higher priority first).
   * Only evaluated when the emitted event is a string.
   */
  private patternListeners: CompiledPatternListener<E>[] = [];

  /** Cache for compiled patterns: `${pattern}|${separator}`. */
  private patternCache = new Map<string, ReturnType<EventBus<E>['compilePattern']>>();

  /** Sticky payload storage for exact events (event -> last payload). */
  private stickyExact = new Map<keyof E, unknown>();

  /**
   * Sticky replay buffer for string events used by pattern listeners.
   * Stores recent `{ event, payload }` pairs up to `stickyMax`.
   */
  private stickyEvents: Array<{ event: string; payload: unknown }> = [];

  /** Maximum number of sticky events kept for pattern replay. */
  private stickyMax = 200;

  /**
   * Index for faster pattern candidate collection.
   *
   * Structure:
   * - separator -> firstExactSegment -> set of listeners
   *
   * Only patterns whose first segment is an exact literal are indexed here.
   */
  private patternIndex = new Map<string, Map<string, Set<CompiledPatternListener<E>>>>();

  /**
   * Bucket for patterns without a first exact segment.
   *
   * Structure:
   * - separator -> set of listeners
   *
   * Includes patterns starting with: `*`, `**`, `{param}`.
   */
  private wildcardBucketBySep = new Map<string, Set<CompiledPatternListener<E>>>();

  /**
   * Register a middleware function.
   *
   * Middleware runs before listener dispatch and may:
   * - inspect `ctx.event` / `ctx.payload`
   * - attach arbitrary data to `ctx.meta`
   * - stop propagation by calling `ctx.block()`
   *
   * Matching:
   * - If `options.pattern` is provided, the middleware runs only when the emitted string event
   *   matches that pattern (with `options.separator`, default `:`).
   * - If `options.onlyWhenPatternListenerMatched` is true, it runs only when at least one
   *   registered pattern listener matches the emitted event.
   * - If `options.match` is provided, it must return true for the middleware to run.
   *
   * @param mw - Middleware implementation.
   * @param options - Optional matcher settings.
   * @returns A function that removes the middleware.
   *
   * @example
   * ```ts
   * bus.use(async (ctx, next) => {
   *   ctx.meta.t0 = Date.now();
   *   await next();
   * });
   * ```
   */
  use(mw: Middleware<E>, options?: UseOptions<E>): () => void {
    const matchers: NonNullable<MiddlewareEntry<E>['match']>[] = [];

    if (options?.pattern) {
      const sep = options.separator ?? ':';
      const compiled = this.compilePattern(options.pattern, sep);
      matchers.push(
        (ctx) => typeof ctx.event === 'string' && compiled.match(ctx.event as string) !== null,
      );
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
    return () => this.removeFromArray(this.middlewares, entry);
  }

  /**
   * Subscribe to an exact event.
   *
   * If the event has a sticky payload (emitted with `{ sticky: true }`),
   * the listener is invoked immediately with the last payload.
   *
   * @param event - Exact event key.
   * @param listener - Listener callback.
   * @param options - Subscription options (currently used for overload symmetry).
   * @returns Unsubscribe function.
   */
  on<K extends keyof E>(event: K, listener: Listener<E[K]>, options?: OnOptions): () => void;

  /**
   * Subscribe to a pattern event.
   *
   * Pattern segments are split by `options.separator` (default `:`):
   * - `*` matches exactly one segment
   * - `**` matches zero or more segments
   * - `{name}` captures one segment into params
   *
   * When registering, the handler replays matching sticky events from the internal buffer.
   *
   * @param pattern - Pattern string.
   * @param handler - Pattern handler `(event, payload, params)`.
   * @param options - Must include `{ pattern: true }`. May provide `separator` and `priority`.
   * @returns Unsubscribe function.
   */
  on(
    pattern: string,
    handler: PatternHandler<E>,
    options: OnOptions & { pattern: true },
  ): () => void;

  on(eventOrPattern: any, handler: any, options?: OnOptions): () => void {
    return this.add(false, eventOrPattern, handler, options);
  }

  /**
   * Subscribe to an event once.
   *
   * The listener is removed automatically after the first invocation.
   *
   * @param event - Exact event key.
   * @param listener - Listener callback.
   * @param options - Subscription options (currently used for overload symmetry).
   * @returns Unsubscribe function.
   */
  once<K extends keyof E>(event: K, listener: Listener<E[K]>, options?: OnOptions): () => void;

  /**
   * Subscribe to a pattern event once.
   *
   * The handler is removed automatically after the first match.
   *
   * @param pattern - Pattern string.
   * @param handler - Pattern handler.
   * @param options - Must include `{ pattern: true }`.
   * @returns Unsubscribe function.
   */
  once(
    pattern: string,
    handler: PatternHandler<E>,
    options: OnOptions & { pattern: true },
  ): () => void;

  once(eventOrPattern: any, handler: any, options?: OnOptions): () => void {
    return this.add(true, eventOrPattern, handler, options);
  }

  /**
   * Remove a previously registered exact event listener.
   *
   * @param event - Exact event key.
   * @param listener - Listener reference (must be the same function object passed to `on`).
   */
  off<K extends keyof E>(event: K, listener: Listener<E[K]>): void {
    const set = this.listenersByEvent.get(event);
    if (!set) {
      return;
    }

    set.delete(listener);
    if (!set.size) {
      this.listenersByEvent.delete(event);
    }
  }

  /**
   * Emit an event.
   *
   * This method starts the middleware/dispatch pipeline and returns immediately.
   * Any thrown error from middleware/listeners is rethrown asynchronously via microtask.
   *
   * @param event - Event key.
   * @param payload - Event payload.
   * @param options - Emit options.
   */
  emit<K extends keyof E>(event: K, payload?: E[K], options?: EmitOptions): void;

  /**
   * Emit an event (overload allowing `(event, options)` without payload).
   *
   * @param event - Event key.
   * @param payloadOrOptions - Either payload or EmitOptions.
   * @param options - EmitOptions if payload is provided.
   */
  emit<K extends keyof E>(event: K, payloadOrOptions?: E[K] | EmitOptions, options?: EmitOptions) {
    const [payload, opts] = this.parseEmitArgs<E[K]>(payloadOrOptions, options);
    this._emit(event, payload as any, opts).catch((e) => this.rethrowAsync(e));
  }

  /**
   * Emit an event and wait until middleware chain and dispatch complete.
   *
   * Errors thrown by middleware/listeners will reject this promise.
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
   * Emit an event asynchronously (overload allowing `(event, options)` without payload).
   *
   * @param event - Event key.
   * @param payloadOrOptions - Either payload or EmitOptions.
   * @param options - EmitOptions if payload is provided.
   */
  async emitAsync<K extends keyof E>(
    event: K,
    payloadOrOptions?: E[K] | EmitOptions,
    options?: EmitOptions,
  ): Promise<void> {
    const [payload, opts] = this.parseEmitArgs<E[K]>(payloadOrOptions, options);
    await this._emit(event, payload as any, opts);
  }

  /**
   * Parse emit overload arguments.
   *
   * @param payloadOrOptions - Either payload or options.
   * @param options - Options if payload is provided.
   * @returns `[payload, options]`
   */
  private parseEmitArgs<P>(
    payloadOrOptions?: P | EmitOptions,
    options?: EmitOptions,
  ): [P | undefined, EmitOptions | undefined] {
    return this.looksLikeEmitOptions(payloadOrOptions)
      ? [undefined, payloadOrOptions]
      : [payloadOrOptions as P | undefined, options];
  }

  /**
   * Check whether a value is an {@link EmitOptions} object.
   *
   * @param x - Unknown value.
   * @returns True if value is an options object.
   */
  private looksLikeEmitOptions(x: any): x is EmitOptions {
    return !!x && typeof x === 'object' && 'sticky' in x;
  }

  /**
   * Internal emit pipeline:
   * - store sticky payload (if requested)
   * - collect pattern matches (if event is a string)
   * - build context
   * - run middleware chain
   * - dispatch listeners (exact + pattern)
   *
   * @param event - Event key.
   * @param payload - Payload.
   * @param options - Emit options.
   */
  private async _emit<K extends keyof E>(event: K, payload: E[K], options?: EmitOptions) {
    let blocked = false;

    if (options?.sticky) {
      // Sticky exact replay
      this.stickyExact.set(event, payload as unknown);

      // Sticky pattern replay only stores string events
      if (typeof event === 'string') {
        this.pushStickyEvent(event, payload);
      }
    }

    const matched = typeof event === 'string' ? this.matchPatternListeners(event) : [];

    const ctx: EmitContext<E, K> = {
      get blocked() {
        return blocked;
      },
      event,
      payload,
      matched: Object.freeze(
        matched.map(
          ({ entry, params }): PatternListenerInfo<E> => ({
            pattern: entry.pattern,
            kind: entry.kind,
            once: entry.once,
            priority: entry.priority,
            params: Object.freeze({ ...params }),
            handler: entry.handler,
          }),
        ),
      ),
      meta: {},
      block() {
        blocked = true;
      },
    };

    await this.runMiddlewares(ctx, matched);
  }

  /**
   * Push a string event into the sticky replay buffer.
   *
   * Keeps at most `stickyMax` entries by discarding the oldest.
   *
   * @param event - String event name.
   * @param payload - Payload.
   */
  private pushStickyEvent(event: string, payload: unknown) {
    this.stickyEvents.push({ event, payload });
    const overflow = this.stickyEvents.length - this.stickyMax;
    if (overflow > 0) {
      this.stickyEvents.splice(0, overflow);
    }
  }

  /**
   * Add an exact or pattern listener.
   *
   * @param once - If true, remove after first invocation.
   * @param eventOrPattern - Exact event key or pattern string.
   * @param handler - Listener or pattern handler.
   * @param options - Subscription options.
   * @returns Unsubscribe function.
   */
  private add(
    once: boolean,
    eventOrPattern: keyof E | string,
    handler: any,
    options?: OnOptions,
  ): () => void {
    if (options?.pattern) {
      return this.addPatternListener(once, String(eventOrPattern), handler, options);
    }

    const event = eventOrPattern as keyof E;

    if (!once) {
      this.getListenerSet(event).add(handler);

      // Immediate replay for sticky exact events.
      if (this.stickyExact.has(event)) {
        this.safeCall(() => handler(this.stickyExact.get(event)));
      }

      return () => this.off(event, handler);
    }

    const wrapper = ((payload: any) => {
      handler(payload);
      this.off(event, wrapper);
    }) as Listener<any>;

    return this.on(event, wrapper);
  }

  /**
   * Add a pattern listener and index it for fast candidate lookup.
   *
   * Also replays matching sticky history events.
   *
   * @param once - If true, remove after first match.
   * @param pattern - Pattern string.
   * @param handler - Pattern handler.
   * @param options - Listener options (separator, priority, etc.)
   * @returns Unsubscribe function.
   */
  private addPatternListener(once: boolean, pattern: string, handler: any, options: OnOptions) {
    const sep = options.separator ?? ':';
    const cacheKey = `${pattern}|${sep}`;

    let compiled = this.patternCache.get(cacheKey);
    if (!compiled) {
      this.patternCache.set(cacheKey, (compiled = this.compilePattern(pattern, sep)));
    }

    const entry: CompiledPatternListener<E> = {
      pattern,
      kind: compiled.kind,
      separator: sep,
      match: compiled.match,
      priority: options.priority ?? compiled.priority,
      once,
      handler,
    };

    this.insertPatternByPriority(entry);
    this.indexPatternListener(entry, compiled.firstExact);

    // Replay matching sticky events (pattern listeners only).
    for (const s of this.stickyEvents) {
      const params = compiled.match(s.event);
      if (params) {
        this.safeCall(() => handler(s.event as any, s.payload as any, params));
      }
    }

    return () => {
      this.removeFromArray(this.patternListeners, entry);
      this.unindexPatternListener(entry, compiled!.firstExact);
    };
  }

  /**
   * Run the middleware pipeline.
   *
   * Middlewares are executed in registration order. Each middleware can call `next()`
   * to continue or stop the pipeline by not calling it / calling `ctx.block()`.
   *
   * @param ctx - Emit context.
   * @param rawMatches - Precomputed pattern matches for this event (used for dispatch).
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

      // Skip middleware if match predicate fails.
      if (entry.match && !entry.match(ctx as any)) {
        return next();
      }

      // Enforce single-call next()
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
   * Dispatch exact listeners and then matched pattern listeners.
   *
   * If `ctx.block()` is called during dispatch, remaining pattern listeners are skipped.
   * Pattern listeners registered with `once` are removed after invocation.
   *
   * @param ctx - Emit context.
   * @param matched - Matched pattern listeners with resolved params.
   */
  private async invokeDispatch(
    ctx: EmitContext<E, keyof E>,
    matched: Array<{ entry: CompiledPatternListener<E>; params: Record<string, string> }>,
  ) {
    // Exact listeners first.
    this.invokeExactListeners(ctx.event, ctx.payload);

    // Then pattern listeners.
    for (const { entry, params } of matched) {
      if (ctx.blocked) {
        return;
      }

      this.safeCall(() => entry.handler(ctx.event, ctx.payload, params));

      if (entry.once) {
        this.removeFromArray(this.patternListeners, entry);
        this.unindexPatternListener(entry, this.getCompiledFirstExact(entry));
      }
    }
  }

  /**
   * Match all pattern listeners for a given string event.
   *
   * Candidate listeners are collected via indexing, then fully matched.
   *
   * @param event - String event.
   * @returns Array of `{ entry, params }` for matches (ordered by priority due to insertion order).
   */
  private matchPatternListeners(event: string) {
    const out: Array<{ entry: CompiledPatternListener<E>; params: Record<string, string> }> = [];
    for (const entry of this.collectCandidates(event)) {
      const params = entry.match(event);
      if (params) {
        out.push({ entry, params });
      }
    }
    return out;
  }

  /**
   * Check whether any pattern listener matches the given string event.
   *
   * @param event - String event.
   * @returns True if at least one pattern listener matches.
   */
  private hasAnyPatternMatch(event: string): boolean {
    for (const entry of this.collectCandidates(event)) {
      if (entry.match(event)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Collect a de-duplicated set of candidate pattern listeners for the given event.
   *
   * Uses:
   * - `patternIndex` for patterns whose first segment is exact
   * - `wildcardBucketBySep` for wildcard-first patterns
   *
   * @param event - String event.
   * @returns Candidate set.
   */
  private collectCandidates(event: string): Set<CompiledPatternListener<E>> {
    const out = new Set<CompiledPatternListener<E>>();

    // For separators that have an index.
    for (const [sep, byFirst] of this.patternIndex) {
      const first = this.firstSeg(event, sep);
      const exact = byFirst.get(first);
      if (exact) {
        for (const e of exact) {
          out.add(e);
        }
      }

      const wild = this.wildcardBucketBySep.get(sep);
      if (wild) {
        for (const e of wild) {
          out.add(e);
        }
      }
    }

    // For separators that only have wildcard bucket (no index created yet).
    for (const [sep, wild] of this.wildcardBucketBySep) {
      if (this.patternIndex.has(sep)) {
        continue;
      }
      for (const e of wild) {
        out.add(e);
      }
    }

    return out;
  }

  /**
   * Return the first segment of an event name for a given separator.
   *
   * @param event - String event.
   * @param sep - Separator.
   * @returns First segment (or the whole event if separator not found).
   */
  private firstSeg(event: string, sep: string): string {
    const idx = event.indexOf(sep);
    return idx === -1 ? event : event.slice(0, idx);
  }

  /**
   * Invoke all exact listeners for an event.
   *
   * Listener exceptions are rethrown asynchronously.
   *
   * @param event - Exact event key.
   * @param payload - Payload.
   */
  private invokeExactListeners<K extends keyof E>(event: K, payload: E[K]) {
    const set = this.listenersByEvent.get(event);
    if (!set) {
      return;
    }
    for (const fn of set) {
      this.safeCall(() => fn(payload));
    }
  }

  /**
   * Get (or create) the listener set for an exact event.
   *
   * @param event - Exact event key.
   * @returns Listener set.
   */
  private getListenerSet<K extends keyof E>(event: K): Set<Listener<E[K]>> {
    let set = this.listenersByEvent.get(event) as Set<Listener<E[K]>> | undefined;
    if (!set) {
      this.listenersByEvent.set(event, (set = new Set()));
    }
    return set;
  }

  /**
   * Insert a pattern listener into `patternListeners` using binary search.
   *
   * Ordering rule:
   * - Higher priority entries appear earlier in the list.
   *
   * @param entry - Pattern listener.
   */
  private insertPatternByPriority(entry: CompiledPatternListener<E>) {
    const arr = this.patternListeners;
    let lo = 0;
    let hi = arr.length;

    while (lo < hi) {
      const mid = (lo + hi) >>> 1;
      if (arr[mid].priority >= entry.priority) {
        lo = mid + 1;
      } else {
        hi = mid;
      }
    }
    arr.splice(lo, 0, entry);
  }

  /**
   * Remove a single item from an array (no-op if missing).
   *
   * @param arr - Array.
   * @param item - Item to remove.
   */
  private removeFromArray<T>(arr: T[], item: T) {
    const i = arr.indexOf(item);
    if (i >= 0) {
      arr.splice(i, 1);
    }
  }

  /**
   * Execute a function and rethrow any error asynchronously.
   *
   * @param fn - Function to execute.
   */
  private safeCall(fn: () => void) {
    try {
      fn();
    } catch (err) {
      this.rethrowAsync(err);
    }
  }

  /**
   * Rethrow an error asynchronously using `queueMicrotask`.
   *
   * @param err - Error.
   */
  private rethrowAsync(err: unknown) {
    queueMicrotask(() => {
      throw err;
    });
  }

  /**
   * Compile a pattern into a matcher function and meta information.
   *
   * Special case:
   * - pattern `"**"` matches all events and yields empty params.
   *
   * Priority:
   * - Exact segments contribute +100 each.
   * - Patterns with more exact literals are matched earlier by default.
   *
   * Matching algorithm:
   * - Greedy scan with backtracking support for `**`.
   *
   * @param pattern - Pattern string.
   * @param sep - Segment separator.
   * @returns Compiled pattern info.
   */
  private compilePattern(pattern: string, sep: string) {
    if (pattern === '**') {
      return {
        kind: 'wildcard' as const,
        priority: -100,
        firstExact: null as string | null,
        match: () => ({}) as Record<string, string>,
      };
    }

    const pSegs = pattern.split(sep);
    let score = 0;
    let hasStar = false;
    let hasParam = false;
    let firstExact: string | null = null;

    const compiled: CompiledSeg[] = pSegs.map((seg, idx) => {
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
        return { type: 'param' as const, key: seg.slice(1, -1) };
      }

      score += 100;
      if (idx === 0) {
        firstExact = seg;
      }
      return { type: 'exact' as const, value: seg };
    });

    const kind: PatternKind = hasStar ? 'wildcard' : hasParam ? 'param' : 'exact';

    const match = (event: string) => {
      const eSegs = event.split(sep);
      const params: Record<string, string> = {};

      let i = 0;
      let j = 0;
      let starI = -1;
      let starJ = -1;

      while (j < eSegs.length) {
        const p = compiled[i];

        if (p) {
          if (p.type === 'deepWildcard') {
            // Remember position of `**` and proceed with pattern.
            starI = i++;
            starJ = j;
            continue;
          }
          if (p.type === 'wildcard') {
            // `*` consumes exactly one segment.
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
            // `{param}` captures a segment.
            params[p.key] = eSegs[j];
            i++;
            j++;
            continue;
          }
        }

        // Backtrack to last `**` if available.
        if (starI !== -1) {
          i = starI + 1;
          j = ++starJ;
          continue;
        }

        return null;
      }

      // Allow trailing `**` to match empty suffix.
      while (compiled[i]?.type === 'deepWildcard') {
        i++;
      }
      return i === compiled.length ? params : null;
    };

    return { kind, priority: score, firstExact, match };
  }

  /**
   * Index a pattern listener into fast-lookup structures.
   *
   * @param entry - Pattern listener.
   * @param firstExact - First exact segment if present; otherwise null.
   */
  private indexPatternListener(entry: CompiledPatternListener<E>, firstExact: string | null) {
    const sep = entry.separator;

    if (firstExact) {
      let byFirst = this.patternIndex.get(sep);
      if (!byFirst) {
        this.patternIndex.set(sep, (byFirst = new Map()));
      }

      let set = byFirst.get(firstExact);
      if (!set) {
        byFirst.set(firstExact, (set = new Set()));
      }

      set.add(entry);
      return;
    }

    let wild = this.wildcardBucketBySep.get(sep);
    if (!wild) {
      this.wildcardBucketBySep.set(sep, (wild = new Set()));
    }
    wild.add(entry);
  }

  /**
   * Remove a pattern listener from index structures.
   *
   * @param entry - Pattern listener.
   * @param firstExact - First exact segment if present; otherwise null.
   */
  private unindexPatternListener(entry: CompiledPatternListener<E>, firstExact: string | null) {
    const sep = entry.separator;

    if (firstExact) {
      const byFirst = this.patternIndex.get(sep);
      const set = byFirst?.get(firstExact);
      if (!set) {
        return;
      }

      set.delete(entry);
      if (set.size) {
        return;
      }

      byFirst!.delete(firstExact);
      if (!byFirst!.size) {
        this.patternIndex.delete(sep);
      }
      return;
    }

    const wild = this.wildcardBucketBySep.get(sep);
    if (!wild) {
      return;
    }

    wild.delete(entry);
    if (!wild.size) {
      this.wildcardBucketBySep.delete(sep);
    }
  }

  /**
   * Compute the first exact segment from the pattern string.
   *
   * Returns null if the first segment is:
   * - `*` / `**`
   * - `{param}`
   *
   * @param entry - Pattern listener entry.
   * @returns First exact segment or null.
   */
  private getCompiledFirstExact(entry: CompiledPatternListener<E>) {
    const first = entry.pattern.split(entry.separator, 1)[0];
    if (first === '*' || first === '**') {
      return null;
    }
    if (first.startsWith('{') && first.endsWith('}')) {
      return null;
    }
    return first;
  }
}
