import type {
  AnyListener,
  CompiledPatternListener,
  CompiledSeg,
  EmitContext,
  EventMap,
  Listener,
  Middleware,
  PatternKind,
  PatternListenerInfo,
  PatternMiddleware,
  PatternOptions,
} from './types.ts';

/**
 * A strongly-typed EventBus supporting:
 *
 * - Exact event listeners (`on`, `once`)
 * - Global listeners (`onAny`)
 * - Pattern listeners (`onPattern`, `oncePattern`)
 * - Global middleware & pattern middleware
 *
 * Pattern syntax (separator defaults to `:`):
 * - `*`      → single-segment wildcard
 * - `**`     → deep wildcard (matches 0..n segments)
 * - `{name}` → named parameter segment
 * - `?`      → single-character wildcard within a segment (regex-like)
 *
 * Error handling:
 * - Listener errors are rethrown asynchronously via `queueMicrotask`,
 *   so they don't break the current emit loop.
 *
 * Emit modes:
 * - `emit()`     : fire-and-forget (async errors rethrown)
 * - `emitAsync()`: await completion of middleware + listeners
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
 * bus.on('user:login', (p) => console.log(p.id));
 * bus.onPattern('user:{action}', (evt, payload, params) => {
 *   console.log(evt, params?.action);
 * });
 *
 * bus.emit('user:login', { id: '42' });
 * ```
 *
 * @author dafengzhen
 */
export class EventBus<E extends EventMap> {
  /** Listeners that receive all events */
  private anyListeners = new Set<AnyListener<E>>();

  /** Exact listeners mapped by event name */
  private listenersByEvent = new Map<keyof E, Set<Listener<any>>>();

  /** Global middleware chain (runs on every emit) */
  private middlewares: Middleware<E>[] = [];

  /**
   * Pattern-specific middleware chain.
   * Only runs when there is at least one matched pattern listener for the emitted event.
   */
  private patternMiddlewares: PatternMiddleware<E>[] = [];

  /** Compiled pattern listeners, sorted by priority (descending) */
  private patternListeners: CompiledPatternListener<E>[] = [];

  /** Cache for compiled patterns (pattern + separator) */
  private patternCache = new Map<string, ReturnType<EventBus<E>['compilePattern']>>();

  /**
   * Clear listeners and middleware.
   * - If `event` is omitted, clears everything (exact/any/pattern listeners + middleware + cache).
   * - If `event` is provided, only clears exact listeners of that event.
   */
  clear(event?: keyof E): void {
    if (event === undefined) {
      this.listenersByEvent.clear();
      this.anyListeners.clear();
      this.patternListeners.length = 0;
      this.middlewares.length = 0;
      this.patternMiddlewares.length = 0;
      this.patternCache.clear();
      return;
    }
    this.listenersByEvent.delete(event);
  }

  /**
   * Register an exact listener for a specific event.
   * @returns Unsubscribe function
   */
  on<K extends keyof E>(event: K, listener: Listener<E[K]>): () => void {
    this.getListenerSet(event).add(listener);
    return () => this.off(event, listener);
  }

  /**
   * Remove an exact event listener (no-op if missing).
   */
  off<K extends keyof E>(event: K, listener: Listener<E[K]>): void {
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
   * Register an exact listener that runs only once.
   * @returns Unsubscribe function (still works before the first run)
   */
  once<K extends keyof E>(event: K, listener: Listener<E[K]>): () => void {
    const wrapper = ((payload: E[K]) => {
      listener(payload);
      this.off(event, wrapper);
    }) as Listener<E[K]>;

    return this.on(event, wrapper);
  }

  /**
   * Register a listener that receives all events.
   * @returns Unsubscribe function
   */
  onAny(listener: AnyListener<E>): () => void {
    this.anyListeners.add(listener);
    return () => this.anyListeners.delete(listener);
  }

  /**
   * Remove a global (any) listener (no-op if missing).
   */
  offAny(listener: AnyListener<E>): void {
    this.anyListeners.delete(listener);
  }

  /**
   * Register a global (any) listener that runs only once.
   * @returns Unsubscribe function (still works before the first run)
   */
  onceAny(listener: AnyListener<E>): () => void {
    const wrapper: AnyListener<E> = (event, payload) => {
      listener(event, payload);
      this.offAny(wrapper);
    };

    return this.onAny(wrapper);
  }

  /**
   * Register a global middleware (runs on every emit).
   *
   * Middleware must call `next()` exactly once to continue.
   * If it never calls `next()`, dispatch stops.
   *
   * @returns Unsubscribe function
   */
  use(mw: Middleware<E>): () => void {
    this.middlewares.push(mw);
    return () => this.removeFromArray(this.middlewares, mw);
  }

  /**
   * Register a pattern-specific middleware.
   *
   * Only invoked when there is at least one matched pattern listener.
   *
   * Gate semantics:
   * - If a pattern middleware does not call `next()`, dispatch stops (acts like a guard).
   * - `next()` must be called exactly once.
   *
   * @returns Unsubscribe function
   */
  usePattern(mw: PatternMiddleware<E>): () => void {
    this.patternMiddlewares.push(mw);
    return () => this.removeFromArray(this.patternMiddlewares, mw);
  }

  /**
   * Register a pattern listener.
   *
   * Supported syntax (separator defaults to `:`):
   * - `*`      → single-segment wildcard
   * - `**`     → deep wildcard (matches 0..n segments)
   * - `{param}`→ named parameter segment
   * - `?`      → single-character wildcard inside a segment
   *
   * Priority:
   * - If `options.priority` is provided, it overrides auto-derived priority.
   * - Higher priority runs earlier.
   *
   * @param pattern Pattern string
   * @param handler Callback invoked when pattern matches
   * @param options Pattern options
   * @returns Unsubscribe function
   */
  onPattern(
    pattern: string,
    handler: (event: keyof E, payload: E[keyof E], params?: Record<string, string>) => void,
    options?: PatternOptions,
  ): () => void {
    const sep = options?.separator ?? ':';
    const cacheKey = `${pattern}|${sep}`;

    let compiled = this.patternCache.get(cacheKey);
    if (!compiled) {
      compiled = this.compilePattern(pattern, sep);
      this.patternCache.set(cacheKey, compiled);
    }

    const entry: CompiledPatternListener<E> = {
      pattern,
      kind: compiled.kind,
      match: compiled.match,
      priority: options?.priority ?? compiled.priority,
      once: options?.once,
      handler: handler as any,
    };

    this.insertPatternByPriority(entry);
    return () => this.removeFromArray(this.patternListeners, entry);
  }

  /**
   * Register a one-time pattern listener.
   * @returns Unsubscribe function
   */
  oncePattern(
    pattern: string,
    handler: (event: keyof E, payload: E[keyof E], params?: Record<string, string>) => void,
    options?: Omit<PatternOptions, 'once'>,
  ): () => void {
    return this.onPattern(pattern, handler, { ...options, once: true });
  }

  /**
   * Count listeners matching the event.
   *
   * Includes:
   * - Exact listeners for `event`
   * - Global `onAny` listeners
   * - Pattern listeners that match (only when `event` is a string)
   */
  listenerCount(event: keyof E): number {
    let count = 0;
    count += this.listenersByEvent.get(event)?.size ?? 0;
    count += this.anyListeners.size;

    if (typeof event === 'string' && this.patternListeners.length) {
      for (const p of this.patternListeners) {
        if (p.match(event) !== null) {
          count++;
        }
      }
    }

    return count;
  }

  /**
   * Emit an event synchronously (fire-and-forget).
   *
   * - Execution is async internally, but this method does not await.
   * - Listener errors are rethrown asynchronously.
   */
  emit<K extends keyof E>(event: K, ...args: E[K] extends void ? [] : [payload: E[K]]): void {
    if (this.listenerCount(event) === 0) {
      return;
    }

    this._emit(event, ...args).catch((err) => this.rethrowAsync(err));
  }

  /**
   * Emit an event asynchronously.
   * Resolves when middleware + listeners have finished.
   */
  async emitAsync<K extends keyof E>(
    event: K,
    ...args: E[K] extends void ? [] : [payload: E[K]]
  ): Promise<void> {
    if (this.listenerCount(event) === 0) {
      return;
    }
    await this._emit(event, ...args);
  }

  private async _emit<K extends keyof E>(
    event: K,
    ...args: E[K] extends void ? [] : [payload: E[K]]
  ): Promise<void> {
    const payload = args[0] as E[K];
    let blocked = false;

    const ctx: EmitContext<E, K> = {
      get blocked() {
        return blocked;
      },
      event,
      payload,
      matched: [],
      meta: {},
      block() {
        blocked = true;
      },
    };

    await this.runMiddlewares(ctx);
  }

  private async runMiddlewares<K extends keyof E>(ctx: EmitContext<E, K>): Promise<void> {
    const mws = this.middlewares;
    let i = 0;

    const next = async (): Promise<void> => {
      if (ctx.blocked) {
        return;
      }

      if (i >= mws.length) {
        await this.invokeUnifiedDispatch(ctx);
        return;
      }

      const mw = mws[i++];
      let nextCalled = false;

      await mw(ctx, async () => {
        if (nextCalled) {
          throw new Error('next() called multiple times.');
        }
        nextCalled = true;
        await next();
      });
    };

    await next();
  }

  private async invokeUnifiedDispatch(ctx: EmitContext<E, keyof E>): Promise<void> {
    const { event, payload } = ctx;

    const matched =
      typeof event === 'string' && this.patternListeners.length
        ? this.matchPatternListeners(event)
        : [];

    // expose matched info for middleware / debugging
    (ctx as any).matched = Object.freeze(
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
    );

    // pattern middleware only runs if there are matches
    if (matched.length && this.patternMiddlewares.length) {
      for (let i = 0; i < this.patternMiddlewares.length; i++) {
        if (ctx.blocked) {
          return;
        }

        let nextCalled = false;
        const mw = this.patternMiddlewares[i];

        await mw(ctx, async () => {
          if (nextCalled) {
            throw new Error('next() called multiple times.');
          }
          nextCalled = true;
        });

        // guard: if not calling next, stop dispatch
        if (!nextCalled) {
          return;
        }
      }
    }

    // exact + any first
    this.invokeExactAndAnyListeners(event, payload);

    // pattern handlers by priority
    for (const { entry, params } of matched) {
      if (ctx.blocked) {
        return;
      }

      this.safeCall(() => entry.handler(event, payload, params));

      if (entry.once) {
        this.removeFromArray(this.patternListeners, entry);
      }
    }
  }

  private matchPatternListeners(event: string) {
    const matches: Array<{ entry: CompiledPatternListener<E>; params: Record<string, string> }> =
      [];

    for (const entry of this.patternListeners) {
      const params = entry.match(event);
      if (params !== null) {
        matches.push({ entry, params });
      }
    }

    return matches;
  }

  private invokeExactAndAnyListeners<K extends keyof E>(event: K, payload: E[K]): void {
    const set = this.listenersByEvent.get(event);
    if (set) {
      for (const fn of set) {
        this.safeCall(() => fn(payload));
      }
    }

    for (const fn of this.anyListeners) {
      this.safeCall(() => fn(event, payload));
    }
  }

  private getListenerSet<K extends keyof E>(event: K): Set<Listener<E[K]>> {
    let set = this.listenersByEvent.get(event) as Set<Listener<E[K]>> | undefined;
    if (!set) {
      set = new Set();
      this.listenersByEvent.set(event, set);
    }
    return set;
  }

  private insertPatternByPriority(entry: CompiledPatternListener<E>): void {
    const arr = this.patternListeners;
    const { priority } = entry;

    // binary insert (descending)
    let lo = 0;
    let hi = arr.length;

    while (lo < hi) {
      const mid = (lo + hi) >>> 1;
      if (arr[mid].priority >= priority) {
        lo = mid + 1;
      } else {
        hi = mid;
      }
    }

    arr.splice(lo, 0, entry);
  }

  private removeFromArray<T>(arr: T[], item: T): void {
    const i = arr.indexOf(item);
    if (i >= 0) {
      arr.splice(i, 1);
    }
  }

  private safeCall(fn: () => void): void {
    try {
      fn();
    } catch (err) {
      this.rethrowAsync(err);
    }
  }

  private rethrowAsync(err: unknown): void {
    queueMicrotask(() => {
      throw err;
    });
  }

  /**
   * Compile a pattern into a matcher.
   *
   * Notes:
   * - `**` as a segment means "match 0..n segments" (deep wildcard).
   * - `*`  as a segment means "match exactly 1 segment".
   * - If the whole pattern is exactly `"**"`, it matches any event (including empty params).
   */
  private compilePattern(pattern: string, sep: string) {
    // Whole-pattern deep wildcard: match everything
    if (pattern === '**') {
      return {
        kind: 'wildcard' as const,
        priority: -100,
        match: () => ({}) as Record<string, string>,
      };
    }

    const pSegs = pattern.split(sep);
    const keys: string[] = [];
    let score = 0;

    const compiled: CompiledSeg[] = pSegs.map((seg) => {
      if (seg === '**') {
        score -= 5;
        return { type: 'deepWildcard' as const };
      }

      if (seg === '*') {
        score -= 1;
        return { type: 'wildcard' as const };
      }

      if (seg.startsWith('{') && seg.endsWith('}')) {
        const key = seg.slice(1, -1);
        keys.push(key);
        score += 10;
        return { type: 'param' as const, key };
      }

      if (seg.includes('?')) {
        // escape then replace ? with single-char wildcard
        const re = new RegExp(
          '^' + seg.replace(/[.*+?^${}()|[\]\\]/g, '\\$&').replace(/\?/g, '.') + '$',
        );
        score += 5;
        return { type: 'regex' as const, re };
      }

      score += 100;
      return { type: 'exact' as const, value: seg };
    });

    let kind: PatternKind = 'exact';
    if (keys.length) {
      kind = 'param';
    } else if (compiled.some((s) => s.type !== 'exact')) {
      kind = 'wildcard';
    }

    function match(event: string) {
      const eSegs = event.split(sep);
      const params: Record<string, string> = {};

      let i = 0; // pattern index
      let j = 0; // event index

      // backtracking position for deepWildcard (**)
      let starI = -1;
      let starJ = -1;

      while (j < eSegs.length) {
        const p = compiled[i];

        if (p) {
          if (p.type === 'deepWildcard') {
            // ** can match empty; remember and advance pattern
            starI = i++;
            starJ = j;
            continue;
          }

          // one segment match
          if (
            p.type === 'wildcard' ||
            (p.type === 'exact' && p.value === eSegs[j]) ||
            (p.type === 'regex' && p.re.test(eSegs[j])) ||
            (p.type === 'param' && ((params[p.key] = eSegs[j]), true))
          ) {
            i++;
            j++;
            continue;
          }
        }

        // mismatch: if we had a ** before, backtrack to let ** consume one more segment
        if (starI !== -1) {
          i = starI + 1;
          j = ++starJ;
          continue;
        }

        return null;
      }

      // event ended; remaining pattern segments must be ** to match empty
      while (compiled[i]?.type === 'deepWildcard') {
        i++;
      }

      return i === compiled.length ? params : null;
    }

    return {
      kind,
      priority: score,
      match,
    };
  }
}
