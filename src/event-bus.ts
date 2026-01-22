import type {
  AnyListener,
  CompiledPatternListener,
  EmitContext,
  EventMap,
  Listener,
  MatchKeys,
  Middleware,
  Pattern,
  PatternKind,
  PatternListenerInfo,
  PatternMiddleware,
  PatternOptions,
} from './types.ts';

/**
 * EventBus.
 *
 * @author dafengzhen
 */
export class EventBus<E extends EventMap> {
  private anyListeners = new Set<AnyListener<E>>();

  private listenersByEvent = new Map<keyof E, Set<Listener<any>>>();

  private middlewares: Middleware<E>[] = [];

  private patternMiddlewares: PatternMiddleware<E>[] = [];

  private patternListeners: CompiledPatternListener<E>[] = [];

  clear(event?: keyof E): void {
    if (event === undefined) {
      this.listenersByEvent.clear();
      this.anyListeners.clear();
      this.patternListeners.length = 0;
      this.middlewares.length = 0;
      this.patternMiddlewares.length = 0;
      return;
    }

    this.listenersByEvent.delete(event);
  }

  on<K extends keyof E>(event: K, listener: Listener<E[K]>): () => void {
    this.getListenerSet(event).add(listener);
    return () => this.off(event, listener);
  }

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

  once<K extends keyof E>(event: K, listener: Listener<E[K]>): () => void {
    const off = this.on(event, ((payload: E[K]) => {
      off();
      listener(payload);
    }) as Listener<E[K]>);

    return off;
  }

  onAny(listener: AnyListener<E>): () => void {
    this.anyListeners.add(listener);
    return () => this.anyListeners.delete(listener);
  }

  use(mw: Middleware<E>): () => void {
    this.middlewares.push(mw);
    return () => this.removeFromArray(this.middlewares, mw);
  }

  usePattern(mw: PatternMiddleware<E>): () => void {
    this.patternMiddlewares.push(mw);
    return () => this.removeFromArray(this.patternMiddlewares, mw);
  }

  onPattern<P extends Pattern<Extract<keyof E, string>>>(
    pattern: P,
    handler: <K extends keyof E & MatchKeys<Extract<keyof E, string>, P>>(
      event: K,
      payload: E[K],
      params?: Record<string, string>,
    ) => void,
    options?: PatternOptions,
  ): () => void {
    const compiled = this.compilePatternMatcher(pattern);

    const prefix =
      typeof pattern === 'string' && pattern.endsWith(':*') ? pattern.slice(0, -2) : undefined;

    const entry: CompiledPatternListener<E> = {
      pattern,
      kind: compiled.kind,
      match: compiled.match,
      priority: options?.priority ?? compiled.priority,
      once: options?.once,
      prefix,
      handler: handler as any,
    };

    this.insertPatternByPriority(entry);

    return () => this.removeFromArray(this.patternListeners, entry);
  }

  oncePattern<P extends Pattern<Extract<keyof E, string>>>(
    pattern: P,
    handler: <K extends keyof E & MatchKeys<Extract<keyof E, string>, P>>(
      event: K,
      payload: E[K],
      params?: Record<string, string>,
    ) => void,
    options?: Omit<PatternOptions, 'once'>,
  ): () => void {
    return this.onPattern(pattern, handler, { ...options, once: true });
  }

  listenerCount(event: keyof E): number {
    let count = 0;
    count += this.listenersByEvent.get(event)?.size ?? 0;
    count += this.anyListeners.size;

    if (typeof event === 'string') {
      for (const p of this.patternListeners) {
        if (p.match(event) !== null) {
          count++;
        }
      }
    }

    return count;
  }

  emit<K extends keyof E>(event: K, ...args: E[K] extends void ? [] : [payload: E[K]]): void {
    void this._emit(event, ...args).catch((err) => {
      queueMicrotask(() => {
        throw err;
      });
    });
  }

  emitAsync<K extends keyof E>(
    event: K,
    ...args: E[K] extends void ? [] : [payload: E[K]]
  ): Promise<void> {
    return this._emit(event, ...args);
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
    let index = -1;

    const dispatch = async (n: number): Promise<void> => {
      if (ctx.blocked) {
        return;
      }

      if (n <= index) {
        throw new Error('next() called multiple times.');
      }

      index = n;

      const mw = this.middlewares[n];
      if (!mw) {
        await this.invokeUnifiedDispatch(ctx);
        return;
      }
      await mw(ctx, () => dispatch(n + 1));
    };

    await dispatch(0);
  }

  private async invokeUnifiedDispatch(ctx: EmitContext<E, keyof E>): Promise<void> {
    const matched: Array<{ entry: CompiledPatternListener<E>; params: Record<string, string> }> =
      [];

    if (typeof ctx.event === 'string') {
      for (const entry of this.patternListeners) {
        const params = entry.match(ctx.event);
        if (params !== null) {
          matched.push({ entry, params });
        }
      }
    }

    (ctx as any).matched = matched.map(
      (m): PatternListenerInfo<E> => ({
        handler: m.entry.handler,
        kind: m.entry.kind,
        once: m.entry.once,
        priority: m.entry.priority,
        prefix: m.entry.prefix,
      }),
    );

    let index = -1;

    const dispatch = async (n: number): Promise<void> => {
      if (ctx.blocked) {
        return;
      }

      if (n <= index) {
        throw new Error('next() called multiple times.');
      }
      index = n;

      const mw = this.patternMiddlewares[n];
      if (!mw) {
        this.invokeExactAndAnyListeners(ctx.event, ctx.payload);

        for (const { entry, params } of matched) {
          if (ctx.blocked) {
            return;
          }
          this.safeCall(() => entry.handler(ctx.event, ctx.payload, params));
          if (entry.once) {
            this.removeFromArray(this.patternListeners, entry);
          }
        }
        return;
      }

      await mw(ctx, () => dispatch(n + 1));
    };

    await dispatch(0);
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
    let lo = 0;
    let hi = this.patternListeners.length;

    while (lo < hi) {
      const mid = (lo + hi) >>> 1;
      if (this.patternListeners[mid].priority >= entry.priority) {
        lo = mid + 1;
      } else {
        hi = mid;
      }
    }

    this.patternListeners.splice(lo, 0, entry);
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
      queueMicrotask(() => {
        throw err;
      });
    }
  }

  private escapeRegexLiteral(s: string): string {
    return s.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
  }

  private compilePatternMatcher(p: string): {
    kind: PatternKind;
    priority: number;
    match: (event: string) => null | Record<string, string>;
  } {
    if (p === '*') {
      return { kind: 'star', priority: 0, match: () => ({}) };
    }

    if (p.endsWith(':*')) {
      const prefix = p.slice(0, -2);
      if (!prefix) {
        throw new Error(`Invalid pattern: ${p}.`);
      }

      const prefixWithColon = prefix + ':';

      return {
        kind: 'prefix',
        priority: 10,
        match: (event: string) =>
          event === prefix || event.startsWith(prefixWithColon) ? {} : null,
      };
    }

    if (p.includes('{')) {
      const keys: string[] = [];
      const parts = p.split(/\{(\w+)}/g);

      let src = '^';
      for (let i = 0; i < parts.length; i++) {
        if (i % 2 === 0) {
          src += this.escapeRegexLiteral(parts[i]);
        } else {
          keys.push(parts[i]);
          src += '([^:]+)';
        }
      }
      src += '$';

      const regex = new RegExp(src);

      return {
        kind: 'param',
        priority: 100 + keys.length,
        match: (event: string) => {
          const m = event.match(regex);
          if (!m) {
            return null;
          }

          const params: Record<string, string> = {};
          for (let i = 0; i < keys.length; i++) {
            params[keys[i]] = m[i + 1];
          }
          return params;
        },
      };
    }

    return {
      kind: 'exact',
      priority: 10_000,
      match: (event: string) => (event === p ? {} : null),
    };
  }
}
