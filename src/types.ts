/**
 * A map of event keys to payload types.
 *
 * Use this to strongly type event names and their payload shapes:
 *
 * @example
 * ```ts
 * type Events = {
 *   'user:login': { id: string };
 *   'user:logout': void;
 * };
 * ```
 */
export type EventMap = Record<PropertyKey, any>;

/**
 * A listener for exact events.
 */
export type Listener<T> = (payload: T) => void;

/**
 * The category of a compiled pattern.
 *
 * - `exact`: contains only literal segments (no wildcards, no params)
 * - `param`: contains parameter segments like `{id}`
 * - `wildcard`: contains `*` and/or `**`
 */
export type PatternKind = 'exact' | 'param' | 'wildcard';

/**
 * A compiled pattern segment.
 *
 * - `exact`: literal match (segment must equal `value`)
 * - `param`: captures the segment under `key`
 * - `wildcard`: matches exactly one segment (`*`)
 * - `deepWildcard`: matches zero or more segments (`**`)
 */
export type CompiledSeg =
  | { type: 'exact'; value: string }
  | { type: 'param'; key: string }
  | { type: 'wildcard' }
  | { type: 'deepWildcard' };

/**
 * Metadata describing a pattern listener match for a single emit.
 *
 * This is provided to middleware via `ctx.matched` (read-only),
 * so middleware can inspect what will be dispatched (patterns, params, etc.).
 */
export type PatternListenerInfo<E extends EventMap> = {
  /** The original pattern string as registered by the user. */
  pattern: string;

  /** The computed pattern kind. */
  kind: PatternKind;

  /** Whether this listener will auto-unsubscribe after being invoked once. */
  once?: boolean;

  /**
   * Listener priority. Higher priority listeners are dispatched earlier.
   *
   * If not explicitly specified, this typically derives from the pattern specificity
   * (more exact segments => higher default priority).
   */
  priority: number;

  /**
   * Captured parameters for this match (e.g. `{id}` -> `params.id`).
   *
   * This object is frozen in the emit context.
   */
  params: Readonly<Record<string, string>>;

  /**
   * The registered handler.
   *
   * @param event - The actual emitted event key.
   * @param payload - The emitted payload.
   * @param params - Captured parameters (if any).
   */
  handler: (event: keyof E, payload: E[keyof E], params?: Record<string, string>) => void;
};

/**
 * Context object passed through middleware and used during dispatch.
 */
export type EmitContext<E extends EventMap, K extends keyof E> = {
  /**
   * Whether propagation has been blocked.
   *
   * Once blocked, remaining middleware and pattern dispatch steps will stop.
   */
  readonly blocked: boolean;

  /** The emitted event key. */
  readonly event: K;

  /** The emitted payload. */
  readonly payload: E[K];

  /**
   * A read-only list of pattern listener matches for this emit.
   *
   * Each entry includes the pattern, kind, priority, captured params, and handler reference.
   */
  readonly matched: ReadonlyArray<PatternListenerInfo<E>>;

  /**
   * Arbitrary mutable metadata bag for middlewares to share information.
   *
   * This is not frozen and can be modified by middleware.
   */
  meta: Record<string, unknown>;

  /**
   * Block further propagation.
   *
   * When called, remaining middleware and/or pattern listeners will not run.
   */
  block(): void;
};

/**
 * Middleware function signature.
 *
 * Middleware runs before dispatch and composes like Koa:
 * each middleware should `await next()` to continue the chain.
 *
 * @param ctx - Emit context for the current emit.
 * @param next - Invoke the next middleware in the chain.
 */
export type Middleware<E extends EventMap> = (
  ctx: EmitContext<E, keyof E>,
  next: () => Promise<void>,
) => void | Promise<void>;

/**
 * Internal middleware entry with an optional match predicate.
 */
export type MiddlewareEntry<E extends EventMap> = {
  /** Middleware implementation. */
  fn: Middleware<E>;

  /**
   * Optional predicate to decide whether this middleware should run.
   *
   * If provided and returns false, the middleware is skipped.
   */
  match?: (ctx: EmitContext<E, keyof E>) => boolean;
};

/**
 * Options for registering middleware via `bus.use(...)`.
 */
export type UseOptions<E extends EventMap> = {
  /**
   * Run this middleware only when the emitted string event matches the given pattern.
   *
   * Pattern syntax is the same as pattern listeners (`*`, `**`, `{param}`).
   */
  pattern?: string;

  /**
   * Segment separator used by `pattern`.
   *
   * Defaults to `':'`.
   */
  separator?: string;

  /**
   * Run only when at least one registered pattern listener matches the emitted event.
   *
   * Useful if a middleware should only apply when pattern dispatch is relevant.
   */
  onlyWhenPatternListenerMatched?: boolean;

  /**
   * Custom matcher predicate for the middleware.
   *
   * When provided, it must return true for the middleware to run.
   */
  match?: (ctx: EmitContext<E, keyof E>) => boolean;
};

/**
 * Options for subscribing listeners via `on(...)` / `once(...)`.
 */
export type OnOptions = {
  /**
   * When true, the subscription is treated as a pattern listener.
   *
   * In that case, the first argument to `on/once` is a pattern string,
   * and the handler receives `(event, payload, params)`.
   */
  pattern?: boolean;

  /**
   * Segment separator for pattern matching.
   *
   * Defaults to `':'`.
   */
  separator?: string;

  /**
   * Explicit priority for a pattern listener.
   *
   * Higher values run earlier. If omitted, a default is derived from the pattern.
   */
  priority?: number;
};

/**
 * Handler signature for pattern subscriptions.
 *
 * @param event - The emitted event key.
 * @param payload - The emitted payload.
 * @param params - Captured parameters (if pattern uses `{name}` segments).
 */
export type PatternHandler<E extends EventMap> = (
  event: keyof E,
  payload: E[keyof E],
  params?: Record<string, string>,
) => void;

/**
 * A registered pattern listener with its compiled matching function.
 */
export type CompiledPatternListener<E extends EventMap> = {
  /** Original pattern string. */
  pattern: string;

  /** Pattern kind derived from the pattern contents. */
  kind: PatternKind;

  /** Separator used to split the event and pattern into segments. */
  separator: string;

  /**
   * Match function. Returns captured params when matched, otherwise null.
   *
   * @param event - String event name to match.
   */
  match: (event: string) => Record<string, string> | null;

  /**
   * Priority for dispatch ordering. Higher values dispatch earlier.
   *
   * May be explicitly supplied or computed from pattern specificity.
   */
  priority: number;

  /** Whether this listener should be removed after the first invocation. */
  once?: boolean;

  /** Pattern handler callback. */
  handler: PatternHandler<E>;
};

/**
 * Options used when emitting events.
 */
export type EmitOptions = {
  /**
   * Whether to store this emission as "sticky".
   *
   * Sticky behavior:
   * - For exact listeners: the last payload per exact event key is cached and replayed
   *   immediately to new exact subscribers.
   * - For pattern listeners: string events are appended into a bounded history buffer,
   *   and new pattern subscribers replay matching history entries.
   */
  sticky?: boolean;
};
