/**
 * A registered pattern listener with its compiled matching function.
 *
 * @typeParam E - The event map defining event keys and payload types.
 *
 * @remarks
 * This is an internal normalized representation used for fast dispatch ordering.
 * `match(event)` returns captured params when matched, otherwise `null`.
 */
export type CompiledPatternListener<E extends EventMap> = {
  /** Pattern handler callback. */
  handler: PatternHandler<E>;

  /** Pattern kind derived from the pattern contents. */
  kind: PatternKind;

  /**
   * Match function. Returns captured params when matched, otherwise null.
   *
   * @param event - String event name to match.
   * @returns Captured params if matched, otherwise `null`.
   */
  match: (event: string) => null | Record<string, string>;

  /** Whether this listener should be removed after the first invocation. */
  once?: boolean;

  /** Original pattern string. */
  pattern: string;

  /**
   * Priority for dispatch ordering. Higher values dispatch earlier.
   *
   * @remarks
   * May be explicitly supplied or computed from pattern specificity.
   */
  priority: number;

  /** Separator used to split the event and pattern into segments. */
  separator: string;

  /**
   * Sequence number for stable ordering when priority ties.
   *
   * @remarks
   * Lower sequence numbers are dispatched earlier.
   */
  seq: number;
};

/**
 * A compiled pattern segment.
 *
 * @remarks
 * Segments are derived by splitting the pattern string by the configured separator.
 *
 * - `exact`: literal match (segment must equal `value`)
 * - `param`: captures the segment under `key` (e.g. `{id}`)
 * - `wildcard`: matches exactly one segment (`*`)
 * - `deepWildcard`: matches zero or more segments (`**`)
 */
export type CompiledSeg =
  | { key: string; type: 'param' }
  | { type: 'deepWildcard' }
  | { type: 'exact'; value: string }
  | { type: 'wildcard' };

/**
 * Context object passed through middleware and used during dispatch.
 *
 * @typeParam E - The event map defining event keys and payload types.
 * @typeParam K - The specific emitted event key type for this invocation.
 *
 * @remarks
 * `EmitContext` is created per `emit` and flows through the middleware chain.
 * Middleware can:
 * - read `event`, `payload`, and `matched`,
 * - store data into `meta`,
 * - short-circuit dispatch via `block()`.
 */
export type EmitContext<E extends EventMap, K extends keyof E> = {
  /**
   * Block further propagation.
   *
   * @remarks
   * When called, remaining middleware and/or pattern listeners will not run.
   */
  block(): void;

  /**
   * Whether propagation has been blocked.
   *
   * @remarks
   * Once blocked, remaining middleware and listener dispatch steps will stop.
   */
  readonly blocked: boolean;

  /** The emitted event key. */
  readonly event: K;

  /**
   * A read-only list of pattern listener matches for this emit.
   *
   * @remarks
   * Each entry includes: the pattern, kind, priority, captured params, and handler reference.
   */
  readonly matched: ReadonlyArray<PatternListenerInfo<E>>;

  /**
   * Arbitrary mutable metadata bag for middlewares to share information.
   *
   * @remarks
   * This is intentionally mutable and is not frozen.
   */
  meta: Record<string, unknown>;

  /** The emitted payload. */
  readonly payload: E[K];
};

/**
 * Options used when emitting events.
 */
export type EmitOptions = {
  /**
   * Patch object merged into `ctx.meta` for this emit.
   *
   * @remarks
   * This is a convenience for passing per-emit metadata into middleware.
   */
  metaPatch?: Record<string, unknown>;

  /**
   * Whether to store this emission as "sticky".
   *
   * @remarks
   * Sticky behavior:
   * - For exact listeners: the last payload per exact event key is cached and replayed
   *   immediately to new exact subscribers.
   * - For pattern listeners: string events are appended into a bounded history buffer,
   *   and new pattern subscribers replay matching history entries.
   */
  sticky?: boolean;
};

/**
 * A map of event keys to payload types.
 *
 * Use this to strongly type event names and their payload shapes.
 *
 * @remarks
 * `EventMap` is the foundational type for the event bus. Keys represent event
 * identifiers; values represent the payload type carried by that event.
 *
 * - Use `void` when an event carries no payload.
 * - Keys can be `string | number | symbol` (anything valid as a property key),
 *   though pattern matching typically applies to string keys.
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
 * A listener callback for an exact event.
 *
 * @typeParam T - The payload type for the event.
 *
 * @param payload - Payload for the emitted event.
 */
export type Listener<T> = (payload: T) => void;

/**
 * Middleware function signature.
 *
 * @typeParam E - The event map defining event keys and payload types.
 *
 * @remarks
 * Middleware runs before listener dispatch and composes like Koa:
 * each middleware should `await next()` to continue the chain.
 *
 * If a middleware calls `ctx.block()`, the chain should stop and no further
 * middleware/listeners should run.
 *
 * @param ctx - Emit context for the current emit.
 * @param next - Invoke the next middleware in the chain.
 */
export type Middleware<E extends EventMap> = (
  ctx: EmitContext<E, keyof E>,
  next: () => Promise<void>,
) => Promise<void> | void;

/**
 * Internal middleware entry with an optional match predicate.
 *
 * @typeParam E - The event map defining event keys and payload types.
 *
 * @remarks
 * `match` allows skipping middleware for certain emits without removing it.
 */
export type MiddlewareEntry<E extends EventMap> = {
  /** Middleware implementation. */
  fn: Middleware<E>;

  /**
   * Optional predicate to decide whether this middleware should run.
   *
   * @remarks
   * If provided and returns `false`, the middleware is skipped.
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
   * @remarks
   * In that case, the first argument to `on/once` is a pattern string,
   * and the handler receives `(event, payload, params)`.
   */
  pattern?: boolean;

  /**
   * Explicit priority for a pattern listener.
   *
   * @remarks
   * Higher values run earlier. If omitted, a default is derived from the pattern.
   */
  priority?: number;

  /**
   * Segment separator for pattern matching.
   *
   * @defaultValue `':'`
   */
  separator?: string;
};

/**
 * Handler signature for pattern subscriptions.
 *
 * @typeParam E - The event map defining event keys and payload types.
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
 * The category of a compiled pattern.
 *
 * @remarks
 * Pattern kind is used to optimize matching and to derive default priority
 * (more specific patterns generally dispatch earlier).
 *
 * - `exact`: contains only literal segments (no wildcards, no params)
 * - `param`: contains parameter segments like `{id}`
 * - `wildcard`: contains `*` and/or `**`
 */
export type PatternKind = 'exact' | 'param' | 'wildcard';

/**
 * Metadata describing a pattern listener match for a single `emit`.
 *
 * @typeParam E - The event map defining event keys and payload types.
 *
 * @remarks
 * This information is exposed on `ctx.matched` (read-only) so middleware can inspect:
 * - which pattern listeners will run,
 * - the captured params for each match,
 * - the dispatch ordering (priority),
 * - whether a listener is `once`.
 *
 * Params are captured only when the emitted event is a string and the pattern matches.
 */
export type PatternListenerInfo<E extends EventMap> = {
  /**
   * The registered handler.
   *
   * @remarks
   * Pattern handlers receive the actual emitted event key, the emitted payload, and
   * any captured params.
   *
   * @param event - The actual emitted event key.
   * @param payload - The emitted payload.
   * @param params - Captured parameters (if any).
   */
  handler: (event: keyof E, payload: E[keyof E], params?: Record<string, string>) => void;

  /** The computed pattern kind derived from the pattern contents. */
  kind: PatternKind;

  /** Whether this listener will auto-unsubscribe after being invoked once. */
  once?: boolean;

  /**
   * Captured parameters for this match (e.g. `{id}` -> `params.id`).
   *
   * @remarks
   * This object is intended to be immutable/frozen in the emit context.
   */
  params: Readonly<Record<string, string>>;

  /** The original pattern string as registered by the user. */
  pattern: string;

  /**
   * Listener priority. Higher priority listeners are dispatched earlier.
   *
   * @remarks
   * If not explicitly specified, a default may be derived from pattern specificity
   * (e.g. more exact segments => higher default priority). Ties are typically broken
   * by registration order.
   */
  priority: number;
};

/**
 * A trie node used to index compiled pattern listeners for efficient matching.
 *
 * @typeParam E - The event map defining event keys and payload types.
 *
 * @remarks
 * Typical edges:
 * - `exact`: literal segment edges
 * - `star`: `*` matches exactly one segment
 * - `params`: `{name}` captures one segment and continues
 * - `deep`: `**` matches zero or more segments
 *
 * `end` holds listeners that terminate at this node.
 */
export type TrieNode<E extends EventMap> = {
  /** Deep wildcard edge (`**`) matching zero or more segments. */
  deep?: TrieNode<E>;

  /**
   * Listeners that end at this node.
   *
   * @remarks
   * Usually kept sorted by `(priority desc, seq asc)` for dispatch.
   */
  end: CompiledPatternListener<E>[];

  /** Literal edges keyed by segment value. */
  exact: Map<string, TrieNode<E>>;

  /** Node identifier (useful for debugging / stable references). */
  id: number;

  /**
   * Parameter edges (`{id}`, `{action}`, ...).
   *
   * @remarks
   * Multiple params may coexist at the same depth; `key` indicates capture name.
   */
  params?: Array<{ key: string; node: TrieNode<E> }>;

  /** Single-segment wildcard edge (`*`). */
  star?: TrieNode<E>;
};

/**
 * Options for registering middleware via `bus.use(...)`.
 *
 * @typeParam E - The event map defining event keys and payload types.
 */
export type UseOptions<E extends EventMap> = {
  /**
   * Custom matcher predicate for the middleware.
   *
   * @remarks
   * When provided, it must return `true` for the middleware to run.
   * If both `pattern` and `match` are present, implementations commonly AND them.
   */
  match?: (ctx: EmitContext<E, keyof E>) => boolean;

  /**
   * Run only when at least one registered pattern listener matches the emitted event.
   *
   * @remarks
   * Useful when a middleware should only apply when pattern dispatch is relevant.
   */
  onlyWhenPatternListenerMatched?: boolean;

  /**
   * Run this middleware only when the emitted string event matches the given pattern.
   *
   * @remarks
   * Pattern syntax is the same as pattern listeners (`*`, `**`, `{param}`).
   * Non-string event keys typically won't participate in pattern matching.
   */
  pattern?: string;

  /**
   * Segment separator used by `pattern`.
   *
   * @defaultValue `':'`
   */
  separator?: string;
};
