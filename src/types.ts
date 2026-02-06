export type CompiledPatternListener<E extends EventMap> = {
  /**
   * The original handler associated with the pattern listener.
   */
  handler: PatternHandler<E>;

  /**
   * Function used to test an event name against the compiled pattern.
   *
   * @param event - The event name to match (raw string form, not segmented).
   * @returns A params object when matched, otherwise `null`.
   */
  match: (event: string) => null | Record<string, string>;

  /**
   * Whether this listener should be removed after the first successful match.
   */
  once: boolean;

  /**
   * The original pattern string provided by the user.
   */
  pattern: string;

  /**
   * Listener priority. Higher values run earlier (or are resolved first,
   * depending on the dispatcher implementation).
   */
  priority: number;

  /**
   * The segment separator used to parse the pattern and incoming event name.
   * For example: "." or "/".
   */
  separator: string;

  /**
   * A monotonically increasing sequence number used to ensure a stable
   * ordering when priorities are equal.
   */
  seq: number;
};

export type CompiledSeg =
  /**
   * A named parameter segment such as `:id`.
   */
  | { key: string; type: 'param' }
  /**
   * A glob segment compiled into a regular expression, e.g. `*`/`{...}`-style
   * patterns depending on your DSL.
   */
  | { re: RegExp; src: string; type: 'glob' }
  /**
   * A deep wildcard that can span multiple segments (e.g. `**`).
   */
  | { type: 'deepWildcard' }
  /**
   * An exact literal segment.
   */
  | { type: 'exact'; value: string }
  /**
   * A single-segment wildcard (e.g. `*` matching exactly one segment).
   */
  | { type: 'segWildcard' };

export type EmitContext<E extends EventMap, K extends keyof E> = {
  /**
   * Prevent further middleware/handlers from running for the current emission.
   * Typically used for short-circuiting or "stop propagation" semantics.
   */
  block(): void;

  /**
   * Indicates whether the current emission has been blocked.
   */
  readonly blocked: boolean;

  /**
   * The emitted event key.
   */
  event: K;

  /**
   * Pattern listeners that matched this emission, in the resolved order.
   */
  matched: ReadonlyArray<PatternListenerInfo<E>>;

  /**
   * Arbitrary metadata carried alongside the emission (e.g. tracing, source).
   */
  meta: Record<string, unknown>;

  /**
   * The payload associated with the emitted event.
   */
  payload: E[K];
};

export type EmitOptions = {
  /**
   * Shallow patch applied to `ctx.meta` for this emission.
   * Useful for injecting per-emit metadata without mutating global state.
   */
  metaPatch?: Record<string, unknown>;

  /**
   * Whether this emission should be "sticky" (implementation-defined),
   * e.g. cached for late subscribers or replayed to new listeners.
   */
  sticky?: boolean;
};

/**
 * Event-to-payload map.
 *
 * Example:
 * ```ts
 * type Events = {
 *   'user.created': { id: string };
 *   'user.deleted': { id: string };
 * };
 * ```
 */
export type EventMap = Record<string, any>;

/**
 * Typed listener for a specific payload type.
 */
export type Listener<P> = (payload: P) => void;

export type Middleware<E extends EventMap> = (
  /**
   * The per-emission context object.
   */
  ctx: EmitContext<E, keyof E>,
  /**
   * Call to continue to the next middleware in the chain.
   */
  next: () => Promise<void>,
) => Promise<void> | void;

export type MiddlewareEntry<E extends EventMap> = {
  /**
   * Middleware function.
   */
  fn: Middleware<E>;

  /**
   * Optional predicate to decide whether to run this middleware for the
   * current emission.
   */
  match?: (ctx: EmitContext<E, keyof E>) => boolean;
};

export type OnOptions = {
  /**
   * Listener priority. Higher values run earlier (or are resolved first,
   * depending on the dispatcher implementation).
   */
  priority?: number;

  /**
   * Segment separator used to interpret pattern/event names for this listener.
   * Overrides the default separator if provided.
   */
  separator?: string;
};

export type PatternHandler<E extends EventMap> = (
  /**
   * The event name that triggered the pattern match (may be a known key of `E`
   * or an arbitrary string).
   */
  event: keyof E | string,
  /**
   * The payload associated with the emitted event (unknown at pattern level).
   */
  payload: unknown,
  /**
   * Parameters extracted from the pattern match.
   */
  params: Record<string, string>,
) => void;

export type PatternListenerInfo<E extends EventMap> = {
  /**
   * The original handler associated with the listener.
   */
  handler: PatternHandler<E>;

  /**
   * Whether this listener should be removed after the first successful match.
   */
  once: boolean;

  /**
   * Readonly parameters extracted from the pattern match.
   */
  params: Readonly<Record<string, string>>;

  /**
   * The pattern string used to register the listener.
   */
  pattern: string;

  /**
   * Listener priority.
   */
  priority: number;
};

export type TrieNode<E extends EventMap> = {
  /**
   * Child node representing a deep wildcard path (e.g. `**`).
   */
  deep?: TrieNode<E>;

  /**
   * Listeners that end exactly at this node (i.e. full pattern match).
   */
  end: CompiledPatternListener<E>[];

  /**
   * Exact literal children keyed by segment value.
   */
  exact: Map<string, TrieNode<E>>;

  /**
   * Glob children compiled as regexes. Each entry pairs the child node with
   * its compiled matcher and original source.
   */
  globs?: Array<{ node: TrieNode<E>; re: RegExp; src: string }>;

  /**
   * Unique node id, useful for debugging or stable traversal ordering.
   */
  id: number;

  /**
   * Parameter children (named segments). Each entry stores the param key and
   * the corresponding child node.
   */
  params?: Array<{ key: string; node: TrieNode<E> }>;

  /**
   * Child node representing a single-segment wildcard (e.g. `*`).
   */
  star?: TrieNode<E>;
};

export type UseOptions<E extends EventMap> = {
  /**
   * Optional predicate that decides whether the middleware should run for the
   * current emission.
   */
  match?: (ctx: EmitContext<E, keyof E>) => boolean;

  /**
   * If true, only run the middleware when at least one pattern listener
   * matched the current event.
   */
  onlyWhenPatternListenerMatched?: boolean;

  /**
   * Optional pattern scope for this middleware. When provided, the middleware
   * applies only to events matching this pattern.
   */
  pattern?: string;

  /**
   * Segment separator used to interpret `pattern` (and matching), if provided.
   */
  separator?: string;
};
