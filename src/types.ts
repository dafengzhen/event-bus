/**
 * Compiled pattern listener stored internally in the trie structure.
 * Contains the fully processed pattern, match function, and listener metadata.
 *
 * @template E - Event map type defining event names and their payloads
 */
export type CompiledPatternListener<E extends EventMap> = {
  /**
   * The original handler associated with the pattern listener.
   * This handler will be invoked when the pattern matches an emitted event.
   */
  handler: PatternHandler<E>;

  /**
   * Function used to test an event name against the compiled pattern.
   * Pre-computed at registration time for optimal matching performance.
   *
   * @param event - The event name to match (raw string form, not segmented).
   * @returns A params object when matched, otherwise `null`.
   */
  match: (event: string) => null | Record<string, string>;

  /**
   * Whether this listener should be removed after the first successful match.
   * Similar to EventEmitter's `once` but works with pattern-based matching.
   */
  once: boolean;

  /**
   * The original pattern string provided by the user.
   * Preserved for introspection, debugging, and potential recompilation.
   */
  pattern: string;

  /**
   * Listener priority. Higher values run earlier (or are resolved first,
   * depending on the dispatcher implementation).
   *
   * @default 0
   */
  priority: number;

  /**
   * The segment separator used to parse the pattern and incoming event name.
   * For example: "." or "/".
   *
   * @default "."
   */
  separator: string;

  /**
   * A monotonically increasing sequence number used to ensure a stable
   * ordering when priorities are equal.
   *
   * @internal
   */
  seq: number;
};

/**
 * Compiled segment representation used during pattern parsing and matching.
 * Each segment of a pattern (split by separator) is compiled into one of these types
 * for efficient trie-based matching.
 */
export type CompiledSeg =
  /**
   * A named parameter segment such as `:id`.
   * Captures the segment value into the params object under the specified key.
   */
  | { key: string; type: 'param' }
  /**
   * A glob segment compiled into a regular expression, e.g. `*`/`{...}`-style
   * patterns depending on your DSL.
   *
   * The regex is used to match against the actual segment value.
   */
  | { re: RegExp; src: string; type: 'glob' }
  /**
   * A deep wildcard that can span multiple segments (e.g. `**`).
   * Matches zero or more segments greedily.
   */
  | { type: 'deepWildcard' }
  /**
   * An exact literal segment.
   * Matches only if the segment equals the specified value exactly.
   */
  | { type: 'exact'; value: string }
  /**
   * A single-segment wildcard (e.g. `*` matching exactly one segment).
   * Matches any single segment value.
   */
  | { type: 'segWildcard' };

/**
 * Per-emission context object passed to middleware and available in handlers.
 * Provides control over the emission flow and access to emission data.
 *
 * @template E - Event map type
 * @template K - Specific event key being emitted
 */
export type EmitContext<E extends EventMap, K extends keyof E> = {
  /**
   * Prevent further middleware/handlers from running for the current emission.
   * Typically used for short-circuiting or "stop propagation" semantics.
   * Once called, subsequent middleware and pattern listeners will not execute.
   */
  block(): void;

  /**
   * Indicates whether the current emission has been blocked.
   * Set to `true` after `block()` is called.
   */
  readonly blocked: boolean;

  /**
   * The emitted event key.
   * This is the raw event name string that was emitted.
   */
  event: K;

  /**
   * Pattern listeners that matched this emission, in the resolved order.
   * Order is determined by:
   * 1. Priority (higher first)
   * 2. Registration sequence (earlier first for equal priority)
   *
   * Read-only to prevent mutation during emission.
   */
  matched: ReadonlyArray<PatternListenerInfo<E>>;

  /**
   * Arbitrary metadata carried alongside the emission (e.g. tracing, source).
   * Can be extended via `metaPatch` in emit options.
   * Useful for cross-cutting concerns like logging, monitoring, or distributed tracing.
   */
  meta: Record<string, unknown>;

  /**
   * Parameters extracted from pattern matching for the current emission.
   * Only populated when the emission triggers pattern-based listeners.
   * Keys correspond to named parameters (`:param`) in matching patterns,
   * values are the matched segment strings.
   *
   * For exact (non-pattern) listeners, this will be an empty object.
   * For pattern listeners, this contains the merged parameters from all
   * matched patterns? Or does it represent the matching path that led here?
   *
   * @remarks
   * The exact semantics depend on implementation - whether this represents
   * the parameters from the most specific match, all matches, or the path
   * through the trie.
   */
  params: Record<string, string>;

  /**
   * The payload associated with the emitted event.
   * Type is inferred from the event map for the specific event key.
   */
  payload: E[K];
};

/**
 * Options for controlling event emission behavior.
 * Provides fine-grained control over metadata, stickiness, and replay behavior.
 */
export type EmitOptions = {
  /**
   * Shallow patch applied to `ctx.meta` for this emission.
   * Useful for injecting per-emit metadata without mutating global state.
   *
   * @example
   * ```ts
   * emitter.emit('user.created', user, {
   *   metaPatch: { requestId: 'abc-123', source: 'api' }
   * });
   * ```
   */
  metaPatch?: Record<string, unknown>;

  /**
   * Whether this emission should be "sticky" (implementation-defined),
   * e.g. cached for late subscribers or replayed to new listeners.
   *
   * Sticky events are stored and replayed to listeners registered after the emission.
   * Useful for initialization events or state synchronization.
   *
   * @default false
   */
  sticky?: boolean;

  /**
   * Maximum number of sticky entries to keep for this event pattern.
   * Only applicable when `sticky: true`.
   *
   * @default undefined (no limit)
   */
  stickyMax?: number;

  /**
   * Controls default replay behavior for this sticky entry.
   * - 'replay'  : replay to future listeners without consuming
   * - 'consume' : replay to a future listener once, then remove from sticky storage
   *
   * @default 'replay'
   */
  stickyMode?: StickyMode;
};

/**
 * Event-to-payload map type definition.
 * This is the core contract that defines all possible events and their associated payload types.
 *
 * Example:
 * ```ts
 * type Events = {
 *   'user.created': { id: string; name: string };
 *   'user.deleted': { id: string; reason?: string };
 *   'system:error': Error;
 * };
 * ```
 */
export type EventMap = Record<string, any>;

/**
 * Type-safe listener function for a specific payload type.
 *
 * @template P - The payload type this listener accepts
 *
 * @param payload - The event payload
 */
export type Listener<P> = (payload: P) => void;

/**
 * Middleware function for intercepting and processing events.
 * Middleware runs before pattern listeners and can modify the emission context,
 * block propagation, or perform side effects.
 *
 * @template E - Event map type
 *
 * @param ctx - Per-emission context object
 * @param next - Call to continue to the next middleware in the chain
 * @returns Promise if async, void if sync
 *
 * @example
 * ```ts
 * const logger: Middleware<Events> = async (ctx, next) => {
 *   console.log(`Emitting ${ctx.event}:`, ctx.payload);
 *   await next();
 *   console.log(`Completed ${ctx.event}`);
 * };
 * ```
 */
export type Middleware<E extends EventMap> = (
  ctx: EmitContext<E, keyof E>,
  next: () => Promise<void>,
) => Promise<void> | void;

/**
 * Middleware entry with optional conditional execution.
 * Allows middleware to be registered with predicates that determine
 * whether it should run for a given emission.
 *
 * @template E - Event map type
 */
export type MiddlewareEntry<E extends EventMap> = {
  /**
   * Middleware function.
   */
  fn: Middleware<E>;

  /**
   * Optional predicate to decide whether to run this middleware for the
   * current emission.
   *
   * @param ctx - The emission context
   * @returns `true` if middleware should run
   */
  match?: (ctx: EmitContext<E, keyof E>) => boolean;
};

/**
 * Options for registering event listeners.
 * Controls listener behavior such as sticky consumption, priority, and separator override.
 */
export type OnOptions = {
  /**
   * If true, replayed sticky events are consumed (removed) from sticky storage.
   * This ensures the listener receives the sticky event only once.
   *
   * @default false
   */
  consumeSticky?: boolean;

  /**
   * Listener priority. Higher values run earlier (or are resolved first,
   * depending on the dispatcher implementation).
   *
   * @default 0
   */
  priority?: number;

  /**
   * Segment separator used to interpret pattern/event names for this listener.
   * Overrides the default separator if provided.
   *
   * @example "." for dotted notation, "/" for path-style
   * @default "."
   */
  separator?: string;
};

/**
 * Pattern-based event handler function.
 * Unlike exact listeners, pattern handlers receive the matched event name
 * and extracted parameters in addition to the payload.
 *
 * The handler can be defined with either 3 parameters (event, payload, params)
 * or 4 parameters (event, payload, params, ctx) for access to the emission context.
 *
 * @template E - Event map type
 *
 * @param event - The full event name that triggered the pattern match
 *                (may be a known key of `E` or an arbitrary string not in the map)
 * @param payload - The payload associated with the emitted event
 * @param params - Parameters extracted from the pattern match.
 *                 Keys correspond to named parameters (`:param`) in the pattern,
 *                 values are the matched segment strings.
 * @param ctx - Optional emission context for advanced control.
 *              Provides access to meta, blocking capability, and other matched listeners.
 *              Only available when handler defines 4 parameters.
 *
 * @example
 * ```ts
 * // Basic pattern handler with 3 parameters
 * const handler: PatternHandler<Events> = (event, payload, params) => {
 *   console.log(`User ${params.id} was ${event.split('.').pop()}`);
 * };
 * emitter.on('user.:id.*', handler);
 *
 * // Pattern handler with context access
 * const handlerWithCtx: PatternHandler<Events> = (event, payload, params, ctx) => {
 *   console.log(`Processing ${event} with trace ID: ${ctx.meta.traceId}`);
 *   if (!isAuthorized(ctx)) ctx.block();
 * };
 * emitter.on('admin.:action', handlerWithCtx);
 * ```
 */
export type PatternHandler<E extends EventMap> =
  | ((event: string, payload: any, params: Record<string, string>) => void)
  | ((
      event: string,
      payload: any,
      params: Record<string, string>,
      ctx: EmitContext<E, keyof E>,
    ) => void);

/**
 * Information about a matched pattern listener during emission.
 * Provides read-only access to the matched listener and its extracted parameters.
 *
 * @template E - Event map type
 */
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
   * Keys correspond to named parameters (`:param`) in the pattern.
   * Values are the matched segment strings.
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

/**
 * Sticky event replay behavior modes.
 * Determines how sticky events are handled when replayed to late subscribers.
 */
export type StickyMode =
  /**
   * Replay to future listeners and keep in sticky storage.
   * Each new matching listener will receive the event.
   */
  | 'consume'
  /**
   * Replay to a future listener once, then remove from sticky storage.
   * Useful for one-time initialization events.
   */
  | 'replay';

/**
 * Node in the pattern matching trie.
 * The trie is built from compiled pattern segments and enables
 * O(n) matching where n is the number of segments in the event name.
 *
 * @template E - Event map type
 */
export type TrieNode<E extends EventMap> = {
  /**
   * Child node representing a deep wildcard path (e.g. `**`).
   * Deep wildcards match zero or more segments and are evaluated last
   * due to their greedy nature.
   */
  deep?: TrieNode<E>;

  /**
   * Listeners that end exactly at this node (i.e. full pattern match).
   * Stored in priority-sorted order.
   */
  end: CompiledPatternListener<E>[];

  /**
   * Exact literal children keyed by segment value.
   * Fast O(1) lookup for exact segment matches.
   */
  exact: Map<string, TrieNode<E>>;

  /**
   * Glob children compiled as regexes. Each entry pairs the child node with
   * its compiled matcher and original source.
   *
   * Evaluated in registration order.
   */
  globs?: Array<{ node: TrieNode<E>; re: RegExp; src: string }>;

  /**
   * Unique node id, useful for debugging or stable traversal ordering.
   *
   * @internal
   */
  id: number;

  /**
   * Parameter children (named segments). Each entry stores the param key and
   * the corresponding child node.
   *
   * Evaluated after exact matches, before globs and wildcards.
   */
  params?: Array<{ key: string; node: TrieNode<E> }>;

  /**
   * Child node representing a single-segment wildcard (e.g. `*`).
   * Matches any single segment value.
   */
  star?: TrieNode<E>;
};

/**
 * Options for registering middleware.
 * Provides fine-grained control over when and how middleware executes.
 *
 * @template E - Event map type
 */
export type UseOptions<E extends EventMap> = {
  /**
   * Optional predicate that decides whether the middleware should run for the
   * current emission.
   *
   * @param ctx - The emission context
   * @returns `true` if middleware should run
   */
  match?: (ctx: EmitContext<E, keyof E>) => boolean;

  /**
   * If true, only run the middleware when at least one pattern listener
   * matched the current event.
   * Useful for middleware that processes matched events, like response handlers.
   *
   * @default false
   */
  onlyWhenPatternListenerMatched?: boolean;

  /**
   * Optional pattern scope for this middleware. When provided, the middleware
   * applies only to events matching this pattern.
   *
   * @example
   * ```ts
   * // Only runs for user-related events
   * emitter.use(authMiddleware, { pattern: 'user.*' });
   * ```
   */
  pattern?: string;

  /**
   * Segment separator used to interpret `pattern` (and matching), if provided.
   *
   * @default "."
   */
  separator?: string;
};
