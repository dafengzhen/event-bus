/**
 * Event map definition.
 *
 * A mapping from **event name** to **payload type**.
 *
 * - **Key**: event name (string literal recommended)
 * - **Value**: payload type for that event
 *
 * Notes:
 * - Event names are typically namespaced strings, e.g. `user:login`, `order:created`.
 * - Payload can be `void` for events without payload.
 *
 * @example
 * ```ts
 * type MyEvents = {
 *   'user:login': { id: string };
 *   'user:logout': void;
 * };
 * ```
 */
export type EventMap = Record<string, unknown>;

/**
 * Listener type for a specific payload.
 *
 * If the payload is `void`, the listener receives **no arguments**.
 * Otherwise, it receives **exactly one argument**: the payload.
 *
 * This enables strict typing for both forms:
 *
 * @example
 * ```ts
 * on('ready', () => {});
 * on('data', (payload) => {
 *   console.log(payload);
 * });
 * ```
 */
export type Listener<Payload> = Payload extends void ? () => void : (payload: Payload) => void;

/**
 * Listener that receives **all emitted events**.
 *
 * It receives:
 * - `event`: the event name
 * - `payload`: the payload associated with that event
 *
 * Typical use cases:
 * - logging / debugging
 * - analytics / tracing
 * - building devtools
 *
 * @example
 * ```ts
 * const off = bus.onAny((event, payload) => {
 *   console.log('event=', event, 'payload=', payload);
 * });
 * off();
 * ```
 */
export type AnyListener<E extends EventMap> = <K extends keyof E>(event: K, payload: E[K]) => void;

/**
 * Pattern matching strategy used by a pattern listener.
 *
 * - `exact`    → exact segment match (no wildcards / params)
 * - `wildcard` → contains wildcards (e.g. `*`, `**`, `?`)
 * - `param`    → contains named params (e.g. `{id}`)
 *
 * Notes:
 * - These are informational categories; the actual matching is implemented by the EventBus.
 */
export type PatternKind = 'exact' | 'wildcard' | 'param';

/**
 * Context object passed through the emit lifecycle.
 *
 * This context is shared across:
 * - global middleware
 * - pattern middleware
 * - dispatch logic
 *
 * It allows middleware to:
 * - observe the event and payload
 * - attach metadata (`meta`)
 * - stop propagation (`block()`)
 */
export type EmitContext<E extends EventMap, K extends keyof E> = {
  /**
   * Whether propagation has been blocked.
   *
   * Once `true`, dispatch stops and no further middleware/listeners should run.
   */
  readonly blocked: boolean;

  /**
   * Current emitted event name.
   */
  readonly event: K;

  /**
   * Payload associated with the event.
   *
   * If the event's payload type is `void`, this value is typically `undefined`
   * (depending on the emitter implementation).
   */
  readonly payload: E[K];

  /**
   * Information about matched pattern listeners for this emit cycle.
   *
   * - Ordered by priority (highest first).
   * - Empty if no pattern listeners matched (or pattern matching not applicable).
   *
   * Useful for middleware that wants to inspect or audit matches.
   */
  readonly matched: readonly PatternListenerInfo<E>[];

  /**
   * Free-form metadata container shared across middleware.
   *
   * Use this to pass state between middleware, e.g.:
   * - correlation IDs
   * - timing info
   * - auth decisions
   *
   * Note: Consumers should avoid putting large objects here in hot paths.
   */
  readonly meta: Record<string, unknown>;

  /**
   * Stop further propagation of this event.
   *
   * Once called:
   * - remaining middleware is skipped
   * - remaining listeners are not executed
   */
  block(): void;
};

/**
 * Middleware executed for **every emit call**.
 *
 * Middleware can:
 * - inspect event/payload/matches
 * - read/write `ctx.meta`
 * - block propagation via `ctx.block()`
 * - perform async work
 *
 * Chain semantics:
 * - Middlewares are executed sequentially in registration order.
 * - `next()` must be called exactly once to continue the chain (depending on implementation).
 */
export type Middleware<E extends EventMap> = <K extends keyof E>(
  ctx: EmitContext<E, K>,
  next: () => Promise<void>,
) => Promise<void> | void;

/**
 * Middleware executed **only when pattern listeners are involved**.
 *
 * Typical uses:
 * - logging pattern matches & params
 * - permission checks / gating
 * - validating extracted params
 *
 * Chain semantics depend on the EventBus implementation.
 * In your EventBus, pattern middleware acts as a guard:
 * - if it does not call `next()`, dispatch stops
 * - `next()` must not be called more than once
 */
export type PatternMiddleware<E extends EventMap> = (
  ctx: EmitContext<E, keyof E>,
  next: () => Promise<void>,
) => Promise<void> | void;

/**
 * Public information of a matched pattern listener.
 *
 * Exposed to middleware and user-land code through `ctx.matched`.
 */
export interface PatternListenerInfo<E extends EventMap> {
  /**
   * Original pattern string.
   *
   * @example
   * ```ts
   * 'user:{id}'
   * 'order:*'
   * '**'
   * ```
   */
  readonly pattern: string;

  /**
   * Matching strategy classification.
   */
  readonly kind: PatternKind;

  /**
   * Listener priority. Higher values run earlier.
   */
  readonly priority: number;

  /**
   * Whether this listener was registered with `once`.
   */
  readonly once?: boolean;

  /**
   * Extracted params from the event name.
   *
   * Example (separator `:`):
   * - pattern: `user:{id}`
   * - event:   `user:42`
   * - params:  `{ id: '42' }`
   */
  readonly params: Readonly<Record<string, string>>;

  /**
   * The actual handler function.
   *
   * Note:
   * - Kept here mainly for introspection/debugging.
   * - Middleware should generally not call handlers directly.
   */
  readonly handler: (event: keyof E, payload: E[keyof E], params?: Record<string, string>) => void;
}

/**
 * Internal compiled representation of a pattern listener.
 *
 * Not exposed publicly.
 * Contains precompiled matching logic for performance.
 */
export interface CompiledPatternListener<E extends EventMap> {
  /**
   * Original pattern string (uncompiled).
   */
  pattern: string;

  /**
   * Matching strategy classification.
   */
  kind: PatternKind;

  /**
   * Listener priority. Higher values run earlier.
   */
  priority: number;

  /**
   * Whether the listener should auto-remove after first run.
   */
  once?: boolean;

  /**
   * Match function produced by the compiler.
   *
   * @param event - Emitted event name
   * @returns
   * - `null` if no match
   * - params record if matched (possibly empty)
   */
  match(event: string): null | Record<string, string>;

  /**
   * Listener handler.
   *
   * @param event  - Emitted event name
   * @param payload - Payload associated with event
   * @param params - Extracted params from pattern (if any)
   */
  handler(event: keyof E, payload: E[keyof E], params?: Record<string, string>): void;
}

/**
 * Options for registering a pattern listener.
 */
export type PatternOptions = {
  /**
   * Invoke the listener only once.
   *
   * After the first successful match, it is automatically removed.
   */
  once?: boolean;

  /**
   * Listener priority.
   *
   * Higher values run earlier.
   * If omitted, EventBus will derive a priority from pattern specificity.
   */
  priority?: number;

  /**
   * Segment separator used when splitting patterns and events.
   *
   * IMPORTANT:
   * - This default should match your EventBus implementation.
   * - In the current EventBus code you posted, the default is `':'`.
   */
  separator?: string;
};

/**
 * Internal compiled segment representation produced by the pattern compiler.
 *
 * - `exact`        → exact segment match
 * - `wildcard`     → `*` matches exactly one segment
 * - `deepWildcard` → `**` matches 0..n segments (backtracking)
 * - `param`        → `{key}` captures a segment into params
 * - `regex`        → per-segment regex (used for `?` patterns)
 */
export type CompiledSeg =
  | { type: 'exact'; value: string }
  | { type: 'wildcard' }
  | { type: 'deepWildcard' }
  | { type: 'param'; key: string }
  | { type: 'regex'; re: RegExp };
