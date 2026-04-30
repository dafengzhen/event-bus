import type { DispatcherRuntime } from './dispatcher-runtime.ts';

/**
 * A pre-compiled pattern listener entry used internally by the event bus.
 * Stores the handler, match function, and metadata needed for dispatch and cleanup.
 *
 * @typeParam E - The event map type.
 */
export interface CompiledPatternListenerEntry<E extends EventMap> {
  /** The callback to invoke when a pattern matches. */
  handler: PatternHandler<E>;
  /** Whether the underlying pattern is a native RegExp (vs. a DFA-compiled string pattern). */
  isNativeRegExp: boolean;
  /** Function that tests an event string and returns extracted params, or `null` if no match. */
  match: (event: string) => null | PatternParams;
  /** Whether this listener is automatically removed after its first invocation. */
  once: boolean;
  /** The original pattern string (or RegExp `toString()` representation). */
  pattern: string;
  /** Priority of this listener. Higher values execute first. */
  priority: number;
  /** Monotonically increasing sequence number to preserve registration order among equal priorities. */
  seq: number;
}

/**
 * Context object provided to listeners and middleware during event dispatch.
 * Allows inspection of the event, payload, matched patterns, and provides mechanisms
 * to block further propagation or attach metadata.
 *
 * @typeParam E - The event map type.
 * @typeParam K - The specific event key being dispatched.
 */
export interface EmitContext<E extends EventMap, K extends keyof E = keyof E> {
  /** Prevents execution of remaining listeners and middleware for this event. */
  block(): void;
  /** Whether `block()` has been called on this context. */
  readonly blocked: boolean;
  /** The event key being dispatched. */
  readonly event: K;
  /** Read-only snapshot of pattern listeners that matched this event (only for string events). */
  readonly matched: ReadonlyArray<PatternListenerInfo<E>>;
  /** Mutable metadata bag. Can be modified by middleware to communicate downstream. */
  meta: Record<string, unknown>;
  /** Parameters extracted from the current pattern match (populated per-listener during dispatch). */
  params: PatternParams;
  /** The event payload. */
  readonly payload: E[K];
}

/**
 * Options for emitting an event.
 */
export interface EmitOptions {
  /** Properties to shallow-merge into `ctx.meta` for this emission. */
  metaPatch?: Record<string, unknown>;
  /** Whether this event should be stored as a sticky event for replay by future listeners. */
  sticky?: boolean;
  /**
   * Sticky mode for this event.
   * - `'replay'`: The event persists and is replayed to each new listener.
   * - `'consume'`: The event is consumed after being replayed to the first matching listener.
   */
  stickyMode?: StickyMode;
}

/**
 * Configuration options for constructing an {@link EventBus}.
 */
export interface EventBusOptions {
  /** Whether to clear the shared global DFA compile cache when this instance is destroyed. */
  clearGlobalCacheOnDestroy?: boolean;
  /** Whether to log listener/middleware errors to `console.error`. Defaults to `true`. */
  logErrors?: boolean;
  /** Optional global error handler for listener/middleware errors. */
  onError?: (e: unknown) => void;
  /** Custom dispatcher runtime. If not provided, a new one is created. */
  runtime?: DispatcherRuntime<any>;
  /** Maximum number of sticky events to retain per exact event key. Defaults to `1`. */
  stickyExactMax?: number;
  /** Maximum number of distinct sticky event keys to retain for pattern replay. Defaults to `200`. */
  stickyMax?: number;
}

/**
 * Valid event key types. Events can be keyed by number, string, or symbol.
 */
export type EventKey = number | string | symbol;

/**
 * The event map type: a record mapping event keys to their payload types.
 *
 * @example
 * ```ts
 * type MyEvents = {
 *   'user:login': { userId: string };
 *   'data:sync': { timestamp: number };
 * };
 * ```
 */
export type EventMap = Record<EventKey, any>;

/**
 * Internal entry stored in the exact listeners map.
 *
 * @typeParam E - The event map type.
 * @typeParam K - The event key.
 */
export type ExactListenerEntry<E extends EventMap, K extends keyof E> = {
  /** The listener function. */
  listener: Listener<E[K], E, K>;
  /** Priority of this listener. */
  priority: number;
  /** Sequence number for deterministic ordering. */
  seq: number;
};

/**
 * A listener function registered for a specific event key.
 *
 * @typeParam P - The payload type.
 * @typeParam E - The event map type.
 * @typeParam K - The event key.
 * @param payload - The event payload.
 * @param ctx - The emit context (optional).
 */
export type Listener<P, E extends EventMap = EventMap, K extends keyof E = keyof E> = (
  payload: P,
  ctx?: EmitContext<E, K>,
) => void;

/**
 * A matched pattern entry paired with its extracted params, produced during dispatch
 * of string events.
 *
 * @typeParam E - The event map type.
 */
export type MatchedPattern<E extends EventMap> = {
  /** The compiled pattern listener entry that matched. */
  entry: CompiledPatternListenerEntry<E>;
  /** Named capture groups or an empty record extracted from the event string. */
  params: Record<string, string>;
};

/**
 * Union type for any middleware function (sync or async).
 *
 * @typeParam E - The event map type.
 */
export type Middleware<E extends EventMap> = MiddlewareAsync<E> | MiddlewareSync<E>;

/**
 * An async middleware function.
 * Must await `next()` to continue the chain.
 *
 * @typeParam E - The event map type.
 * @param ctx - The emit context.
 * @param next - Callback to invoke the next middleware/listeners. Returns a Promise.
 */
export type MiddlewareAsync<E extends EventMap> = (
  ctx: EmitContext<E>,
  next: () => Promise<void>,
) => Promise<void>;

/**
 * Internal entry for a registered middleware, including its match filter and async flag.
 *
 * @typeParam E - The event map type.
 */
export interface MiddlewareEntry<E extends EventMap> {
  /** The middleware function. */
  fn: Middleware<E>;
  /** Whether the middleware is async. */
  isAsync: boolean;
  /** Optional filter function to determine whether the middleware applies to a given event. */
  match?: (ctx: EmitContext<E>) => boolean;
}

/**
 * A synchronous middleware function.
 * Must call `next()` exactly once, or call `ctx.block()` to halt dispatch.
 *
 * @typeParam E - The event map type.
 * @param ctx - The emit context.
 * @param next - Callback to invoke the next middleware/listeners.
 */
export type MiddlewareSync<E extends EventMap> = (ctx: EmitContext<E>, next: () => void) => void;

/**
 * Options for registering a listener via `on`, `once`, `onMatch`, or `onceMatch`.
 */
export interface OnOptions {
  /** Override sticky consumption behaviour for this listener. */
  consumeSticky?: boolean;
  /** Priority of this listener. Higher values execute first. Defaults to `0` for exact listeners, `80` for pattern listeners. */
  priority?: number;
}

/**
 * A handler function for pattern-based event matching.
 *
 * @typeParam E - The event map type.
 * @param event - The matched event string.
 * @param payload - The event payload.
 * @param params - Parameters extracted from the pattern match (named capture groups or empty).
 * @param ctx - The emit context (optional).
 */
export type PatternHandler<E extends EventMap> = (
  event: string,
  payload: E[keyof E],
  params: PatternParams,
  ctx?: EmitContext<E>,
) => void;

/**
 * Read-only info about a matched pattern listener, exposed via `ctx.matched`.
 *
 * @typeParam E - The event map type.
 */
export interface PatternListenerInfo<E extends EventMap> {
  /** The pattern handler function. */
  handler: PatternHandler<E>;
  /** Whether the handler is one-shot. */
  once: boolean;
  /** Frozen parameters extracted from the match. */
  params: Readonly<PatternParams>;
  /** The pattern string. */
  pattern: string;
  /** Priority of the pattern listener. */
  priority: number;
}

/**
 * A record of named parameters extracted from a pattern match. Empty for DFA-based matches
 * (which do not support capture groups).
 */
export type PatternParams = Record<string, string>;

/**
 * Result of replaying a single sticky exact event.
 */
export type ReplayOneResult = { found: false } | { found: true; payload: unknown };

/**
 * A stored sticky event, including its mode and payload.
 */
export type StickyEvent = {
  /** The sticky mode (`replay` or `consume`). */
  mode: StickyMode;
  /** The event payload. */
  payload: unknown;
};

/**
 * Sticky mode for stored events.
 * - `'replay'`: Replayed to every new matching listener.
 * - `'consume'`: Consumed after being replayed to the first matching listener.
 */
export type StickyMode = 'consume' | 'replay';

/**
 * Options for registering middleware via `use` or `useAsync`.
 *
 * @typeParam E - The event map type.
 */
export interface UseOptions<E extends EventMap> {
  /** Custom match function to filter which events the middleware applies to. */
  match?: (ctx: EmitContext<E>) => boolean;
  /** String pattern (compiled via DFA) to filter events. The middleware only runs on matching string events. */
  pattern?: string;
}
