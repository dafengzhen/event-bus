import type { DispatcherRuntime } from './dispatcher-runtime.ts';
import type { EventScope } from './event-scope.ts';

/**
 * Base context interface shared across all dispatch phases.
 *
 * @typeParam E - The event map type.
 * @typeParam K - The specific event key type.
 */
export interface BaseContext<E extends EventMap, K extends keyof E = keyof E> {
  /**
   * The event key being dispatched.
   */
  readonly event: K;

  /**
   * Monotonically increasing event ID unique per emit operation.
   */
  readonly id: number;

  /**
   * Optional origin identifier for the event (e.g., source module name).
   */
  readonly origin?: string | undefined;

  /**
   * The payload associated with this event.
   */
  readonly payload: E[K];

  /**
   * Unix timestamp (in milliseconds) when the event was emitted.
   */
  readonly timestamp: number;
}

/**
 * Internal storage for a compiled pattern listener.
 *
 * @typeParam E - The event map type.
 */
export interface CompiledPatternListenerEntry<E extends EventMap> {
  /**
   * The handler function to invoke when a pattern matches.
   */
  handler: PatternHandler<E>;

  /**
   * Whether the pattern is a native RegExp (`true`) or a DFA-compiled string (`false`).
   */
  isNativeRegExp: boolean;

  /**
   * Function that attempts to match an event string against the pattern.
   * Returns `null` if no match, or a `PatternParams` object with captured groups.
   */
  match: (event: string) => null | PatternParams;

  /**
   * Whether this listener should be removed after its first match.
   */
  once: boolean;

  /**
   * The original pattern string (for debugging and deduplication).
   */
  pattern: string;

  /**
   * Priority value. Higher values execute before lower values.
   */
  priority: number;

  /**
   * Sequence number for stable ordering of listeners with equal priority.
   */
  seq: number;
}

/**
 * The phase of event dispatch.
 *
 * - `'exact'`: Dispatch to exact key listeners.
 * - `'middleware'`: Execution of the middleware pipeline.
 * - `'pattern'`: Dispatch to pattern-matching listeners.
 */
export type DispatchPhase = 'exact' | 'middleware' | 'pattern';

/**
 * Options for emitting an event.
 */
export interface EmitOptions {
  /**
   * Additional properties merged into the context's `meta` object.
   * The scope automatically injects `{ scope: EventScope }` when using `EventScope.emit()`.
   */
  metaPatch?: Record<string, unknown> | undefined;

  /**
   * Optional origin identifier (e.g., the module or system that produced the event).
   */
  origin?: string | undefined;

  /**
   * If `true`, the event is stored as sticky and replayed to future listeners
   * that register after this emission.
   */
  sticky?: boolean | undefined;

  /**
   * Controls the lifecycle of a sticky event:
   * - `'consume'`: The event is removed after being replayed once.
   * - `'replay'`: The event persists for multiple replays (default).
   */
  stickyMode?: StickyMode | undefined;
}

/**
 * Configuration options for creating an EventBus.
 *
 * @typeParam E - The event map type.
 */
export interface EventBusOptions<E extends EventMap = EventMap> {
  /**
   * If `true`, clears the global regex-derivative compilation cache when the bus is disposed.
   * Useful in long-running applications where pattern memory should be reclaimed.
   */
  clearGlobalCacheOnDispose?: boolean | undefined;

  /**
   * If `true` (default), errors thrown by listeners/middleware are logged via `console.error`.
   */
  logErrors?: boolean | undefined;

  /**
   * Optional global error handler for listener and middleware errors.
   * If not provided, errors are re-thrown asynchronously.
   */
  onError?: ((e: unknown) => void) | undefined;

  /**
   * Custom dispatcher runtime for scope management.
   * Defaults to a stack-based `DispatcherRuntime`.
   */
  runtime?: DispatcherRuntime<E> | undefined;

  /**
   * Maximum number of sticky events retained per exact event key.
   * Defaults to `1`.
   */
  stickyExactMax?: number | undefined;

  /**
   * Maximum number of distinct sticky event keys retained globally.
   * Defaults to `200`.
   */
  stickyMax?: number | undefined;

  /**
   * Maximum number of sticky events retained per pattern key.
   * Defaults to `stickyMax`.
   */
  stickyPatternMaxPerKey?: number | undefined;
}

/**
 * Valid event key types. Events can be keyed by number, string, or symbol.
 */
export type EventKey = number | string | symbol;

/**
 * Mapping from event keys to their payload types.
 *
 * @example
 * ```typescript
 * interface MyEvents {
 *   'user:login': { userId: string; timestamp: number };
 *   'user:logout': { userId: string };
 *   'error': Error;
 * }
 * ```
 */
export type EventMap = Record<EventKey, any>;

/**
 * Internal storage for an exact (non-pattern) listener entry.
 *
 * @typeParam E - The event map type.
 * @typeParam K - The event key type.
 */
export type ExactListenerEntry<E extends EventMap, K extends keyof E> = {
  /**
   * The listener function.
   */
  listener: Listener<E[K], E, K>;

  /**
   * Priority value. Higher values execute first.
   */
  priority: number;

  /**
   * Registration sequence number.
   */
  seq: number;
};

/**
 * A listener function invoked when a matching event is emitted.
 *
 * @typeParam P - The payload type for this event.
 * @typeParam E - The event map type.
 * @typeParam K - The event key type.
 * @param payload - The event payload.
 * @param ctx - The listener context providing event metadata and lifecycle state.
 * @returns Optionally a Promise if this is an async listener.
 */
export type Listener<P, E extends EventMap = EventMap, K extends keyof E = keyof E> = (
  payload: P,
  ctx: ListenerContext<E, K>,
) => MaybePromise<void>;

/**
 * Context provided to event listeners during dispatch.
 * This context is frozen (immutable) and uses reactive getters for lifecycle state.
 *
 * @typeParam E - The event map type.
 * @typeParam K - The event key type.
 */
export interface ListenerContext<
  E extends EventMap,
  K extends keyof E = keyof E,
> extends BaseContext<E, K> {
  /**
   * Whether the event has been canceled by `ctx.cancel()`.
   */
  readonly isCanceled: boolean;

  /**
   * Whether immediate propagation has been stopped by `ctx.stopImmediate()`.
   */
  readonly isImmediateStopped: boolean;

  /**
   * Whether propagation has been stopped by `ctx.stop()` or `ctx.stopImmediate()`.
   */
  readonly isStopped: boolean;

  /**
   * Shared metadata object for cross-listener communication.
   * Initially empty; populated lazily via `metaPatch` on emit.
   */
  readonly meta: Readonly<Record<string, unknown>>;

  /**
   * The current dispatch phase. Listeners receive either `'exact'` or `'pattern'`.
   */
  readonly phase: 'exact' | 'pattern';
}

/**
 * A matched pattern entry with its extracted match parameters.
 *
 * @typeParam E - The event map type.
 */
export type MatchedPattern<E extends EventMap> = {
  /**
   * The compiled pattern listener entry that matched.
   */
  entry: CompiledPatternListenerEntry<E>;

  /**
   * The extracted match parameters (named groups from RegExp or empty object from DFA).
   */
  match: PatternParams;
};

/**
 * A value that may be a direct value or a Promise wrapping that value.
 *
 * @typeParam T - The value type.
 */
export type MaybePromise<T> = PromiseLike<T> | T;

/**
 * Union type representing any middleware (sync or async).
 *
 * @typeParam E - The event map type.
 */
export type Middleware<E extends EventMap> = MiddlewareAsync<E> | MiddlewareSync<E>;

/**
 * An asynchronous middleware function.
 *
 * @typeParam E - The event map type.
 * @param ctx - The middleware context.
 * @param next - Callback to invoke the next middleware in the chain. Must be awaited.
 * @returns A Promise that resolves when the middleware's work is complete.
 */
export type MiddlewareAsync<E extends EventMap> = (
  ctx: MiddlewareContext<E>,
  next: () => Promise<void>,
) => Promise<void>;

/**
 * Context provided to middleware during execution.
 *
 * @typeParam E - The event map type.
 * @typeParam K - The event key type.
 */
export interface MiddlewareContext<
  E extends EventMap,
  K extends keyof E = keyof E,
> extends BaseContext<E, K> {
  /**
   * Cancels the event dispatch. No further middleware or listeners will be invoked.
   */
  cancel(): void;

  /**
   * Whether `cancel()` has been called.
   */
  readonly isCanceled: boolean;

  /**
   * Whether `stopImmediate()` has been called.
   */
  readonly isImmediateStopped: boolean;

  /**
   * Whether `stop()` or `stopImmediate()` has been called.
   */
  readonly isStopped: boolean;

  /**
   * Mutable shared metadata object for cross-middleware and cross-listener communication.
   */
  readonly meta: Record<string, unknown>;

  /**
   * The current dispatch phase. Always `'middleware'` for middleware contexts.
   */
  readonly phase: 'middleware';

  /**
   * Stops event propagation to subsequent middleware and listeners.
   * The current middleware may still call `next()` to continue.
   */
  stop(): void;

  /**
   * Stops event propagation immediately, preventing further middleware
   * and listeners from being invoked, including the rest of the current middleware chain.
   */
  stopImmediate(): void;
}

/**
 * Internal storage for a registered middleware entry.
 *
 * @typeParam E - The event map type.
 */
export interface MiddlewareEntry<E extends EventMap> {
  /**
   * The middleware function (sync or async).
   */
  fn: Middleware<E>;

  /**
   * Whether this middleware is asynchronous.
   */
  isAsync: boolean;

  /**
   * Optional filter function to conditionally apply this middleware.
   * Returns `true` if the middleware should be invoked for the given context.
   */
  match?: ((ctx: MiddlewareContext<E>) => boolean) | undefined;
}

/**
 * A synchronous middleware function.
 *
 * @typeParam E - The event map type.
 * @param ctx - The middleware context.
 * @param next - Callback to invoke the next middleware in the chain. Must be called to continue.
 */
export type MiddlewareSync<E extends EventMap> = (
  ctx: MiddlewareContext<E>,
  next: () => void,
) => void;

/**
 * A function that removes a previously registered listener, middleware, or pattern handler.
 * Safe to call multiple times; subsequent calls are no-ops.
 */
export type Off = () => void;

/**
 * Options for registering event listeners.
 */
export interface OnOptions {
  /**
   * Override for sticky event consumption behavior:
   * - `true`: Always consume sticky events on replay.
   * - `false`: Never consume sticky events.
   * - `undefined`: Use the event's `stickyMode` (default).
   */
  consumeSticky?: boolean | undefined;

  /**
   * Listener priority. Higher values execute before lower values.
   * Default is `0` for exact listeners, `80` for pattern listeners.
   */
  priority?: number | undefined;
}

/**
 * A pattern-matching handler function.
 *
 * @typeParam E - The event map type.
 * @param event - The matched event string.
 * @param payload - The event payload.
 * @param match - The extracted match parameters (named capture groups).
 * @param ctx - The listener context.
 * @returns Optionally a Promise if this is an async handler.
 */
export type PatternHandler<E extends EventMap> = (
  event: string,
  payload: E[keyof E],
  match: PatternParams,
  ctx: ListenerContext<E>,
) => MaybePromise<void>;

/**
 * Parameters extracted from a pattern match.
 * Keys are named capture group names, values are the matched strings or `undefined`.
 * DFA-compiled string patterns yield an empty object.
 */
export type PatternParams = Record<string, string | undefined>;

/**
 * Result of attempting to replay a single sticky event.
 *
 * - `{ found: false }`: No sticky event was found.
 * - `{ found: true; payload: unknown }`: A sticky event was found.
 */
export type ReplayOneResult =
  | {
      /**
       * Indicates a sticky event was found.
       */
      found: true;
      /**
       * The stored event payload.
       */
      payload: unknown;
    }
  | { found: false };

/**
 * A frame in the scope stack, associating a scope with its execution context.
 *
 * @typeParam E - The event map type.
 */
export type ScopeFrame<E extends EventMap> = {
  /**
   * The event scope active in this frame.
   */
  readonly scope: EventScope<E>;
};

/**
 * Interface for external scope storage (e.g., AsyncLocalStorage in Node.js).
 *
 * @typeParam E - The event map type.
 */
export interface ScopeStorage<E extends EventMap> {
  /**
   * Returns the currently stored scope frame, or `undefined` if none is active.
   */
  getStore(): ScopeFrame<E> | undefined;

  /**
   * Executes a callback with the given scope frame as the current context.
   *
   * @typeParam T - The return type.
   * @param store - The scope frame to make active.
   * @param callback - The function to execute within the scope.
   * @returns The callback's return value.
   */
  run<T>(store: ScopeFrame<E>, callback: () => T): T;
}

/**
 * A stored sticky event.
 */
export type StickyEvent = {
  /**
   * The lifecycle mode of this sticky event.
   */
  mode: StickyMode;

  /**
   * The stored event payload.
   */
  payload: unknown;
};

/**
 * Controls the lifecycle behavior of a sticky event.
 *
 * - `'consume'`: The event is removed after being replayed to one listener.
 * - `'replay'`: The event persists and is replayed to all future matching listeners.
 */
export type StickyMode = 'consume' | 'replay';

/**
 * Extended exact listener entry that tracks the original listener function
 * for proper removal when the listener has been wrapped (e.g., for `once` semantics).
 *
 * @typeParam E - The event map type.
 */
export type StoredExactListenerEntry<E extends EventMap> = ExactListenerEntry<E, any> & {
  /**
   * The original (unwrapped) listener function used for identity checks during removal.
   */
  readonly originalListener: Listener<any, E, any>;
};

/**
 * Options for registering middleware.
 *
 * @typeParam E - The event map type.
 */
export interface UseOptions<E extends EventMap> {
  /**
   * Optional custom match function to conditionally apply this middleware.
   * Receives the middleware context; should return `true` to invoke the middleware.
   */
  match?: ((ctx: MiddlewareContext<E>) => boolean) | undefined;

  /**
   * Optional string pattern to conditionally apply this middleware.
   * The middleware will only be invoked for events whose key matches this pattern.
   */
  pattern?: string | undefined;
}
