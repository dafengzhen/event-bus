export type AsyncMiddleware<E extends object> = (
  event: EventContext<E>,
  next: () => Promise<void>,
) => Promise<void>;
export interface DispatchFailure<E extends object> {
  readonly error: unknown;
  readonly event: EventContext<E>;
  readonly phase: 'listener' | 'middleware' | 'replay';
}
export interface DispatchResult<E extends object> {
  /** Number of listener invocations, including failures. */
  readonly delivered: number;
  readonly errors: readonly DispatchFailure<E>[];
  readonly event: EventContext<E>;
}

/** Tuple union prevents mismatched key/payload pairs, including union keys. */
export type EmitArgs<E extends object> = {
  [K in keyof E]-?: undefined extends E[K]
    ? [type: K, payload?: E[K], options?: EmitOptions]
    : [type: K, payload: E[K], options?: EmitOptions];
}[keyof E];

export interface EmitOptions {
  readonly meta?: Readonly<Record<string, unknown>>;
  /** Retain the latest event reaching the listener stage, even if a listener fails. */
  readonly retain?: boolean;
}

export interface EventBusOptions<E extends object> {
  /** Handles errors; otherwise dispatch throws AggregateError after delivery. */
  readonly onError?: (failure: DispatchFailure<E>) => void;
  /** Override host APIs; inherited by every scope. */
  readonly platform?: Partial<EventBusPlatform>;
  /** Maximum retained keys. Oldest updated key is evicted. Default: 100. */
  readonly retainedLimit?: number;
}
export interface EventBusPlatform {
  /** Receives the opaque handle returned by setTimeout. */
  readonly clearTimeout: (timer: unknown) => void;
  /** Supply a compatible controller in hosts without native AbortController. */
  readonly createAbortController: () => AbortController;
  /** Defaults to DOMException, or an Error with the same name when unavailable. */
  readonly createError: (message: string, name: 'AbortError' | 'TimeoutError') => Error;
  /** Schedule asynchronously, like the host setTimeout. Override with clearTimeout as a pair. */
  readonly setTimeout: (callback: () => void, timeout: number) => unknown;
}
/** Narrowing type also narrows payload. */
export type EventContext<E extends object, K extends keyof E = keyof E> = {
  [P in K]-?: { readonly payload: E[P]; readonly type: P } & EventDetails;
}[K];
export interface EventDetails {
  readonly id: number;
  readonly meta: Readonly<Record<string, unknown>>;
  readonly propagationStopped: boolean;
  readonly replayed: boolean;
  /** Prevent subsequent middleware and listeners from running. */
  stopPropagation(): void;
  readonly timestamp: number;
}
export type EventMap = Record<string, unknown>;

/** Strings use derivative-matcher's full-string DSL. */
export type EventPattern = { test(input: string): boolean } | RegExp | string;

export type Listener<E extends object, K extends keyof E> = (
  payload: E[K],
  event: EventContext<E, K>,
) => MaybePromise<void>;

export type MaybePromise<T> = PromiseLike<T> | T;

export type Middleware<E extends object> = (event: EventContext<E>, next: () => void) => void;

export interface MiddlewareOptions {
  readonly pattern?: EventPattern;
  readonly signal?: AbortSignal;
}

export type Observer<E extends object> = (event: EventContext<E>) => MaybePromise<void>;

export interface SubscriptionOptions {
  readonly once?: boolean;
  /** Higher runs first; ties preserve registration order across all listener kinds. */
  readonly priority?: number;
  /** Immediately deliver retained events. Default: false. Replay is synchronous. */
  readonly replay?: boolean;
  readonly signal?: AbortSignal;
}

/** Idempotent subscription cleanup. */
export type Unsubscribe = () => void;

export interface WaitForOptions<E extends object, K extends keyof E> {
  readonly filter?: (payload: E[K], event: EventContext<E, K>) => boolean;
  readonly replay?: boolean;
  readonly signal?: AbortSignal;
  readonly timeout?: number;
}
