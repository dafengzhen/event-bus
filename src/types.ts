/**
 * Event map definition.
 *
 * Key is the event name, value is the payload type of that event.
 */
export type EventMap = Record<string, unknown>;

/**
 * Listener type for a specific payload.
 *
 * - If payload is `void`, listener receives no arguments.
 * - Otherwise, listener receives the payload.
 */
export type Listener<Payload> = Payload extends void ? () => void : (payload: Payload) => void;

/**
 * Listener that listens to all events.
 */
export type AnyListener<E extends EventMap> = <K extends keyof E>(event: K, payload: E[K]) => void;

/**
 * Pattern matching kind.
 */
export type PatternKind =
  | 'star' // '*'
  | 'prefix' // 'foo:*'
  | 'param' // 'foo:{id}:bar'
  | 'exact'; // 'foo:bar'

/**
 * Pattern syntax.
 *
 * Supported forms:
 * - '*'                      : match all events
 * - 'prefix:*'               : match prefix or prefix:xxx
 * - 'foo:{param}:bar'        : match param segments
 */
export type Pattern<_K extends string> = '*' | `${string}:*` | `${string}{${string}}${string}`;

/**
 * Convert a pattern string to the possible event key shapes.
 */
export type PatternToKeyShape<P extends string> = P extends '*'
  ? string
  : P extends `${infer Prefix}:*`
    ? `${Prefix}:${string}` | Prefix
    : P extends `${infer A}{${string}}${infer B}`
      ? `${A}${string}${B}`
      : never;

/**
 * Extract event keys that match a given pattern.
 */
export type MatchKeys<Keys extends string, P extends Pattern<Keys>> = Extract<
  Keys,
  PatternToKeyShape<P>
>;

/**
 * Emit context passed through middleware chain.
 */
export type EmitContext<E extends EventMap, K extends keyof E> = {
  /** Whether propagation is blocked */
  readonly blocked: boolean;

  /** Current event name */
  readonly event: K;

  /** Payload of the event */
  readonly payload: E[K];

  /** Matched pattern listeners info */
  readonly matched: readonly PatternListenerInfo<E>[];

  /** Free-form metadata container */
  readonly meta: Record<string, unknown>;

  /** Stop further propagation */
  block(): void;
};

/**
 * Middleware executed for every emit.
 */
export type Middleware<E extends EventMap> = <K extends keyof E>(
  ctx: EmitContext<E, K>,
  next: () => Promise<void>,
) => Promise<void> | void;

/**
 * Middleware executed only when pattern listeners are involved.
 */
export type PatternMiddleware<E extends EventMap> = (
  ctx: EmitContext<E, keyof E>,
  next: () => Promise<void>,
) => Promise<void> | void;

/**
 * Public information of a matched pattern listener.
 */
export type PatternListenerInfo<E extends EventMap> = {
  readonly kind: PatternKind;
  readonly once?: boolean;
  readonly priority: number;
  readonly prefix?: string;
  readonly handler: (event: keyof E, payload: E[keyof E]) => void;
};

/**
 * Internal compiled pattern listener.
 */
export type CompiledPatternListener<E extends EventMap> = {
  readonly kind: PatternKind;
  readonly once?: boolean;
  readonly priority: number;
  readonly prefix?: string;
  readonly prefixWithColon?: string;

  /** Original pattern or compiled matcher */
  readonly pattern: Pattern<Extract<keyof E, string>> | RegExp | ((event: string) => boolean);

  /** Listener handler */
  readonly handler: (event: keyof E, payload: E[keyof E], params?: Record<string, string>) => void;

  /**
   * Try match an event.
   * @returns params object or null if not matched
   */
  match(event: string): null | Record<string, string>;
};

/**
 * Options for pattern listeners.
 */
export type PatternOptions = {
  /** Invoke only once */
  once?: boolean;

  /** Priority (higher runs earlier) */
  priority?: number;
};
