export type EventMap = Record<string, unknown>;

export type Listener<Payload> = Payload extends void ? () => void : (payload: Payload) => void;

export type AnyListener<E extends EventMap> = <K extends keyof E>(event: K, payload: E[K]) => void;

export type PatternKind = 'star' | 'prefix' | 'param';

export type Pattern<_K extends string> = '*' | `${string}:*` | `${string}{${string}}${string}`;

type PatternToKeyShape<P extends string> = P extends '*'
  ? string
  : P extends `${infer Prefix}:*`
    ? `${Prefix}:${string}` | Prefix
    : P extends `${infer A}{${string}}${infer B}`
      ? `${A}${string}${B}`
      : never;

export type MatchKeys<Keys extends string, P extends Pattern<Keys>> = Extract<
  Keys,
  PatternToKeyShape<P>
>;

export type EmitContext<E extends EventMap, K extends keyof E> = {
  readonly blocked: boolean;
  readonly event: K;
  readonly payload: E[K];
  readonly matched: readonly PatternListenerInfo<E>[];
  readonly meta: Record<string, unknown>;
  block(): void;
};

export type Middleware<E extends EventMap> = <K extends keyof E>(
  ctx: EmitContext<E, K>,
  next: () => Promise<void>,
) => Promise<void> | void;

export type PatternMiddleware<E extends EventMap> = (
  ctx: EmitContext<E, keyof E>,
  next: () => Promise<void>,
) => Promise<void> | void;

export type PatternListenerInfo<E extends EventMap> = {
  readonly kind: PatternKind;
  readonly once?: boolean;
  readonly priority: number;
  readonly prefix?: string;
  readonly handler: (event: keyof E, payload: E[keyof E]) => void;
};

export type CompiledPatternListener<E extends EventMap> = {
  readonly kind: PatternKind;
  readonly once?: boolean;
  readonly priority: number;
  readonly prefix?: string;
  readonly prefixWithColon?: string;
  readonly pattern: Pattern<Extract<keyof E, string>> | RegExp | ((event: string) => boolean);
  readonly handler: (event: keyof E, payload: E[keyof E], params?: Record<string, string>) => void;
  match(event: string): null | Record<string, string>;
};

export type PatternOptions = { once?: boolean; priority?: number };
