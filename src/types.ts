export type CompiledPatternListener<E extends EventMap> = {
  handler: PatternHandler<E>;

  match: (event: string) => null | Record<string, string>;

  once: boolean;

  pattern: string;

  priority: number;

  separator: string;

  seq: number;
};

export type CompiledSeg =
  | { key: string; type: 'param' }
  | { re: RegExp; src: string; type: 'glob' }
  | { type: 'deepWildcard' }
  | { type: 'exact'; value: string }
  | { type: 'segWildcard' };

export type EmitContext<E extends EventMap, K extends keyof E> = {
  block(): void;

  readonly blocked: boolean;

  event: K;

  matched: ReadonlyArray<PatternListenerInfo<E>>;

  meta: Record<string, unknown>;

  params: Record<string, string>;

  payload: E[K];
};

export type EmitOptions = {
  metaPatch?: Record<string, unknown>;

  sticky?: boolean;

  stickyMax?: number;

  stickyMode?: StickyMode;
};

export type EventMap = Record<string, any>;

export type Listener<P> = (payload: P) => void;

export type Middleware<E extends EventMap> = (
  ctx: EmitContext<E, keyof E>,
  next: () => Promise<void>,
) => Promise<void> | void;

export type MiddlewareEntry<E extends EventMap> = {
  fn: Middleware<E>;

  match?: (ctx: EmitContext<E, keyof E>) => boolean;
};

export type OnOptions = {
  consumeSticky?: boolean;

  priority?: number;

  separator?: string;
};

export type ParamNode = { k: string; prev?: ParamNode; v: string };

export type PatternHandler<E extends EventMap> = (
  event: string,
  payload: E[keyof E],
  params: Record<string, string>,
  ctx?: EmitContext<E, keyof E>,
) => void;

export type PatternListenerInfo<E extends EventMap> = {
  handler: PatternHandler<E>;

  once: boolean;

  params: Readonly<Record<string, string>>;

  pattern: string;

  priority: number;
};

export type StickyMode = 'consume' | 'replay';

export type TrieNode<E extends EventMap> = {
  deep?: TrieNode<E>;

  end: CompiledPatternListener<E>[];

  exact: Map<string, TrieNode<E>>;

  globs?: Array<{ node: TrieNode<E>; re: RegExp; src: string }>;

  id: number;

  params?: Array<{ key: string; node: TrieNode<E> }>;

  star?: TrieNode<E>;
};

export type UseOptions<E extends EventMap> = {
  match?: (ctx: EmitContext<E, keyof E>) => boolean;

  onlyWhenPatternListenerMatched?: boolean;

  pattern?: string;

  separator?: string;
};
