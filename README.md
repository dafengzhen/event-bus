## event-bus

[![GitHub License](https://img.shields.io/github/license/dafengzhen/event-bus?color=blue)](https://github.com/dafengzhen/event-bus)
[![PRs Welcome](https://img.shields.io/badge/PRs-welcome-brightgreen.svg)](https://github.com/dafengzhen/event-bus/pulls)

[简体中文](./README.zh.md)

`EventBus` is a lightweight, strongly-typed (TypeScript generics) event bus featuring:

- Exact listeners (`on` / `once` / `off`)
- Pattern listeners: params, wildcards, and glob segments
- Middleware pipeline (·) with optional filters; supports `ctx.block()` to stop dispatch
- Scoped lifetimes via `EventScope` for automatic cleanup
- Sticky events with replay/consume semantics and bounded retention
- Safe error handling: exceptions are captured and reported without breaking dispatch

## Installation

```bash
npm install @dafengzhen/event-bus
```

## Basic Usage

### Define Event Map

```ts
type MyEvents = {
  'user:login': { id: string };
  'user:logout': { id: string };
  'order:created': { orderId: string; amount: number };
};

const bus = new EventBus<MyEvents>();
```

## Exact Listeners (on / once / off)

```ts
const off = bus.on('user:login', (payload) => {
  console.log('login', payload.id);
});

bus.emit('user:login', { id: 'u1' });

off();
```

One-time:

```ts
bus.once('user:logout', (payload) => {
  console.log('logout once', payload.id);
});
```

### Pattern Listeners

A string is treated as a pattern when it contains wildcard/param/glob syntax:

```ts
bus.on('user:{id}:**', (event, payload, params) => {
  console.log(event); // e.g. "user:u1:profile:update"
  console.log(params.id); // "u1"
});
```

Pattern syntax (split by `separator`, default `:`):

- `**` deep wildcard: matches 0..N segments
- `*` segment wildcard: matches exactly one segment
- `{name}` param segment: captured into `params.name`
- glob segment: supports `*`, `?`, and character classes `[abc]` / `[!abc]`

Examples:

```ts
bus.on('order:*', (event) => console.log('any order event:', event));
bus.on('foo[ab]', (event) => console.log('matches fooa or foob:', event));
bus.on('user:{id}:profile:?pdate', (event, _, params) => {
  console.log('glob+param', params.id, event);
});
```

### Custom Separator

Default separator is` :`. Override via options:

```ts
bus.on('api/{ver}/**', (event, payload, params) => {}, { separator: '/' });
bus.emit('api/v1/users/create' as any, {
  /* ... */
});
```

### Middleware (use)

Signature: (ctx, next) => void | Promise<void>

- Runs in insertion order
- Must call `next()` exactly once to continue
- `ctx.block()` stops the remaining pipeline

```ts
bus.use(async (ctx, next) => {
  const t0 = performance.now();
  await next();
  console.log('cost(ms)=', performance.now() - t0);
});
```

Run only for matching string events:

```ts
bus.use(
  (ctx, next) => next(),
  { pattern: 'user:**' }, // 只对 string event 且匹配 pattern 才运行
);
```

Run only when any pattern listener would match:

```ts
bus.use((ctx, next) => next(), { onlyWhenPatternListenerMatched: true });
```

Custom predicate:

```ts
bus.use((ctx, next) => next(), { match: (ctx) => ctx.meta?.traceId != null });
```

### Emit Context（ctx）

Available in middleware and listeners:

- `ctx.event`, ctx.payload
- `ctx.meta` (shallow-cloned from `emit` `metaPatch`)
- `ctx.params` (params for the current pattern listener; `{}` for exact listeners)
- `ctx.matched` (frozen, sorted pattern matches)
- `ctx.block()` / `ctx.blocked`

### Sticky for exact (typed) events

When `emit(..., { sticky: true })`, the payload will be saved, and future `on(event, ...)` will be replayed immediately:

```ts
bus.emit('user:login', { id: 'u1' }, { sticky: true });

bus.on('user:login', (p) => {
  // immediately receives { id: 'u1' }
});
```

Retention per exact key is controlled by `stickyExactMax` (default `1`):

```ts
const bus = new EventBus<MyEvents>({ stickyExactMax: 5 });
```

### Sticky for pattern replay (string event FIFO)

String events are additionally stored in a bounded FIFO queue for pattern replay (`stickyMax`, default `200`).

- `stickyMode: 'replay'` keeps the sticky entry after replay (default)
- `stickyMode: 'consume'` removes it after replay

```ts
bus.emit('user:login', { id: 'u1' }, { sticky: true, stickyMode: 'consume' });
```

Listeners can override via consumeSticky:

```ts
bus.on('user:login', (p) => {}, { consumeSticky: false });
```

### Error Handling (onError)

Listener/handler errors are captured:

- If `onError` is provided: called synchronously
- Otherwise: logged and rethrown asynchronously (microtask) to avoid breaking dispatch

```ts
const bus = new EventBus<MyEvents>({
  onError: (err) => {
    // e.g. Sentry.captureException(err)
    console.error('EventBus error:', err);
  },
});
```

### Scopes (EventScope)

Use scopes to auto-dispose temporary listeners:

```ts
await bus.withScope(async (scope) => {
  bus.on('user:login', () => console.log('temp'));
  bus.emit('user:login', { id: 'u1' });
}); // auto cleanup
```

You can also create a scope manually:

```ts
const scope = bus.createScope();
const off = bus.on('user:login', () => {});
// scope.registerOff(off) is done automatically internally (there is an active scope when registering)
scope.destroy();
```

### Reset / Destroy

`reset()` clears listeners, middlewares, and sticky state (keeps pattern compilation cache)

`destroy()` fully tears down the instance; all subsequent API calls throw

```ts
bus.reset();
bus.destroy();
```

## Contributing

Pull requests are welcome!

## License

[MIT](https://opensource.org/licenses/MIT)
