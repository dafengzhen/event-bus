# event-bus

[简体中文](./README.zh.md)

A typed TypeScript event bus with explicit lifetimes, pattern observers, middleware, and retained events.

```bash
npm install @dafengzhen/event-bus
```

```ts
import { createEventBus } from '@dafengzhen/event-bus';

interface Events {
  'user:login': { id: string };
  'user:logout': string;
  ready: void;
}

const bus = createEventBus<Events>();
const off = bus.on('user:login', ({ id }) => console.log(id));

bus.emit('user:login', { id: 'Ada' });
bus.emit('ready'); // void / undefined payloads may be omitted

off(); // idempotent
```

`new EventBus<Events>(options)` is also available. Event maps can be interfaces or type aliases; no index signature is required. Exact keys support strings, numbers, and symbols. There is one payload per event; use an object or tuple when you need several values. Invalid keys, missing required payloads, and mismatched key/payload pairs are type errors.

## Package entry points and compatibility

| Entry point                    | ESM                                                             | CommonJS                                                             |
| ------------------------------ | --------------------------------------------------------------- | -------------------------------------------------------------------- |
| `@dafengzhen/event-bus`        | `import { createEventBus } from '@dafengzhen/event-bus'`        | `const { createEventBus } = require('@dafengzhen/event-bus')`        |
| `@dafengzhen/event-bus/legacy` | `import { createEventBus } from '@dafengzhen/event-bus/legacy'` | `const { createEventBus } = require('@dafengzhen/event-bus/legacy')` |

Both entries expose the same API, with declarations for ESM and CommonJS.

Both entries load the `core-js` `AggregateError` polyfill. `AbortController` must be available when creating a bus, either natively, through a polyfill, or through `platform.createAbortController`. `AbortSignal.any()` is not required. If `DOMException` is unavailable, cancellation and timeout errors fall back to `Error` objects with the appropriate `name`.

### Host API overrides

Use `platform` to adapt cancellation, errors, or timers to your host or test environment. Overrides are partial; the bus and all scopes created through `scope()` share the configured adapters.

```ts
const bus = createEventBus<Events>({
  platform: {
    createError: (message, name) => Object.assign(new Error(message), { name }),
  },
});
```

| `platform` member               | Default                                         | Contract                                                                          |
| ------------------------------- | ----------------------------------------------- | --------------------------------------------------------------------------------- |
| `createAbortController()`       | `new AbortController()`                         | Return a compatible controller with a signal supporting abort event listeners     |
| `createError(message, name)`    | `DOMException`, falling back to a named `Error` | Return an error named `AbortError` or `TimeoutError`                              |
| `setTimeout(callback, timeout)` | Host `setTimeout`                               | Schedule asynchronously and return an opaque timer handle                         |
| `clearTimeout(timer)`           | Host `clearTimeout`                             | Cancel the handle returned by `setTimeout`; override both timer adapters together |

## Subscribe

```ts
bus.on('user:login', (payload, event) => {
  console.log(payload.id, event.id, event.meta);
});
bus.once('ready', () => console.log('once'));

bus.onPattern('user:.*', (event) => {
  if (event.type === 'user:login') {
    console.log(event.payload.id); // narrowed to { id: string }
  }
});
bus.onAny((event) => console.log(event.type, event.payload));
```

`on` and `once` receive `(payload, event)`. `onAny` and `onPattern` receive a discriminated event object, preserving the relationship between `type` and `payload`. Pattern matches do not narrow the event union statically: narrow `event.type` inside the callback.

All subscription methods return an unsubscribe function. Registering the same function twice creates two independent subscriptions.

| Subscription option | Default | Behavior                                                                                           |
| ------------------- | ------- | -------------------------------------------------------------------------------------------------- |
| `priority`          | `0`     | Higher runs first; equal values follow registration order across exact, pattern, and any listeners |
| `once`              | `false` | Unsubscribe before the first invocation, including replay and nested dispatch                      |
| `signal`            | —       | Unsubscribe on abort; an already-aborted signal registers nothing                                  |
| `replay`            | `false` | Synchronously deliver matching retained events during registration                                 |

`once()` always sets `once: true`. Priorities must be finite numbers.

### Patterns

`onPattern(pattern, observer, options?)` and middleware's `pattern` option accept:

- A string in **derivative-matcher DSL**: `user:.*`, `(user|team):.*`, `user:.* & !(user:logout)`.
- A native `RegExp`: `/^user:/`, including Unicode patterns and regex flags.
- A reusable matcher with `test(input: string): boolean`, such as `compile('user:.*')` from `@dafengzhen/derivative-matcher`.

DSL patterns match the **entire event name**: `user:` does not match `user:login`; use `user:.*`. They are not glob patterns. The DSL's supported alphabet is Latin-1, unescaped whitespace is ignored, and it supplies boolean results without captures. Native regexes use JavaScript's normal search semantics. Each regex registration owns its cursor, so `g`/`y` and changes to the caller's `lastIndex` do not affect delivery. Pattern observers only match string keys; `onAny` also sees number and symbol keys. Invalid DSL patterns throw during registration. Custom matchers should be pure and nonthrowing.

## Dispatch

```ts
const result = bus.emit(
  'user:login',
  { id: 'Ada' },
  {
    meta: { requestId: 'req-1' },
  },
);
console.log(result.delivered, result.errors, result.event);

bus.on('user:logout', async (id) => {
  await saveLogout(id);
});
await bus.emitAsync('user:logout', 'Ada');
```

`emit()` delivers synchronously. `emitAsync()` delivers sequentially and awaits every handler, preserving the same priority order. Separate `emitAsync()` calls may overlap; there is no global queue. A `once` subscription is removed before invocation, so concurrent dispatches cannot invoke it twice.

Use `emitAsync()` for asynchronous handlers. `emit()` rejects matching `useAsync()` middleware before running handlers. If a listener or `use()` middleware returns a promise during synchronous delivery, it is reported as a `TypeError` through the dispatch error policy, and its rejection is observed. The already-started async operation cannot be undone. Replay is synchronous too; use `waitFor(type, { replay: true })` when you want to await a retained value and then do async work.

Dispatch snapshots subscriptions and middleware at its start. Registrations added during dispatch participate in later emissions. Registrations removed or aborted before their turn are skipped. `clear()` and `dispose()` stop the remaining work of an in-flight dispatch, including work waiting on middleware; already-running callbacks complete normally.

### Event context

| Field                | Meaning                                        |
| -------------------- | ---------------------------------------------- |
| `type`, `payload`    | Correlated key and payload                     |
| `id`                 | Increasing emission ID, local to the bus       |
| `timestamp`          | Emission time in milliseconds since Unix epoch |
| `meta`               | A shallow frozen copy of emission metadata     |
| `replayed`           | Whether this invocation is a replay            |
| `propagationStopped` | Reactive propagation state                     |
| `stopPropagation()`  | Skip subsequent middleware and listeners       |

The event object and dispatch result are frozen. Payloads and nested metadata are shared by reference, without cloning or deep freezing. Replay preserves the original ID, timestamp, payload, and metadata, with fresh propagation state. Stopping propagation affects delivery, without undoing callbacks or side effects that already ran.

## Middleware

```ts
const off = bus.use(
  (event, next) => {
    if (event.meta.blocked) return; // omit next() to prevent delivery
    next();
  },
  { pattern: 'user:.*' },
);

bus.useAsync(async (event, next) => {
  await authorize(event);
  await next();
  await audit(event);
});
```

Middleware runs in registration order before listeners. Call `next()` at most once and before the middleware completes. Omitting `next()` prevents downstream execution and retention. `event.stopPropagation()` also prevents downstream execution, even if you call `next()` afterward. Middleware supports `{ pattern?, signal? }`; all registration methods return an unsubscribe function.

`use()` is synchronous. In `emitAsync()`, its `next()` starts the downstream work but cannot synchronously wait for it; use `useAsync()` and `await next()` for before/after logic around async delivery. Dispatch also drains an unawaited `next()` before completing, but middleware should await it to preserve its own before/after ordering.

## Wait for an event

```ts
const controller = new AbortController();
const login = bus.waitFor('user:login', {
  filter: (payload) => payload.id === 'Ada',
  timeout: 5_000,
  signal: controller.signal,
  replay: true,
});

bus.emit('user:login', { id: 'Ada' });
console.log((await login).payload.id);
```

`waitFor` resolves to the event context and automatically removes its listener, abort hook, and timeout. `filter` defaults to accepting every payload, `replay` defaults to `false`, and no timeout is applied unless provided. Timeout must be a finite number from `0` to `2147483647` milliseconds. Timeout rejects with `TimeoutError`; abort rejects with the signal's reason, falling back to `AbortError` when the reason is `undefined`. Explicit reasons such as `null`, `false`, `0`, and `''` are preserved. Filter exceptions reject the wait without failing the emission. `clear()`, bus disposal, and owning scope disposal reject pending waits with `AbortError`. Rejections from `waitFor` must be handled by the caller.

## Retain and replay

```ts
bus.emit('ready', undefined, { retain: true });
bus.once('ready', (_, event) => console.log(event.replayed), { replay: true });

bus.clearRetained('ready');
bus.clearRetained(); // all keys
```

Retention stores the latest event per key, replacing its previous value. Events are retained when middleware reaches the listener stage, before listeners run, even if a listener later fails. Emissions blocked by middleware are not retained. Ordinary emissions without `retain: true` leave any previous retained value unchanged.

Late listeners only replay when explicitly requested. Replay invokes only the new subscriber, bypassing middleware and other listeners. Matching keys replay in order of their last retained update, oldest first. `once` with replay receives the first matching retained value and unsubscribes. If replay throws an unhandled error, the new subscription is removed before the registration call throws.

`createEventBus({ retainedLimit: 100 })` limits distinct retained keys (default `100`). Updating a key refreshes its position; the oldest updated key is evicted when the limit is exceeded. `0` disables retention. The limit must be a nonnegative safe integer. Disposing a bus never clears derivative-matcher's shared compilation cache.

## Scopes and cleanup

```ts
const scope = bus.scope();
const child = scope.scope();

scope.on('user:login', ({ id }) => console.log(id));
child.onAny((event) => console.log(event.type));
const pending = child.waitFor('ready');

scope.dispose(); // also disposes child and rejects pending
await pending.catch(() => {});
```

Scopes expose `on`, `once`, `onAny`, `onPattern`, `use`, `useAsync`, `waitFor`, `scope`, `dispose`, `signal`, and `disposed`. Emit through the bus. Ownership is explicit and stays correct across `await`; there is no ambient scope or global runtime. Subscription signals combine caller cancellation with scope cancellation. Disposing a child leaves its parent and siblings active; disposing the bus disposes all scopes. Disposal is idempotent. Registration through a disposed bus or scope throws; `waitFor` returns a rejected promise.

| Bus method             | Behavior                                                                                    |
| ---------------------- | ------------------------------------------------------------------------------------------- |
| `listenerCount()`      | Total active listener registrations, excluding middleware                                   |
| `listenerCount(type)`  | Exact, pattern, and any listeners matching a key                                            |
| `clearRetained(type?)` | Forget retained data only                                                                   |
| `clear()`              | Remove listeners, middleware, and retained data; reject waits; keep bus and scopes reusable |
| `dispose()`            | Clear everything and permanently close the bus and its scopes                               |
| `disposed`             | Whether permanently closed                                                                  |

## Errors

By default, listener errors are collected while other listeners continue; dispatch then throws `AggregateError` (`emitAsync` rejects). A middleware failure prevents its unstarted downstream work; downstream work already started through `next()` is allowed to complete. Configuration and pattern compilation errors throw directly.

```ts
const bus = createEventBus<Events>({
  onError: ({ error, event, phase }) => {
    console.error(event.type, phase, error);
  },
});
```

`onError` runs synchronously after delivery for each failure and handles that failure. The returned result still contains `errors: readonly DispatchFailure[]`; each entry has `error`, `event`, and `phase` (`listener`, `middleware`, or `replay`). `delivered` counts invocations, including failed ones. An error thrown by `onError` is aggregated and propagated without recursive reporting. Do not provide an async error handler.

## License

[MIT](./LICENSE)
