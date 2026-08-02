## @dafengzhen/event-bus

[![GitHub License](https://img.shields.io/github/license/dafengzhen/event-bus?color=blue)](https://github.com/dafengzhen/event-bus)
[![npm version](https://img.shields.io/npm/v/@dafengzhen/event-bus)](https://www.npmjs.com/package/@dafengzhen/event-bus)
[![PRs Welcome](https://img.shields.io/badge/PRs-welcome-brightgreen.svg)](https://github.com/dafengzhen/event-bus/pulls)

[简体中文](./README.zh.md)

A lightweight, fully typed TypeScript event bus with middleware pipeline, pattern-based event listeners, sticky events, and scoped lifecycle management.

## Features

- **Strongly Typed** — Full type safety for event keys, payloads, and listener contexts
- **Middleware Pipeline** — Sync and async middleware with `stop()`, `cancel()`, and `stopImmediate()` flow control
- **Pattern Matching** — Register listeners on regex or string patterns (DFA-compiled for performance)
- **Sticky Events** — Replay events to listeners that register after the event was emitted
- **Event Scopes** — Automatic cleanup of listeners and middleware via lifecycle-scoped containers
- **Priority Ordering** — Control listener execution order with priority values
- **Error Handling** — Configurable error logging and custom error handler

## Installation

```bash
npm install @dafengzhen/event-bus
```

## Quick Start

```typescript
import { EventBus } from '@dafengzhen/event-bus';

// 1. Define your event map
type MyEvents = {
  'user:login': { userId: string; timestamp: number };
  'user:logout': { userId: string };
  notification: { message: string; level: 'info' | 'warn' | 'error' };
};

// 2. Create an EventBus instance
const bus = new EventBus<MyEvents>();

// 3. Register listeners
bus.on('user:login', (payload, ctx) => {
  console.log(`User ${payload.userId} logged in at ${payload.timestamp}`);
});

// 4. Emit events
bus.emit('user:login', { userId: 'abc123', timestamp: Date.now() });
```

## API

### `EventBus<E>`

#### Constructor

```typescript
new EventBus<E>(options?: EventBusOptions<E>)
```

| Option                      | Type                   | Default                   | Description                                         |
| --------------------------- | ---------------------- | ------------------------- | --------------------------------------------------- |
| `logErrors`                 | `boolean`              | `true`                    | Log listener/middleware errors to `console.error`   |
| `onError`                   | `(e: unknown) => void` | —                         | Custom error handler for listener/middleware errors |
| `clearGlobalCacheOnDispose` | `boolean`              | `false`                   | Clear the global regex compile cache on disposal    |
| `stickyMax`                 | `number`               | `200`                     | Max number of distinct sticky event keys            |
| `stickyExactMax`            | `number`               | `1`                       | Max sticky events per exact event key               |
| `stickyPatternMaxPerKey`    | `number`               | `stickyMax`               | Max sticky events per pattern-matched key           |
| `runtime`                   | `DispatcherRuntime<E>` | `new DispatcherRuntime()` | Custom dispatcher runtime                           |

#### Registering Listeners

```typescript
// Exact listener — fires on every emission
bus.on('user:login', (payload, ctx) => {
  /*...*/
});

// One-time listener — auto-removed after first invocation
bus.once('user:login', (payload, ctx) => {
  /*...*/
});

// Pattern listener — fires when event key matches the pattern
bus.onMatch(/^user:/, (event, payload, match, ctx) => {
  console.log(`Matched event: ${event}`);
});

// One-time pattern listener
bus.onceMatch('user:login', (event, payload, match, ctx) => {
  /*...*/
});
```

All registration methods accept an optional `OnOptions`:

```typescript
{
  priority?: number;       // Higher = earlier execution (default: 0 for exact, 80 for pattern)
  consumeSticky?: boolean; // Override sticky consumption behavior
}
```

All return an `Off` function to unregister:

```typescript
const off = bus.on('user:login', handler);
off(); // Remove the listener
```

#### Emitting Events

```typescript
// Synchronous emit
bus.emit('user:login', { userId: 'abc', timestamp: Date.now() });

// Async emit (supports async middleware)
await bus.emitAsync('user:login', { userId: 'abc', timestamp: Date.now() });

// With options
bus.emit('user:login', payload, {
  sticky: true, // Store for replay to future listeners
  stickyMode: 'consume', // 'consume' (remove after replay) or 'replay' (default, persist)
  origin: 'auth-module', // Origin tag for debugging
  metaPatch: { requestId: 'req-1' }, // Shared metadata
});
```

#### Middleware

```typescript
// Synchronous middleware
bus.use((ctx, next) => {
  console.log(`Processing: ${String(ctx.event)}`);
  next(); // Must call next() to continue dispatch
});

// Async middleware
bus.useAsync(async (ctx, next) => {
  await someAsyncOperation();
  await next();
});

// Middleware flow control
bus.use((ctx, next) => {
  ctx.stop(); // Stop propagation (current middleware may still call next)
  ctx.stopImmediate(); // Stop immediately (no further middleware or listeners)
  ctx.cancel(); // Cancel the event entirely
  next();
});

// Conditional middleware
bus.use(logger, {
  pattern: 'user:', // Only run for events matching this pattern
  match: (ctx) => ctx.event !== 'user:logout', // Custom predicate
});
```

#### Event Scopes

Scopes provide automatic cleanup of listeners and middleware:

```typescript
const scope = bus.createScope();

// All registrations are bound to the scope
scope.on('user:login', handler);
scope.use(middleware);

// Dispose to clean up all scope-bound registrations
scope.dispose();

// Nest scopes for hierarchical lifecycle
const childScope = bus.createScope(scope);
childScope.dispose(); // Parent scope is unaffected
```

Use `withScope` for temporary scopes:

```typescript
await bus.withScope(async (scope) => {
  scope.on('user:login', handler);
  // ... handler is automatically removed when scope is disposed
});
```

#### Sticky Events

Sticky events are replayed to listeners that register after the event was emitted:

```typescript
// Emit a sticky event
bus.emit('user:login', { userId: 'abc', timestamp: Date.now() }, { sticky: true });

// Late-registering listener receives the sticky event immediately
bus.on('user:login', (payload) => {
  console.log(payload.userId); // 'abc'
});
```

Sticky modes:

- `'replay'` (default) — Event persists and replays to all future listeners
- `'consume'` — Event is removed after being replayed once

#### Lifecycle

```typescript
bus.clearAll(); // Remove all listeners, middleware, and sticky events
bus.clearListeners(); // Remove only listeners (keep middleware & sticky events)
bus.dispose(); // Full cleanup — instance cannot be used again
```

### Listener Context

Listeners receive a frozen context object with reactive lifecycle state:

```typescript
bus.on('user:login', (payload, ctx) => {
  ctx.event; // The event key
  ctx.payload; // The event payload
  ctx.id; // Monotonic emit ID
  ctx.timestamp; // Unix timestamp of emission
  ctx.origin; // Optional origin tag
  ctx.isStopped; // Whether propagation has been stopped
  ctx.isCanceled; // Whether the event has been canceled
  ctx.isImmediateStopped; // Whether immediate stop was called
  ctx.meta; // Shared metadata (readonly)
  ctx.phase; // 'exact' or 'pattern'
});
```

### Pattern Handlers

Pattern handlers receive additional match information:

```typescript
bus.onMatch(/^user:(?<action>\w+)$/, (event, payload, match, ctx) => {
  console.log(match.action); // Named capture group value
});
```

## License

[MIT](./LICENSE)
