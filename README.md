## event-bus

[![GitHub License](https://img.shields.io/github/license/dafengzhen/event-bus?color=blue)](https://github.com/dafengzhen/event-bus)
[![PRs Welcome](https://img.shields.io/badge/PRs-welcome-brightgreen.svg)](https://github.com/dafengzhen/event-bus/pulls)

[简体中文](./README.zh.md)

A fully typed EventBus for TypeScript with middleware and pattern-based event matching.

## Features

- Type-safe events & payloads
- Middleware pipeline
- Pattern listeners (`*`, `prefix:*`, `{param}`)
- Listener priority & `once`
- Framework agnostic

## Installation

```bash
npm install @dafengzhen/event-bus
```

## Basic Usage

```ts
type Events = {
  'user:login': { id: string };
  'user:logout': void;
  'user:42:update': {};
};

const bus = new EventBus<Events>();

bus.on('user:login', ({ id }) => console.log(id));
bus.onPattern('user:{id}:update', (_e, _p, params) => console.log(params?.id));

bus.use(async (ctx, next) => {
  console.log(ctx.event);
  await next();
});

bus.emit('user:login', { id: 'u1' });
bus.emit('user:42:update', {});
```

## Wildcard & Prefix

```ts
bus.onPattern('*', (event, payload) => {});
bus.onPattern('user:*', (event, payload) => {});
```

## Parameterized Patterns

```ts
bus.onPattern('user:{id}:update', (_event, payload, params) => {
  console.log(params.id);
});
```

## Middleware

```ts
bus.use(async (ctx, next) => {
  console.log('before', ctx.event);
  await next();
});
```

You can block event propagation:

```ts
bus.use((ctx) => {
  if (ctx.event === 'user:logout') {
    ctx.block();
  }
});
```

## Pattern Middleware

```ts
bus.usePattern(async (ctx, next) => {
  console.log(ctx.matched);
  await next();
});
```

## Contributing

Pull requests are welcome!

## License

[MIT](https://opensource.org/licenses/MIT)
