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

## Contributing

Pull requests are welcome!

## License

[MIT](https://opensource.org/licenses/MIT)
