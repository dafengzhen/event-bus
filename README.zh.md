## event-bus

[![GitHub License](https://img.shields.io/github/license/dafengzhen/event-bus?color=blue)](https://github.com/dafengzhen/event-bus)
[![PRs Welcome](https://img.shields.io/badge/PRs-welcome-brightgreen.svg)](https://github.com/dafengzhen/event-bus/pulls)

[English](./README.md)

`EventBus` 是一个轻量、强类型（TypeScript 泛型）的事件总线，支持：

- 精确事件监听（`on` / `once` / `off`）
- 模式监听（Pattern Listener）：参数、通配符、glob 字符类
- 中间件管线（`use`）：可按 pattern / predicate 过滤，支持 `ctx.block()` 阻断
- 作用域生命周期（`EventScope`）：自动清理临时监听
- Sticky 事件：对未来订阅者重放（可设置上限、支持 consume / replay）

## 安装

```bash
npm install @dafengzhen/event-bus
```

## 示例

### 定义事件类型

```ts
type MyEvents = {
  'user:login': { id: string };
  'user:logout': { id: string };
  'order:created': { orderId: string; amount: number };
};

const bus = new EventBus<MyEvents>();
```

## 贡献

欢迎贡献 PR！

## License

[MIT](https://opensource.org/licenses/MIT)
