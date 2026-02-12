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

### 精确事件监听（on / once / off）

```ts
const off = bus.on('user:login', (payload) => {
  console.log('login', payload.id);
});

bus.emit('user:login', { id: 'u1' });

off(); // 等价于 bus.off('user:login', listener)
```

一次性监听：

```ts
bus.once('user:logout', (payload) => {
  console.log('logout once', payload.id);
});
```

### 模式监听（Pattern）

当第一个参数是字符串且 "看起来像 pattern"（包含 `*` / `**` / `{param}` / glob）时，将按 pattern 处理：

```ts
bus.on('user:{id}:**', (event, payload, params) => {
  console.log(event); // e.g. "user:u1:profile:update"
  console.log(params.id); // "u1"
});
```

支持的 pattern 语法（按 segment 分割，默认分隔符 `:`）：

- `**`：深通配（匹配 0..N 段）
- `*`：段通配（匹配恰好 1 段）
- `{name}`：参数段（捕获到 params.name）
- glob 段：`*`、`?`、字符类 `[abc]` / `[!abc]`

示例：

```ts
bus.on('order:*', (event) => console.log('any order event:', event));
bus.on('foo[ab]', (event) => console.log('matches fooa or foob:', event));
bus.on('user:{id}:profile:?pdate', (event, _, params) => {
  console.log('glob+param', params.id, event);
});
```

### 分隔符（separator）

默认按 `:` 分段。可在 `on`/`once`/`use` 的 options 里指定：

```ts
bus.on('api/{ver}/**', (event, payload, params) => {}, { separator: '/' });
bus.emit('api/v1/users/create' as any, {
  /* ... */
});
```

### 中间件（use）

中间件签名：(ctx, next) => void | Promise<void>

- 中间件按注册顺序执行
- 必须调用 `next()` 才会继续
- 可调用 `ctx.block()` 直接阻断剩余管线（包括后续 middleware 和 listener）

```ts
bus.use(async (ctx, next) => {
  const t0 = performance.now();
  await next();
  console.log('cost(ms)=', performance.now() - t0);
});
```

按 pattern 限制中间件仅对某些字符串事件触发：

```ts
bus.use(
  (ctx, next) => next(),
  { pattern: 'user:**' }, // 只对 string event 且匹配 pattern 才运行
);
```

仅当 "存在任意 pattern listener 能匹配该事件" 时运行：

```ts
bus.use((ctx, next) => next(), { onlyWhenPatternListenerMatched: true });
```

自定义 match predicate：

```ts
bus.use((ctx, next) => next(), { match: (ctx) => ctx.meta?.traceId != null });
```

### Emit Context（ctx）

在 middleware 与 listener 中，你可以访问：

- `ctx.event`：事件名
- `ctx.payload`：payload
- `ctx.meta`：元信息（从 `emit` 的 `metaPatch` 浅拷贝而来）
- `ctx.params`：当前 pattern listener 的 params（精确 listener 为空对象）
- `ctx.matched`：本次匹配到的 pattern listeners（冻结、按优先级/注册顺序排序）
- `ctx.block()` / `ctx.blocked`

### 精确事件的 sticky

当 `emit(..., { sticky: true })` 时，会把 payload 存起来，未来 `on(event, ...)` 会立刻重放：

```ts
bus.emit('user:login', { id: 'u1' }, { sticky: true });

bus.on('user:login', (p) => {
  // 这里会立即收到 { id: 'u1' }
});
```

`stickyExactMax` 控制每个精确事件最多保留多少条历史（默认 `1`）：

```ts
const bus = new EventBus<MyEvents>({ stickyExactMax: 5 });
```

### Pattern 的 sticky

对于 string 事件，`EventBus` 维护一个 FIFO 队列用于 pattern listener 的重放匹配（默认 `stickyMax=200`）：

- `stickyMode: 'replay'`：重放后仍保留（默认）
- `stickyMode: 'consume'`：重放后移除（一次性消费）

```ts
bus.emit('user:login', { id: 'u1' }, { sticky: true, stickyMode: 'consume' });
```

监听侧也可用 `consumeSticky` 覆盖（true/false）：

```ts
bus.on('user:login', (p) => {}, { consumeSticky: false });
```

### 错误处理（onError）

监听器/handler 抛错会被捕获： 若提供 `onError`：同步回调处理错误，否则：`console.error` 并通过异步抛出

```ts
const bus = new EventBus<MyEvents>({
  onError: (err) => {
    // e.g. Sentry.captureException(err)
    console.error('EventBus error:', err);
  },
});
```

### 作用域（EventScope）

`EventScope` 用于批量管理监听生命周期：当 scope destroy 时自动 off：

```ts
await bus.withScope(async (scope) => {
  bus.on('user:login', () => console.log('temp')); // 注册在当前 scope
  bus.emit('user:login', { id: 'u1' });
}); // scope 自动销毁，监听自动清理
```

你也可以手动创建 scope：

```ts
const scope = bus.createScope();
const off = bus.on('user:login', () => {});
// scope.registerOff(off) 会在内部自动做（注册时有 active scope）
scope.destroy();
```

### Reset / Destroy

`reset()`：清空 listeners + middlewares + sticky（不清 pattern 编译缓存）

`destroy()`：彻底销毁实例，清空所有状态并标记 destroyed；之后调用任何 API 都会抛错

```ts
bus.reset();
bus.destroy();
```

## 贡献

欢迎贡献 PR！

## License

[MIT](https://opensource.org/licenses/MIT)
