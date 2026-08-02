## @dafengzhen/event-bus

[![GitHub License](https://img.shields.io/github/license/dafengzhen/event-bus?color=blue)](https://github.com/dafengzhen/event-bus)
[![npm version](https://img.shields.io/npm/v/@dafengzhen/event-bus)](https://www.npmjs.com/package/@dafengzhen/event-bus)
[![PRs Welcome](https://img.shields.io/badge/PRs-welcome-brightgreen.svg)](https://github.com/dafengzhen/event-bus/pulls)

[English](./README.md)

一个轻量级、完全类型化的 TypeScript 事件总线，支持中间件管道、基于模式的事件监听、粘性事件以及作用域生命周期管理。

## 特性

- **强类型** — 事件键、负载和监听器上下文均具有完整的类型安全
- **中间件管道** — 支持同步和异步中间件，提供 `stop()`、`cancel()`、`stopImmediate()` 流程控制
- **模式匹配** — 支持基于正则表达式或字符串模式（DFA 编译，高性能）的监听器注册
- **粘性事件** — 在事件触发后注册的监听器也能接收到该事件
- **事件作用域** — 通过生命周期作用域容器自动清理监听器和中间件
- **优先级排序** — 通过优先级值控制监听器执行顺序
- **错误处理** — 可配置的错误日志和自定义错误处理器

## 安装

```bash
npm install @dafengzhen/event-bus
```

## 快速开始

```typescript
import { EventBus } from '@dafengzhen/event-bus';

// 1. 定义事件映射
type MyEvents = {
  'user:login': { userId: string; timestamp: number };
  'user:logout': { userId: string };
  notification: { message: string; level: 'info' | 'warn' | 'error' };
};

// 2. 创建 EventBus 实例
const bus = new EventBus<MyEvents>();

// 3. 注册监听器
bus.on('user:login', (payload, ctx) => {
  console.log(`用户 ${payload.userId} 在 ${payload.timestamp} 登录`);
});

// 4. 触发事件
bus.emit('user:login', { userId: 'abc123', timestamp: Date.now() });
```

## API

### `EventBus<E>`

#### 构造函数

```typescript
new EventBus<E>(options?: EventBusOptions<E>)
```

| 选项                        | 类型                   | 默认值                    | 描述                                          |
| --------------------------- | ---------------------- | ------------------------- | --------------------------------------------- |
| `logErrors`                 | `boolean`              | `true`                    | 是否将监听器/中间件错误输出到 `console.error` |
| `onError`                   | `(e: unknown) => void` | —                         | 自定义错误处理器                              |
| `clearGlobalCacheOnDispose` | `boolean`              | `false`                   | 销毁时是否清除全局正则编译缓存                |
| `stickyMax`                 | `number`               | `200`                     | 全局粘性事件键的最大数量                      |
| `stickyExactMax`            | `number`               | `1`                       | 每个精确事件键的最大粘性事件数                |
| `stickyPatternMaxPerKey`    | `number`               | `stickyMax`               | 每个模式匹配键的最大粘性事件数                |
| `runtime`                   | `DispatcherRuntime<E>` | `new DispatcherRuntime()` | 自定义调度器运行时                            |

#### 注册监听器

```typescript
// 精确监听器 — 每次触发都会执行
bus.on('user:login', (payload, ctx) => {
  /*...*/
});

// 一次性监听器 — 首次触发后自动移除
bus.once('user:login', (payload, ctx) => {
  /*...*/
});

// 模式监听器 — 当事件键匹配模式时触发
bus.onMatch(/^user:/, (event, payload, match, ctx) => {
  console.log(`匹配的事件: ${event}`);
});

// 一次性模式监听器
bus.onceMatch('user:login', (event, payload, match, ctx) => {
  /*...*/
});
```

所有注册方法都接受可选的 `OnOptions`：

```typescript
{
  priority?: number;       // 优先级，数值越大越先执行（精确监听器默认 0，模式监听器默认 80）
  consumeSticky?: boolean; // 覆盖粘性事件的消费行为
}
```

所有方法都返回一个 `Off` 函数用于取消注册：

```typescript
const off = bus.on('user:login', handler);
off(); // 移除监听器
```

#### 触发事件

```typescript
// 同步触发
bus.emit('user:login', { userId: 'abc', timestamp: Date.now() });

// 异步触发（支持异步中间件）
await bus.emitAsync('user:login', { userId: 'abc', timestamp: Date.now() });

// 带选项触发
bus.emit('user:login', payload, {
  sticky: true, // 存储为粘性事件，供后续注册的监听器重放
  stickyMode: 'consume', // 'consume'（重放后移除）或 'replay'（默认，持久保留）
  origin: 'auth-module', // 来源标记，便于调试
  metaPatch: { requestId: 'req-1' }, // 共享元数据
});
```

#### 中间件

```typescript
// 同步中间件
bus.use((ctx, next) => {
  console.log(`处理中: ${String(ctx.event)}`);
  next(); // 必须调用 next() 才能继续分发
});

// 异步中间件
bus.useAsync(async (ctx, next) => {
  await someAsyncOperation();
  await next();
});

// 中间件流程控制
bus.use((ctx, next) => {
  ctx.stop(); // 停止传播（当前中间件仍可调用 next）
  ctx.stopImmediate(); // 立即停止（不再执行后续中间件和监听器）
  ctx.cancel(); // 完全取消事件
  next();
});

// 条件中间件
bus.use(logger, {
  pattern: 'user:', // 仅对匹配此模式的事件执行
  match: (ctx) => ctx.event !== 'user:logout', // 自定义断言
});
```

#### 事件作用域

作用域提供监听器和中间件的自动清理：

```typescript
const scope = bus.createScope();

// 所有注册都绑定到此作用域
scope.on('user:login', handler);
scope.use(middleware);

// 销毁作用域以清理所有绑定在其上的注册
scope.dispose();

// 嵌套作用域实现分层生命周期
const childScope = bus.createScope(scope);
childScope.dispose(); // 父作用域不受影响
```

使用 `withScope` 创建临时作用域：

```typescript
await bus.withScope(async (scope) => {
  scope.on('user:login', handler);
  // ... handler 在作用域销毁时自动移除
});
```

#### 粘性事件

粘性事件会在事件触发之后注册的监听器中重放：

```typescript
// 触发粘性事件
bus.emit('user:login', { userId: 'abc', timestamp: Date.now() }, { sticky: true });

// 后续注册的监听器会立即收到粘性事件
bus.on('user:login', (payload) => {
  console.log(payload.userId); // 'abc'
});
```

粘性模式：

- `'replay'`（默认）— 事件持久保留，对所有后续监听器重放
- `'consume'` — 事件在重放一次后被移除

#### 生命周期

```typescript
bus.clearAll(); // 移除所有监听器、中间件和粘性事件
bus.clearListeners(); // 仅移除监听器（保留中间件和粘性事件）
bus.dispose(); // 完全清理 — 实例不可再使用
```

### 监听器上下文

监听器接收一个冻结的上下文对象，包含响应式生命周期状态：

```typescript
bus.on('user:login', (payload, ctx) => {
  ctx.event; // 事件键
  ctx.payload; // 事件负载
  ctx.id; // 单调递增的触发 ID
  ctx.timestamp; // 触发时的 Unix 时间戳
  ctx.origin; // 可选的来源标记
  ctx.isStopped; // 传播是否已被停止
  ctx.isCanceled; // 事件是否已被取消
  ctx.isImmediateStopped; // 是否已调用立即停止
  ctx.meta; // 共享元数据（只读）
  ctx.phase; // 'exact' 或 'pattern'
});
```

### 模式处理器

模式处理器接收额外的匹配信息：

```typescript
bus.onMatch(/^user:(?<action>\w+)$/, (event, payload, match, ctx) => {
  console.log(match.action); // 命名捕获组的值
});
```

## License

[MIT](./LICENSE)
