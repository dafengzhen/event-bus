# event-bus

[English](./README.md)

强类型 TypeScript 事件总线，提供显式作用域、模式监听、中间件和事件保留。

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
bus.emit('ready'); // void / undefined 负载可以省略

off(); // 多次调用安全
```

也可以使用 `new EventBus<Events>(options)`。事件映射支持接口和类型别名，无需索引签名。精确事件键支持字符串、数字和 symbol。每个事件携带一个负载，需要多个值时使用对象或元组。未知事件、缺少必需负载、事件键与负载类型不匹配都会产生类型错误。

## 订阅

```ts
bus.on('user:login', (payload, event) => {
  console.log(payload.id, event.id, event.meta);
});
bus.once('ready', () => console.log('只执行一次'));

bus.onPattern('user:.*', (event) => {
  if (event.type === 'user:login') {
    console.log(event.payload.id); // 自动收窄为 { id: string }
  }
});
bus.onAny((event) => console.log(event.type, event.payload));
```

`on`、`once` 接收 `(payload, event)`；`onAny`、`onPattern` 接收事件对象。事件对象是可辨识联合类型，检查 `event.type` 时，`event.payload` 会一起收窄。模式字符串本身不会在编译时收窄联合类型，需要在回调内检查事件名。

所有订阅方法都返回取消函数。同一个函数注册两次会创建两个独立订阅。

| 订阅选项   | 默认值  | 行为                                                                 |
| ---------- | ------- | -------------------------------------------------------------------- |
| `priority` | `0`     | 越大越先执行；相同优先级按注册顺序执行，精确、模式、全局监听统一排序 |
| `once`     | `false` | 在首次调用前移除，重放、嵌套分发也只执行一次                         |
| `signal`   | —       | 中止时移除；已经中止的信号不会创建有效订阅                           |
| `replay`   | `false` | 注册时同步重放匹配的已保留事件                                       |

`once()` 固定使用 `once: true`。优先级必须是有限数字。

### 模式匹配

`onPattern(pattern, observer, options?)` 和中间件的 `pattern` 选项接受：

- **derivative-matcher DSL 字符串**：`user:.*`、`(user|team):.*`、`user:.* & !(user:logout)`。
- 原生 `RegExp`：例如 `/^user:/`，支持 JavaScript 正则标志和 Unicode。
- 实现 `test(input: string): boolean` 的可复用匹配器，例如 `@dafengzhen/derivative-matcher` 的 `createPatternMatcher('user:.*')` 返回值。

DSL 匹配**整个事件名**：`user:` 不匹配 `user:login`，应写成 `user:.*`。它不是 glob 通配符语法。DSL 字符范围为 Latin-1，忽略未转义的空白，不提供捕获组。原生正则使用 JavaScript 的搜索语义；每次注册独立管理正则游标，`g` / `y` 和调用方的 `lastIndex` 修改不会干扰分发。模式仅匹配字符串键，数字和 symbol 可通过 `onAny` 观察。非法 DSL 在注册时抛错；自定义匹配器应保持纯函数行为且不抛异常。

## 分发

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

`emit()` 同步执行；`emitAsync()` 按相同优先级顺序逐个执行并等待所有处理器。多个独立的 `emitAsync()` 调用可以并发，不使用全局队列。一次性订阅在调用前移除，因此并发分发也不会重复执行它。

异步处理器应使用 `emitAsync()`。`emit()` 在执行处理器前检查匹配的 `useAsync()` 中间件并直接拒绝同步分发。如果监听器或 `use()` 中间件在同步分发时返回 Promise，则按分发错误策略报告 `TypeError`，并接住该 Promise 的拒绝；已经启动的异步操作无法撤销。重放也是同步的；需要异步处理已保留值时，可以先 `await bus.waitFor(type, { replay: true })`，再继续异步工作。

分发开始时为监听器和中间件创建快照。分发中新注册的处理器从下一次事件开始参与；轮到执行前已移除或取消的处理器会跳过。`clear()`、`dispose()` 会停止进行中的分发后续步骤，包括尚在等待中间件的分发；已经运行的回调会正常结束。

### 事件上下文

| 字段                 | 含义                         |
| -------------------- | ---------------------------- |
| `type`、`payload`    | 保持类型关联的事件键与负载   |
| `id`                 | 当前总线内递增的分发 ID      |
| `timestamp`          | 发出事件时的 Unix 毫秒时间戳 |
| `meta`               | 发出时元数据的浅冻结副本     |
| `replayed`           | 当前调用是否为重放           |
| `propagationStopped` | 可实时读取的传播状态         |
| `stopPropagation()`  | 跳过后续中间件和监听器       |

事件对象和分发结果被冻结。负载及嵌套元数据保留引用，不复制、不深冻结。重放保留原始 ID、时间戳、负载和元数据，但拥有独立传播状态。停止传播不会撤销已执行回调和副作用。

## 中间件

```ts
const off = bus.use(
  (event, next) => {
    if (event.meta.blocked) return; // 不调用 next() 即阻止后续分发
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

中间件按注册顺序在监听器之前执行。每个中间件最多调用一次 `next()`，且必须在自身完成前调用。不调用 `next()` 会阻止后续执行与事件保留。调用 `stopPropagation()` 后，即使再调用 `next()`，也不会执行下游。中间件接受 `{ pattern?, signal? }`，注册后返回取消函数。

`use()` 是同步中间件。在 `emitAsync()` 中，其 `next()` 启动下游但无法同步等待异步工作；需要包围异步分发执行前后逻辑时，使用 `useAsync()` 和 `await next()`。即使忘记等待 `next()`，分发也会等待已经启动的下游完成；中间件自身仍应正确等待，以保证前后逻辑的顺序。

## 等待事件

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

`waitFor` 返回事件上下文，结束后自动清理监听器、中止钩子和计时器。`filter` 默认接收所有负载，`replay` 默认为 `false`，不传 `timeout` 则不限时。超时必须是 `0` 至 `2147483647` 之间的有限毫秒数。超时以 `TimeoutError` 拒绝，中止以信号的 `reason` 拒绝。过滤函数抛错只拒绝等待，不使事件分发失败。`clear()`、总线销毁、所属作用域销毁会以 `AbortError` 拒绝尚未完成的等待。调用方需要处理 Promise 拒绝。

## 保留与重放

```ts
bus.emit('ready', undefined, { retain: true });
bus.once('ready', (_, event) => console.log(event.replayed), { replay: true });

bus.clearRetained('ready');
bus.clearRetained(); // 清除所有键
```

每个键只保留最新的一条事件，更新时替换旧值。事件经过中间件、到达监听器阶段时保留，在监听器调用之前写入；监听器随后失败也不回滚保留值。被中间件阻止的事件不会保留。未设置 `retain: true` 的普通分发不会覆盖或清除旧保留值。

迟到的订阅只有显式开启 `replay` 才会重放。重放只调用新订阅，不经过中间件、不触发其他监听器。多个匹配键按最后保留更新时间从旧到新重放。`once` 重放第一条匹配值后立即取消。如果重放出现未处理错误，注册方法抛错前会移除新订阅。

`createEventBus({ retainedLimit: 100 })` 限制保留的不同键数量，默认 `100`。更新已有键会刷新顺序，超过上限时淘汰最久未更新的键。`0` 禁用保留；上限必须是非负安全整数。销毁总线不会清理 derivative-matcher 的共享编译缓存。

## 作用域与清理

```ts
const scope = bus.scope();
const child = scope.scope();

scope.on('user:login', ({ id }) => console.log(id));
child.onAny((event) => console.log(event.type));
const pending = child.waitFor('ready');

scope.dispose(); // 同时销毁 child 并拒绝 pending
await pending.catch(() => {});
```

作用域提供 `on`、`once`、`onAny`、`onPattern`、`use`、`useAsync`、`waitFor`、`scope`、`dispose`、`signal`、`disposed`。通过总线发出事件。所有权显式指定，跨 `await` 不会串作用域；无需隐式上下文或全局运行时。传入的订阅信号与作用域信号共同控制取消。销毁子作用域不影响父作用域和兄弟作用域；销毁总线会销毁全部作用域。销毁可重复调用。通过已销毁实例注册会抛错；`waitFor` 返回拒绝的 Promise。

| 总线成员               | 行为                                                       |
| ---------------------- | ---------------------------------------------------------- |
| `listenerCount()`      | 有效监听注册总数，不含中间件                               |
| `listenerCount(type)`  | 匹配指定键的精确、模式、全局监听总数                       |
| `clearRetained(type?)` | 仅清除保留数据                                             |
| `clear()`              | 清除监听、中间件和保留数据，拒绝等待；总线和作用域仍可复用 |
| `dispose()`            | 完整清理并永久关闭总线和所有作用域                         |
| `disposed`             | 是否永久关闭                                               |

## 错误处理

默认收集监听错误并继续执行其他监听器，最后抛出 `AggregateError`；`emitAsync()` 对应拒绝 Promise。中间件失败会阻止尚未启动的下游；通过 `next()` 已经启动的下游可以完成。配置错误与模式编译错误直接抛出。

```ts
const bus = createEventBus<Events>({
  onError: ({ error, event, phase }) => {
    console.error(event.type, phase, error);
  },
});
```

`onError` 在分发结束后逐条同步处理失败，提供该回调即表示接管错误。返回结果仍包含 `errors: readonly DispatchFailure[]`，每条具有 `error`、`event` 和 `phase`（`listener`、`middleware`、`replay`）。`delivered` 统计调用次数，包含失败调用。错误回调自身抛错会被聚合并向调用方传播，不递归上报。不要传入异步错误回调。

## 开发

开发需要 Node.js `^20.19.0 || >=22.12.0`，使用类型声明需要 TypeScript 5.4+。浏览器运行时需要 `AbortController`、`AbortSignal.any`（组合调用方和作用域信号时）、`DOMException`、`AggregateError`；构建不会注入运行时 polyfill。

```bash
npm ci
npm run check         # 类型、lint、格式、测试、全部构建与包入口检查
npm run coverage      # 覆盖率与门槛检查
npm run dev           # 交互示例
npm run build:example
```

提供 ESM / CommonJS 及各自类型声明，以及现代 / legacy 语法构建。`/legacy` 仅改变输出语法目标。`scripts/build.mjs` 构建库，`vite.config.ts` 用于示例开发和构建。

## 许可

[MIT](./LICENSE)
