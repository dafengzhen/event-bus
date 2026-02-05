import type { EventBus } from './event-bus.ts';
import type {
  EmitOptions,
  EventMap,
  Listener,
  Middleware,
  OnOptions,
  UseOptions,
} from './types.ts';

let SCOPE_ID = 0;

/**
 * A lifecycle-bound scope for an {@link EventBus}.
 *
 * `EventScope` provides a convenient way to register listeners/middlewares and ensure they are
 * automatically cleaned up when the scope is destroyed. Scopes can be nested; destroying a scope
 * will destroy all of its child scopes first.
 *
 * The scope is also attached to emitted events through `metaPatch.scope`, allowing downstream
 * logic/middlewares to inspect the originating scope.
 *
 * @typeParam E - Event map type (`eventName -> payload type`).
 *
 * @example
 * ```ts
 * const scope = bus.createScope();
 *
 * scope.on('tick', () => console.log('tick'));
 * scope.use((ctx, next) => next());
 *
 * scope.emit('tick');
 * scope.destroy(); // auto-unregisters listeners and middlewares registered via this scope
 * ```
 *
 * @example
 * ```ts
 * await scope.withChild(async (child) => {
 *   child.on('message', (p) => console.log(p));
 *   child.emit('message', { text: 'hi' });
 * }); // child is destroyed automatically
 * ```
 *
 * @author dafengzhen
 */
export class EventScope<E extends EventMap> {
  /**
   * Monotonically increasing identifier for this scope instance.
   * Useful for debugging/tracing.
   */
  readonly id = ++SCOPE_ID;

  /**
   * Child scopes created under this scope. Destroying this scope will destroy all children first.
   */
  private children = new Set<EventScope<E>>();

  /**
   * Whether this scope has been destroyed. A destroyed scope can no longer be used.
   */
  private destroyed = false;

  /**
   * Cleanup callbacks registered by this scope. Each function should undo a side effect
   * such as removing a listener, middleware, timer, etc.
   */
  private offs: Array<() => void> = [];

  /**
   * Creates a new scope bound to the given bus.
   *
   * @param bus - The owning event bus.
   * @param parent - Optional parent scope. If provided, this scope will be registered as a child
   * of the parent and will be destroyed when the parent is destroyed.
   */
  constructor(
    private bus: EventBus<E>,
    readonly parent?: EventScope<E>,
  ) {
    parent?.addChild(this);
  }

  /**
   * Destroys this scope.
   *
   * Destruction is idempotent. On first call it:
   * 1) Marks the scope destroyed
   * 2) Destroys all child scopes
   * 3) Flushes all registered cleanups
   * 4) Detaches from the parent (if any)
   */
  destroy(): void {
    if (this.destroyed) {
      return;
    }
    this.destroyed = true;

    for (const c of this.children) {
      c.destroy();
    }
    this.children.clear();

    this.flushOffs();
    this.parent?.removeChild(this);
  }

  /**
   * Emits an event from this scope.
   *
   * The emitted event will include `metaPatch.scope = this`, merged with any provided `metaPatch`.
   *
   * @typeParam K - Event key.
   * @param event - Event name.
   * @param payload - Event payload.
   * @param options - Emit options forwarded to the bus.
   * @throws Error if this scope is already destroyed.
   */
  emit<K extends keyof E>(event: K, payload?: E[K], options?: EmitOptions): void {
    this.assertAlive();
    this.bus.emit(event, payload, {
      ...options,
      metaPatch: {
        ...options?.metaPatch,
        scope: this,
      },
    });
  }

  /**
   * Emits an event asynchronously from this scope.
   *
   * The emitted event will include `metaPatch.scope = this`, merged with any provided `metaPatch`.
   *
   * @typeParam K - Event key.
   * @param event - Event name.
   * @param payload - Event payload.
   * @param options - Emit options forwarded to the bus.
   * @returns A promise that resolves when all async dispatch completes.
   * @throws Error if this scope is already destroyed.
   */
  emitAsync<K extends keyof E>(event: K, payload?: E[K], options?: EmitOptions): Promise<void> {
    this.assertAlive();
    return this.bus.emitAsync(event, payload, {
      ...options,
      metaPatch: {
        ...options?.metaPatch,
        scope: this,
      },
    });
  }

  /**
   * Executes and clears all registered cleanup callbacks.
   *
   * Errors thrown by cleanup callbacks are swallowed to ensure all cleanups are attempted.
   * If the scope is already destroyed, this is a no-op.
   */
  flushOffs(): void {
    if (this.destroyed) {
      return;
    }

    const arr = this.offs.splice(0);
    for (const off of arr) {
      try {
        off();
      } catch {
        // ignore
      }
    }
  }

  /**
   * Subscribes to an event and registers the returned cleanup function in this scope.
   *
   * When this scope is destroyed (or {@link flushOffs} is called), the subscription is removed.
   *
   * @typeParam K - Event key.
   * @param event - Event name.
   * @param listener - Event listener.
   * @param options - Subscription options forwarded to the bus.
   * @returns An `off()` function that removes the subscription.
   * @throws Error if this scope is already destroyed.
   */
  on<K extends keyof E>(event: K, listener: Listener<E[K]>, options?: OnOptions): () => void {
    this.assertAlive();
    const off = this.bus.on(event, listener, options);
    this.offs.push(off);
    return off;
  }

  /**
   * Subscribes to an event for a single invocation and registers the cleanup function in this scope.
   *
   * @typeParam K - Event key.
   * @param event - Event name.
   * @param listener - Event listener.
   * @param options - Subscription options forwarded to the bus.
   * @returns An `off()` function that cancels the one-time subscription.
   * @throws Error if this scope is already destroyed.
   */
  once<K extends keyof E>(event: K, listener: Listener<E[K]>, options?: OnOptions): () => void {
    this.assertAlive();
    const off = this.bus.once(event, listener, options);
    this.offs.push(off);
    return off;
  }

  /**
   * Registers an arbitrary cleanup callback to be executed when the scope is flushed/destroyed.
   *
   * This is useful when you create side effects outside of {@link on}, {@link once}, or {@link use},
   * but still want them cleaned up with the scope.
   *
   * @param off - Cleanup callback.
   * @throws Error if this scope is already destroyed.
   */
  registerOff(off: () => void): void {
    this.assertAlive();
    this.offs.push(off);
  }

  /**
   * Runs a synchronous function within this scope as the "current scope" in the bus runtime.
   *
   * Downstream code that relies on the runtime's current-scope tracking can access this scope
   * while `fn` executes.
   *
   * @typeParam T - Return type of the function.
   * @param fn - Function to execute.
   * @returns The function result.
   * @throws Error if this scope is already destroyed.
   */
  run<T>(fn: () => T): T {
    this.assertAlive();
    return this.bus.runtime.runWithScope(this, fn);
  }

  /**
   * Runs an asynchronous function within this scope as the "current scope" in the bus runtime.
   *
   * @typeParam T - Resolved return type.
   * @param fn - Async function to execute.
   * @returns A promise resolving to the function result.
   * @throws Error if this scope is already destroyed.
   */
  async runAsync<T>(fn: () => Promise<T>): Promise<T> {
    this.assertAlive();
    return this.bus.runtime.runWithScopeAsync(this, fn);
  }

  /**
   * Installs a middleware and registers the returned cleanup function in this scope.
   *
   * @param mw - Middleware to install.
   * @param options - Middleware installation options forwarded to the bus.
   * @returns An `off()` function that uninstalls the middleware.
   * @throws Error if this scope is already destroyed.
   */
  use(mw: Middleware<E>, options?: UseOptions<E>): () => void {
    this.assertAlive();
    const off = this.bus.use(mw, options);
    this.offs.push(off);
    return off;
  }

  /**
   * Creates a child scope, runs the callback with it, and always destroys the child afterwards.
   *
   * This is a structured-concurrency style helper: any resources registered in the child scope
   * are guaranteed to be cleaned up when the callback finishes (even if it throws).
   *
   * @typeParam T - Return type of the callback.
   * @param fn - Callback invoked with the newly created child scope.
   * @returns The callback result.
   * @throws Error if this scope is already destroyed.
   */
  async withChild<T>(fn: (scope: EventScope<E>) => Promise<T> | T): Promise<T> {
    this.assertAlive();
    const child = this.bus.createScope(this);

    try {
      return await fn(child);
    } finally {
      child.destroy();
    }
  }

  /**
   * Adds a child scope to this scope's child set.
   *
   * @param child - Child scope to add.
   * @internal
   */
  private addChild(child: EventScope<E>) {
    this.children.add(child);
  }

  /**
   * Throws if the scope has already been destroyed.
   *
   * @throws Error if destroyed.
   */
  private assertAlive() {
    if (this.destroyed) {
      throw new Error('EventScope already destroyed.');
    }
  }

  /**
   * Removes a child scope from this scope's child set.
   *
   * @param child - Child scope to remove.
   * @internal
   */
  private removeChild(child: EventScope<E>) {
    this.children.delete(child);
  }
}
