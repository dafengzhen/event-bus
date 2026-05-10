import type { EventBus } from './event-bus.ts';
import type {
  EmitOptions,
  EventMap,
  Listener,
  MaybePromise,
  MiddlewareAsync,
  MiddlewareSync,
  Off,
  OnOptions,
  PatternHandler,
  UseOptions,
} from './types.ts';

/**
 * Global counter for generating unique scope IDs.
 */
let SCOPE_ID = 0;

/**
 * A lifecycle-scoped container for event bus registrations.
 *
 * EventScope provides automatic cleanup of listeners, middleware, and pattern
 * handlers registered within its context. When a scope is disposed:
 *
 * - All registered `Off` functions are called.
 * - All child scopes are recursively disposed.
 * - The scope is detached from its parent.
 *
 * Scopes can be nested, forming a parent-child tree that mirrors application
 * lifecycle (e.g., request-scoped, component-scoped, session-scoped).
 *
 * @typeParam E - The event map type.
 *
 * @example
 * ```typescript
 * const bus = new EventBus<MyEvents>();
 * const scope = bus.createScope();
 *
 * // All registrations are bound to this scope
 * scope.on('user:login', (payload) => { ... });
 * scope.use((ctx, next) => { ... next(); });
 *
 * // Later, dispose to clean up all scope-bound listeners
 * scope.dispose();
 * ```
 *
 * @author dafengzhen
 */
export class EventScope<E extends EventMap> {
  /**
   * Unique identifier for this scope, used for debugging.
   */
  readonly id = ++SCOPE_ID;

  /**
   * Whether this scope has been disposed. Once disposed, the scope
   * cannot be used for new registrations or emissions.
   */
  get isDisposed(): boolean {
    return this.disposed;
  }

  /**
   * Set of child scopes created from this scope.
   */
  private readonly children = new Set<EventScope<E>>();

  /**
   * Internal disposed flag.
   */
  private disposed = false;

  /**
   * Map of tracked `Off` functions, keyed by the original `Off` function.
   * Each entry maps to a wrapped version that ensures single execution.
   */
  private readonly offs = new Map<Off, Off>();

  /**
   * Creates a new EventScope.
   *
   * @param bus - The parent EventBus instance.
   * @param parent - Optional parent scope. If provided, this scope is added as a child.
   * @throws If the parent scope has been disposed.
   */
  constructor(
    private readonly bus: EventBus<E>,
    readonly parent?: EventScope<E>,
  ) {
    parent?.assertAlive();
    parent?.addChild(this);
  }

  /**
   * Disposes this scope and all its child scopes.
   *
   * All registered `Off` functions are invoked, child scopes are recursively
   * disposed, and this scope is removed from its parent. Multiple calls are safe;
   * subsequent calls after the first are no-ops.
   */
  dispose(): void {
    if (this.disposed) {
      return;
    }

    this.disposed = true;

    const children = [...this.children];
    this.children.clear();

    for (const child of children) {
      child.dispose();
    }

    this.flushOffs();
    this.parent?.removeChild(this);
  }

  /**
   * Emits an event through the parent bus with this scope attached to the metadata.
   *
   * @typeParam K - The event key type.
   * @param event - The event key to emit.
   * @param payload - The event payload.
   * @param options - Optional emit configuration. Scope is injected into `metaPatch`.
   * @throws If the scope has been disposed.
   */
  emit<K extends keyof E>(event: K, payload: E[K], options?: EmitOptions): void {
    this.assertAlive();
    this.bus.emit(event, payload, this.withScopeMeta(options));
  }

  /**
   * Asynchronously emits an event through the parent bus with this scope attached.
   *
   * @typeParam K - The event key type.
   * @param event - The event key to emit.
   * @param payload - The event payload.
   * @param options - Optional emit configuration. Scope is injected into `metaPatch`.
   * @returns A promise that resolves when all middleware and listeners have completed.
   * @throws If the scope has been disposed.
   */
  emitAsync<K extends keyof E>(event: K, payload: E[K], options?: EmitOptions): Promise<void> {
    this.assertAlive();
    return this.bus.emitAsync(event, payload, this.withScopeMeta(options));
  }

  /**
   * Calls all tracked `Off` functions immediately, without disposing the scope.
   * This is useful for clearing scope-bound listeners without destroying the scope itself.
   */
  flushOffs(): void {
    if (this.offs.size === 0) {
      return;
    }

    const pending = [...this.offs.values()];
    this.offs.clear();

    for (const off of pending) {
      try {
        off();
      } catch {}
    }
  }

  /**
   * Registers a listener for the given event key, bound to this scope.
   *
   * @typeParam K - The event key type.
   * @param event - The event key to listen for.
   * @param listener - The listener function.
   * @param options - Optional registration configuration.
   * @returns An `Off` function to remove this listener.
   * @throws If the scope has been disposed.
   */
  on<K extends keyof E>(event: K, listener: Listener<E[K], E, K>, options?: OnOptions): Off {
    return this.trackRegistration(() => this.bus.on(event, listener, options));
  }

  /**
   * Registers a one-time listener for the given event key, bound to this scope.
   *
   * @typeParam K - The event key type.
   * @param event - The event key to listen for.
   * @param listener - The listener function (invoked once).
   * @param options - Optional registration configuration.
   * @returns An `Off` function to remove this listener before its first invocation.
   * @throws If the scope has been disposed.
   */
  once<K extends keyof E>(event: K, listener: Listener<E[K], E, K>, options?: OnOptions): Off {
    return this.trackRegistration(() => this.bus.once(event, listener, options));
  }

  /**
   * Registers a one-time pattern-matching listener, bound to this scope.
   *
   * @param pattern - A string pattern or RegExp to match event keys.
   * @param handler - The handler invoked once on first match.
   * @param options - Optional registration configuration.
   * @returns An `Off` function to remove this handler.
   * @throws If the scope has been disposed.
   */
  onceMatch(pattern: RegExp | string, handler: PatternHandler<E>, options?: OnOptions): Off {
    return this.trackRegistration(() => this.bus.onceMatch(pattern, handler, options));
  }

  /**
   * Registers a pattern-matching listener, bound to this scope.
   *
   * @param pattern - A string pattern or RegExp to match event keys.
   * @param handler - The handler invoked on each match.
   * @param options - Optional registration configuration.
   * @returns An `Off` function to remove this handler.
   * @throws If the scope has been disposed.
   */
  onMatch(pattern: RegExp | string, handler: PatternHandler<E>, options?: OnOptions): Off {
    return this.trackRegistration(() => this.bus.onMatch(pattern, handler, options));
  }

  /**
   * Registers an `Off` function with this scope for tracking.
   * The function will be called when the scope is disposed.
   *
   * @param off - The `Off` function to track.
   * @returns A wrapped `Off` function that also removes the tracking.
   * @throws If the scope has been disposed.
   */
  registerOff(off: Off): Off {
    this.assertAlive();
    return this.trackOff(off);
  }

  /**
   * Executes a synchronous function with this scope active on the bus runtime.
   *
   * @typeParam T - The return type.
   * @param fn - The function to execute.
   * @returns The function's return value.
   * @throws If the scope has been disposed.
   */
  run<T>(fn: () => T): T {
    this.assertAlive();
    return this.bus.runtime.runWithScope(this, fn);
  }

  /**
   * Executes an async function with this scope active on the bus runtime.
   *
   * @typeParam T - The return type.
   * @param fn - The function to execute (sync or async).
   * @returns A promise resolving to the function's return value.
   * @throws If the scope has been disposed.
   */
  runAsync<T>(fn: () => MaybePromise<T>): Promise<T> {
    this.assertAlive();
    return this.bus.runtime.runWithScopeAsync(this, fn);
  }

  /**
   * Registers a synchronous middleware, bound to this scope.
   *
   * @param mw - The synchronous middleware function.
   * @param options - Optional middleware configuration.
   * @returns An `Off` function to remove this middleware.
   * @throws If the scope has been disposed.
   */
  use(mw: MiddlewareSync<E>, options?: UseOptions<E>): Off {
    return this.trackRegistration(() => this.bus.use(mw, options));
  }

  /**
   * Registers an asynchronous middleware, bound to this scope.
   *
   * @param mw - The asynchronous middleware function.
   * @param options - Optional middleware configuration.
   * @returns An `Off` function to remove this middleware.
   * @throws If the scope has been disposed.
   */
  useAsync(mw: MiddlewareAsync<E>, options?: UseOptions<E>): Off {
    return this.trackRegistration(() => this.bus.useAsync(mw, options));
  }

  /**
   * Creates a child scope, executes a function within it, then disposes it.
   *
   * @typeParam T - The return type.
   * @param fn - The function to execute within the child scope.
   * @returns A promise resolving to the function's return value.
   * @throws If this scope has been disposed.
   */
  async withChild<T>(fn: (scope: EventScope<E>) => MaybePromise<T>): Promise<T> {
    this.assertAlive();
    const child = this.bus.createScope(this);

    try {
      return await child.runAsync(() => fn(child));
    } finally {
      child.dispose();
    }
  }

  /**
   * Adds a child scope to this scope's children set.
   *
   * @param child - The child scope to add.
   * @throws If this scope has been disposed.
   */
  private addChild(child: EventScope<E>): void {
    this.assertAlive();
    this.children.add(child);
  }

  /**
   * Asserts that this scope is alive (not disposed).
   *
   * @throws If the scope has been disposed.
   */
  private assertAlive(): void {
    if (this.disposed) {
      throw new Error('EventScope has been disposed.');
    }
  }

  /**
   * Removes a child scope from this scope's children set.
   *
   * @param child - The child scope to remove.
   */
  private removeChild(child: EventScope<E>): void {
    this.children.delete(child);
  }

  /**
   * Tracks an `Off` function, returning a wrapped version that ensures
   * at-most-once execution and removes itself from tracking on invocation.
   *
   * @param off - The original `Off` function.
   * @returns A wrapped `Off` function.
   */
  private trackOff(off: Off): Off {
    const tracked = this.offs.get(off);
    if (tracked) {
      return tracked;
    }

    let executed = false;
    const trackedOff: Off = () => {
      if (executed) {
        return;
      }

      executed = true;
      this.offs.delete(off);
      off();
    };

    this.offs.set(off, trackedOff);
    return trackedOff;
  }

  /**
   * Registers a listener or middleware through the parent bus, and tracks
   * the resulting `Off` function with this scope. If the scope was disposed
   * during registration (edge case), the `Off` function is called immediately.
   *
   * @param register - A callback that performs the bus registration and returns an `Off`.
   * @returns The `Off` function.
   * @throws If the scope has been disposed before registration.
   */
  private trackRegistration(register: () => Off): Off {
    this.assertAlive();

    const off = register();
    const autoBoundScope = this.bus.runtime.getScope();

    if (autoBoundScope && autoBoundScope !== this) {
      autoBoundScope.untrackOff(off);
    }

    if (this.disposed) {
      off();
      return off;
    }

    return this.trackOff(off);
  }

  /**
   * Removes an `Off` function from tracking (used when the bus's auto-binding
   * assigned the registration to a different scope).
   *
   * @param off - The `Off` function to untrack.
   */
  private untrackOff(off: Off): void {
    this.offs.delete(off);
  }

  /**
   * Merges this scope into the emit options metadata, ensuring the scope
   * is available to all listeners in the call chain.
   *
   * @param options - Original emit options (may be `undefined`).
   * @returns Emit options with scope injected into `metaPatch`.
   */
  private withScopeMeta(options?: EmitOptions): EmitOptions {
    if (!options) {
      return { metaPatch: { scope: this } };
    }

    return {
      ...options,
      metaPatch: {
        ...options.metaPatch,
        scope: this,
      },
    };
  }
}
