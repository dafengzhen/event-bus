import type { EventBus } from './event-bus.ts';
import type {
  EmitOptions,
  EventMap,
  Listener,
  MiddlewareAsync,
  MiddlewareSync,
  OnOptions,
  PatternHandler,
  UseOptions,
} from './types.ts';

/**
 * Auto-incrementing counter used to assign a unique identifier to each scope instance.
 */
let SCOPE_ID = 0;

/**
 * A function that, when called, removes a previously registered listener or middleware.
 */
type Off = () => void;

/**
 * A scoped context for managing the lifecycle of event listeners and middleware.
 *
 * Listeners and middleware registered through a scope are automatically tracked.
 * When the scope is destroyed, all tracked registrations are removed from the
 * underlying {@link EventBus}. Scopes can be nested, forming a tree where destroying
 * a parent scope also destroys all its children.
 *
 * Scopes are typically obtained via {@link EventBus.createScope} or
 * {@link EventBus.withScope}.
 *
 * @typeParam E - The event map type.
 *
 * @example
 * ```ts
 * const scope = bus.createScope();
 *
 * scope.on('user:login', (payload) => {
 *   console.log('Scoped listener:', payload.userId);
 * });
 *
 * // All scope listeners are removed.
 * scope.destroy();
 * ```
 *
 * @author dafengzhen
 */
export class EventScope<E extends EventMap> {
  /**
   * Unique identifier for this scope, useful for debugging and logging.
   */
  readonly id = ++SCOPE_ID;

  /**
   * Whether this scope has been destroyed. A destroyed scope cannot register
   * new listeners or emit events.
   */
  get isDestroyed(): boolean {
    return this.destroyed;
  }

  /**
   * Set of child scopes created from this scope.
   */
  private readonly children = new Set<EventScope<E>>();

  /**
   * Internal flag indicating whether this scope has been destroyed.
   */
  private destroyed = false;

  /**
   * Set of unregistration functions for all listeners and middleware
   * that were registered through this scope.
   */
  private readonly offs = new Set<Off>();

  /**
   * Creates a new scope bound to the given event bus.
   *
   * @param bus - The event bus this scope belongs to.
   * @param parent - An optional parent scope. If provided, this scope becomes a child
   *   of the parent and will be destroyed when the parent is destroyed.
   */
  constructor(
    private readonly bus: EventBus<E>,
    readonly parent?: EventScope<E>,
  ) {
    parent?.assertAlive();
    parent?.addChild(this);
  }

  /**
   * Destroys this scope and all its descendant scopes.
   *
   * All listeners and middleware registered through this scope (or any child scope)
   * are removed from the event bus. After destruction, the scope is no longer usable.
   */
  destroy(): void {
    if (this.destroyed) {
      return;
    }

    this.destroyed = true;

    // Take a snapshot and clear before destroying children to avoid mutation during iteration.
    const children = [...this.children];
    this.children.clear();

    for (const child of children) {
      child.destroy();
    }

    this.flushOffs();
    this.parent?.removeChild(this);
  }

  /**
   * Emits an event through the underlying event bus, automatically attaching
   * this scope to the event's metadata so receivers can identify the source scope.
   *
   * @param event - The event key to emit.
   * @param payload - The event payload.
   * @param options - Optional emission options.
   */
  emit<K extends keyof E>(event: K, payload: E[K], options?: EmitOptions): void;
  /**
   * Emits an event with only options (no payload).
   *
   * @param event - The event key to emit.
   * @param options - Emission options.
   */
  emit<K extends keyof E>(event: K, options: EmitOptions): void;
  emit<K extends keyof E>(
    event: K,
    payloadOrOptions?: E[K] | EmitOptions,
    options?: EmitOptions,
  ): void {
    this.assertAlive();

    const { emitOptions, optionsOnly, payload } = this.parseEmitArgs<E[K]>(
      payloadOrOptions,
      options,
    );
    const scopedOptions = this.withScopeMeta(emitOptions);

    if (optionsOnly) {
      this.bus.emit(event, scopedOptions);
      return;
    }

    this.bus.emit(event, payload as E[K], scopedOptions);
  }

  /**
   * Asynchronously emits an event through the underlying event bus, attaching
   * this scope's metadata.
   *
   * @param event - The event key to emit.
   * @param payload - The event payload.
   * @param options - Optional emission options.
   * @returns A Promise that resolves when all middleware and listeners have completed.
   */
  emitAsync<K extends keyof E>(event: K, payload: E[K], options?: EmitOptions): Promise<void>;
  /**
   * Asynchronously emits an event with only options (no payload).
   *
   * @param event - The event key to emit.
   * @param options - Emission options.
   * @returns A Promise that resolves when all middleware and listeners have completed.
   */
  emitAsync<K extends keyof E>(event: K, options: EmitOptions): Promise<void>;
  emitAsync<K extends keyof E>(
    event: K,
    payloadOrOptions?: E[K] | EmitOptions,
    options?: EmitOptions,
  ): Promise<void> {
    this.assertAlive();

    const { emitOptions, optionsOnly, payload } = this.parseEmitArgs<E[K]>(
      payloadOrOptions,
      options,
    );
    const scopedOptions = this.withScopeMeta(emitOptions);

    if (optionsOnly) {
      return this.bus.emitAsync(event, scopedOptions);
    }

    return this.bus.emitAsync(event, payload as E[K], scopedOptions);
  }

  /**
   * Immediately invokes all tracked unregistration functions, removing all scope-managed
   * listeners and middleware from the event bus.
   *
   * This is called automatically during {@link destroy}. Invoking it manually allows
   * clearing registrations without destroying the scope itself.
   *
   * Errors thrown by individual `off` functions are silently caught to ensure
   * best-effort cleanup of all remaining registrations.
   */
  flushOffs(): void {
    const pending = [...this.offs];
    this.offs.clear();

    for (const off of pending) {
      try {
        off();
      } catch {
        // Keep cleanup best-effort and preserve the original behavior.
      }
    }
  }

  /**
   * Registers an exact listener through the event bus and tracks it for automatic
   * cleanup when this scope is destroyed.
   *
   * @param event - The event key to listen for.
   * @param listener - The callback to invoke.
   * @param options - Registration options.
   * @returns A function that removes the listener (and untracks it from this scope).
   */
  on<K extends keyof E>(event: K, listener: Listener<E[K]>, options?: OnOptions): () => void {
    this.assertAlive();
    return this.trackOff(this.bus.on(event, listener, options));
  }

  /**
   * Registers a one-shot exact listener and tracks it for scope cleanup.
   *
   * @param event - The event key.
   * @param listener - The callback (invoked at most once).
   * @param options - Registration options.
   * @returns A function that removes the listener before it fires.
   */
  once<K extends keyof E>(event: K, listener: Listener<E[K]>, options?: OnOptions): () => void {
    this.assertAlive();
    return this.trackOff(this.bus.once(event, listener, options));
  }

  /**
   * Registers a one-shot pattern listener and tracks it for scope cleanup.
   *
   * @param pattern - The pattern to match.
   * @param handler - The handler (invoked at most once).
   * @param options - Registration options.
   * @returns A function that removes the listener.
   */
  onceMatch(pattern: RegExp | string, handler: PatternHandler<E>, options?: OnOptions): () => void {
    this.assertAlive();
    return this.trackOff(this.bus.onceMatch(pattern, handler, options));
  }

  /**
   * Registers a pattern listener and tracks it for scope cleanup.
   *
   * @param pattern - The pattern to match.
   * @param handler - The handler to invoke on match.
   * @param options - Registration options.
   * @returns A function that removes the listener.
   */
  onMatch(pattern: RegExp | string, handler: PatternHandler<E>, options?: OnOptions): () => void {
    this.assertAlive();
    return this.trackOff(this.bus.onMatch(pattern, handler, options));
  }

  /**
   * Registers an external unregistration function to be tracked by this scope.
   * When the scope is destroyed (or `flushOffs` is called), `off` will be invoked.
   *
   * This is used internally by the event bus when listeners are registered inside
   * an active scope via `runWithScope`.
   *
   * @param off - The function to call to unregister the resource.
   */
  registerOff(off: () => void): void {
    this.assertAlive();
    this.offs.add(off);
  }

  /**
   * Executes a synchronous function within the context of this scope.
   * Any listeners registered inside `fn` will be automatically bound to this scope.
   *
   * @typeParam T - The return type of `fn`.
   * @param fn - The function to execute.
   * @returns The return value of `fn`.
   */
  run<T>(fn: () => T): T {
    this.assertAlive();
    return this.bus.runtime.runWithScope(this, fn);
  }

  /**
   * Executes an asynchronous function within the context of this scope.
   * Any listeners registered inside `fn` will be automatically bound to this scope.
   *
   * @typeParam T - The resolution type.
   * @param fn - The async function to execute.
   * @returns A Promise resolving to the return value of `fn`.
   */
  runAsync<T>(fn: () => Promise<T>): Promise<T> {
    this.assertAlive();
    return this.bus.runtime.runWithScopeAsync(this, fn);
  }

  /**
   * Registers synchronous middleware through the event bus and tracks it for scope cleanup.
   *
   * @param mw - The middleware function.
   * @param options - Options to filter events.
   * @returns A function that removes the middleware.
   */
  use(mw: MiddlewareSync<E>, options?: UseOptions<E>): () => void {
    this.assertAlive();
    return this.trackOff(this.bus.use(mw, options));
  }

  /**
   * Registers asynchronous middleware through the event bus and tracks it for scope cleanup.
   *
   * @param mw - The async middleware function.
   * @param options - Options to filter events.
   * @returns A function that removes the middleware.
   */
  useAsync(mw: MiddlewareAsync<E>, options?: UseOptions<E>): () => void {
    this.assertAlive();
    return this.trackOff(this.bus.useAsync(mw, options));
  }

  /**
   * Creates a temporary child scope, executes the provided function within its context,
   * and automatically destroys the child scope when done (even on error).
   *
   * @typeParam T - The return type of `fn`.
   * @param fn - The function to execute with the child scope.
   * @returns A Promise resolving to the return value of `fn`.
   *
   * @example
   * ```ts
   * await parentScope.withChild(async (childScope) => {
   *   childScope.on('user:login', handler);
   *   // ... do work ...
   *   // Child scope is automatically destroyed when this callback completes.
   * });
   * ```
   */
  async withChild<T>(fn: (scope: EventScope<E>) => Promise<T> | T): Promise<T> {
    this.assertAlive();
    const child = this.bus.createScope(this);

    try {
      return await child.run(() => fn(child));
    } finally {
      child.destroy();
    }
  }

  /**
   * Adds a child scope to the tracked set. The child will be destroyed when this scope
   * is destroyed.
   *
   * @param child - The child scope to track.
   */
  private addChild(child: EventScope<E>): void {
    this.assertAlive();
    this.children.add(child);
  }

  /**
   * Throws an error if this scope has been destroyed, preventing further use.
   */
  private assertAlive(): void {
    if (this.destroyed) {
      throw new Error('EventScope already destroyed.');
    }
  }

  /**
   * Parses the overloaded arguments of `emit`/`emitAsync` into a normalized structure
   * indicating whether the call was options-only and what the payload and options are.
   *
   * @typeParam P - The payload type.
   * @param payloadOrOptions - Either the payload or the options object.
   * @param options - The options object, if the first argument was the payload.
   * @returns An object with `payload`, `emitOptions`, and an `optionsOnly` flag.
   */
  private parseEmitArgs<P>(
    payloadOrOptions?: EmitOptions | P,
    options?: EmitOptions,
  ): {
    emitOptions: EmitOptions | undefined;
    optionsOnly: boolean;
    payload: P | undefined;
  } {
    if (options !== undefined) {
      return {
        emitOptions: options,
        optionsOnly: false,
        payload: payloadOrOptions as P | undefined,
      };
    }

    if (looksLikeEmitOptions(payloadOrOptions)) {
      return {
        emitOptions: payloadOrOptions,
        optionsOnly: true,
        payload: undefined,
      };
    }

    return {
      emitOptions: undefined,
      optionsOnly: false,
      payload: payloadOrOptions as P | undefined,
    };
  }

  /**
   * Removes a child scope from the tracked set. Called when the child is destroyed.
   *
   * @param child - The child scope to remove.
   */
  private removeChild(child: EventScope<E>): void {
    this.children.delete(child);
  }

  /**
   * Wraps a raw unregistration function so that it is both tracked by this scope
   * and idempotent (multiple calls only invoke the underlying `off` once).
   *
   * @param off - The raw unregistration function from the event bus.
   * @returns An idempotent unregistration function that also untracks itself from the scope.
   */
  private trackOff(off: Off): Off {
    this.offs.add(off);

    let done = false;
    return () => {
      if (done) {
        return;
      }

      done = true;
      this.offs.delete(off);
      off();
    };
  }

  /**
   * Merges this scope instance into the `metaPatch` of the emit options so that
   * receivers can identify which scope emitted the event.
   *
   * @param options - The original emit options (may be undefined).
   * @returns New emit options with the scope injected into `metaPatch`.
   */
  private withScopeMeta(options?: EmitOptions): EmitOptions {
    return {
      ...options,
      metaPatch: {
        ...options?.metaPatch,
        scope: this,
      },
    };
  }
}

/**
 * Safe wrapper around `Object.prototype.hasOwnProperty.call`.
 *
 * @param value - The object to check.
 * @param key - The property key.
 * @returns `true` if the object has the own property.
 */
function hasOwn(value: object, key: PropertyKey): boolean {
  return Object.prototype.hasOwnProperty.call(value, key);
}

/**
 * Heuristic to determine if a value looks like an {@link EmitOptions} object
 * by checking for the presence of known emit option keys.
 *
 * @param value - The value to test.
 * @returns `true` if the value likely represents emit options.
 */
function looksLikeEmitOptions(value: unknown): value is EmitOptions {
  if (!value || typeof value !== 'object') {
    return false;
  }

  return hasOwn(value, 'sticky') || hasOwn(value, 'stickyMode') || hasOwn(value, 'metaPatch');
}
