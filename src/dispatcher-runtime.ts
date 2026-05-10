import type { EventScope } from './event-scope.ts';
import type { EventMap, MaybePromise, ScopeFrame, ScopeStorage } from './types.ts';

/**
 * Manages the execution context stack for EventScope instances.
 *
 * The DispatcherRuntime is responsible for tracking which scope is currently
 * active so that listener registrations can be automatically bound to the
 * correct scope for cleanup. It supports two modes:
 *
 * - **Stack-based**: Uses an internal array-based stack (default).
 * - **Storage-based**: Delegates scope storage to an external `ScopeStorage`
 *   implementation (e.g., `AsyncLocalStorage` for async context tracking).
 *
 * @typeParam E - The event map type.
 *
 * @example
 * ```typescript
 * // Stack-based (default)
 * const runtime = new DispatcherRuntime<MyEvents>();
 *
 * // AsyncLocalStorage-based (Node.js)
 * import { AsyncLocalStorage } from 'node:async_hooks';
 * const als = new AsyncLocalStorage<ScopeFrame<MyEvents>>();
 * const runtime = new DispatcherRuntime<MyEvents>({
 *   getStore: () => als.getStore(),
 *   run: (frame, fn) => als.run(frame, fn),
 * });
 * ```
 *
 * @author dafengzhen
 */
export class DispatcherRuntime<E extends EventMap> {
  /**
   * Internal stack of scope frames used when no external storage is provided.
   * The last element represents the current active scope.
   */
  private readonly scopeStack: Array<ScopeFrame<E>> = [];

  /**
   * Creates a new DispatcherRuntime.
   *
   * @param storage - Optional external scope storage for async context propagation.
   */
  constructor(private readonly storage?: ScopeStorage<E> | undefined) {}

  /**
   * Returns the currently active EventScope, or `undefined` if none is active.
   *
   * If an external storage is configured, it is queried first. Otherwise,
   * the top of the internal scope stack is returned.
   *
   * @returns The current scope, or `undefined`.
   */
  getScope(): EventScope<E> | undefined {
    return this.storage?.getStore()?.scope ?? this.scopeStack[this.scopeStack.length - 1]?.scope;
  }

  /**
   * Executes a function within the given scope, returning a Promise.
   *
   * @typeParam T - The return type of the function.
   * @param scope - The scope to make active during execution.
   * @param fn - The function to execute.
   * @returns A promise resolving to the function's return value.
   * @throws If the scope has been disposed.
   */
  runWithScope<T>(scope: EventScope<E>, fn: () => Promise<T>): Promise<T>;
  /**
   * Executes a function within the given scope, returning the result directly.
   *
   * @typeParam T - The return type of the function.
   * @param scope - The scope to make active during execution.
   * @param fn - The synchronous function to execute.
   * @returns The function's return value.
   * @throws If the scope has been disposed.
   */
  runWithScope<T>(scope: EventScope<E>, fn: () => T): T;
  runWithScope<T>(scope: EventScope<E>, fn: () => MaybePromise<T>): MaybePromise<T> {
    this.assertScopeAlive(scope);

    const frame: ScopeFrame<E> = { scope };

    if (this.storage) {
      return this.storage.run(frame, fn);
    }

    this.scopeStack.push(frame);

    let result: MaybePromise<T>;
    try {
      result = fn();
    } catch (error) {
      this.removeScopeFrame(frame);
      throw error;
    }

    if (isPromiseLike<T>(result)) {
      return Promise.resolve(result).finally(() => {
        this.removeScopeFrame(frame);
      });
    }

    this.removeScopeFrame(frame);
    return result;
  }

  /**
   * Executes an async function within the given scope, always returning a Promise.
   *
   * This is a convenience wrapper around `runWithScope` that ensures the return
   * value is always a Promise regardless of whether the function is sync or async.
   *
   * @typeParam T - The return type of the function.
   * @param scope - The scope to make active during execution.
   * @param fn - The function to execute (sync or async).
   * @returns A promise resolving to the function's return value.
   */
  runWithScopeAsync<T>(scope: EventScope<E>, fn: () => MaybePromise<T>): Promise<T> {
    return Promise.resolve(this.runWithScope(scope, fn));
  }

  /**
   * Asserts that the given scope has not been disposed.
   *
   * @param scope - The scope to check.
   * @throws If the scope has been disposed.
   */
  private assertScopeAlive(scope: EventScope<E>): void {
    if (scope.isDisposed) {
      throw new Error('EventScope has been disposed.');
    }
  }

  /**
   * Removes a scope frame from the internal stack.
   *
   * @param frame - The scope frame to remove.
   */
  private removeScopeFrame(frame: ScopeFrame<E>): void {
    const index = this.scopeStack.lastIndexOf(frame);
    if (index !== -1) {
      this.scopeStack.splice(index, 1);
    }
  }
}

/**
 * Checks whether a value is Promise-like (has a `.then` method).
 *
 * @typeParam T - The resolved type of the potential promise.
 * @param value - The value to check.
 * @returns `true` if the value is Promise-like, `false` otherwise.
 */
function isPromiseLike<T = unknown>(value: unknown): value is PromiseLike<T> {
  return (
    value !== null &&
    (typeof value === 'object' || typeof value === 'function') &&
    typeof (value as { then?: unknown }).then === 'function'
  );
}
