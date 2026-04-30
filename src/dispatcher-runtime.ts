import type { EventScope } from './event-scope.ts';
import type { EventMap } from './types.ts';

/**
 * Internal frame representing a scope pushed onto the runtime stack.
 * Each frame links to its {@link EventScope} instance.
 *
 * @typeParam E - The event map type.
 */
type ScopeFrame<E extends EventMap> = {
  readonly scope: EventScope<E>;
};

/**
 * Manages a stack of active {@link EventScope} instances, providing the mechanism
 * for scope-aware listener lifecycle management.
 *
 * When listeners are registered inside a `runWithScope` callback, they are automatically
 * bound to the active scope and cleaned up when that scope is destroyed.
 *
 * @typeParam E - The event map type.
 *
 * @example
 * ```ts
 * const runtime = new DispatcherRuntime<MyEvents>();
 * const scope = new EventScope(bus);
 *
 * runtime.runWithScope(scope, () => {
 *   // Any listeners registered here are bound to `scope`.
 *   bus.on('user:login', handler);
 * });
 *
 * scope.destroy(); // All listeners registered above are now removed.
 * ```
 *
 * @author dafengzhen
 */
export class DispatcherRuntime<E extends EventMap> {
  /**
   * Stack of active scope frames. The top of the stack represents the currently
   * active scope (if any).
   */
  private readonly scopeStack: Array<ScopeFrame<E>> = [];

  /**
   * Returns the currently active scope, or `undefined` if no scope is active.
   *
   * @returns The active {@link EventScope}, or `undefined`.
   */
  getScope(): EventScope<E> | undefined {
    return this.scopeStack[this.scopeStack.length - 1]?.scope;
  }

  /**
   * Executes a synchronous or asynchronous function within the context of the given scope.
   * The scope is pushed onto the stack before `fn` is called and popped afterwards,
   * even if `fn` throws or returns a rejected Promise.
   *
   * @typeParam T - The return type of `fn`.
   * @param scope - The scope to activate.
   * @param fn - The function to execute within the scope.
   * @returns The return value of `fn` (or a Promise resolving to it).
   */
  runWithScope<T>(scope: EventScope<E>, fn: () => Promise<T>): Promise<T>;
  runWithScope<T>(scope: EventScope<E>, fn: () => T): T;
  runWithScope<T>(scope: EventScope<E>, fn: () => Promise<T> | T): Promise<T> | T {
    const frame: ScopeFrame<E> = { scope };
    this.scopeStack.push(frame);

    let result: Promise<T> | T;
    try {
      result = fn();
    } catch (error) {
      this.removeFrame(frame);
      throw error;
    }

    if (isPromiseLike<T>(result)) {
      return Promise.resolve(result).finally(() => {
        this.removeFrame(frame);
      });
    }

    this.removeFrame(frame);
    return result;
  }

  /**
   * Convenience method to execute an async function within a scope.
   * Equivalent to `runWithScope(scope, fn)` but explicitly typed for async use cases.
   *
   * @typeParam T - The return type of `fn`.
   * @param scope - The scope to activate.
   * @param fn - The async function to execute.
   * @returns A Promise resolving to the return value of `fn`.
   */
  runWithScopeAsync<T>(scope: EventScope<E>, fn: () => Promise<T>): Promise<T> {
    return this.runWithScope(scope, fn);
  }

  /**
   * Removes a scope frame from the stack. Uses `lastIndexOf` to safely handle
   * cases where the frame may have already been removed (e.g., by a parent scope cleanup).
   *
   * @param frame - The frame to remove.
   */
  private removeFrame(frame: ScopeFrame<E>): void {
    const index = this.scopeStack.lastIndexOf(frame);
    if (index !== -1) {
      this.scopeStack.splice(index, 1);
    }
  }
}

/**
 * Checks whether a value is "thenable" (has a `.then` method), indicating it is
 * a Promise-like object.
 *
 * @typeParam T - The expected resolution type.
 * @param value - The value to test.
 * @returns `true` if the value is promise-like.
 */
function isPromiseLike<T = unknown>(value: unknown): value is PromiseLike<T> {
  return (
    value !== null &&
    (typeof value === 'object' || typeof value === 'function') &&
    typeof (value as { then?: unknown }).then === 'function'
  );
}
