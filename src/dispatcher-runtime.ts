import type { EventScope } from './event-scope.ts';
import type { EventMap } from './types.ts';

/**
 * DispatcherRuntime
 *
 * A small runtime helper that manages an {@link EventScope} stack and provides
 * scoped execution for event dispatching.
 *
 * ## Scope semantics
 * - {@link runWithScope} pushes the given scope onto an internal stack,
 *   executes the callback, and then pops the scope afterwards.
 * - Nested scopes are supported: the most recently pushed scope is considered
 *   the "current" scope.
 * - {@link getScope} always returns the current (top-most) scope.
 *
 * ## Sync / async behavior
 * - If the callback returns a non-Promise value, the scope is popped
 *   synchronously in the `finally` block.
 * - If the callback returns a Promise, the scope is popped after the Promise
 *   settles (via `finally()`), ensuring the scope remains active for the entire
 *   async chain.
 *
 * ## Safety notes
 * This implementation ensures the scope is popped exactly once:
 * - For synchronous callbacks, pop happens in the outer `finally`.
 * - For async callbacks, pop happens in the Promise `finally()`, and the outer
 *   `finally` will detect that the scope is no longer on top (or is already
 *   popped) and will not pop again.
 *
 * @typeParam E - The event map type describing event names and their payloads.
 *
 * @author dafengzhen
 */
export class DispatcherRuntime<E extends EventMap> {
  /**
   * Internal scope stack.
   * The last element is considered the current scope.
   */
  private scopeStack: EventScope<E>[] = [];

  /**
   * Returns the current active {@link EventScope} (the top of the stack),
   * or `undefined` if no scope is active.
   */
  getScope(): EventScope<E> | undefined {
    return this.scopeStack[this.scopeStack.length - 1];
  }

  /**
   * Executes a function within the given {@link EventScope}.
   *
   * The scope is pushed before execution and guaranteed to be popped after
   * the function finishes.
   *
   * If the function returns a Promise, the scope remains active until that
   * Promise settles.
   *
   * @typeParam T - The return type of the callback.
   * @param scope - The scope to activate for the duration of the callback.
   * @param fn - The callback to execute under the given scope.
   * @returns The callback result (either a value or a Promise).
   */
  runWithScope<T>(scope: EventScope<E>, fn: () => T): T {
    this.scopeStack.push(scope);

    try {
      const ret = fn();
      if (ret instanceof Promise) {
        return ret.finally(() => {
          this.scopeStack.pop();
        }) as T;
      }
      return ret;
    } finally {
      const top = this.scopeStack[this.scopeStack.length - 1];
      if (top === scope) {
        this.scopeStack.pop();
      }
    }
  }

  /**
   * Async convenience wrapper for {@link runWithScope}.
   *
   * This is equivalent to calling {@link runWithScope} with an async callback,
   * but provides a clearer API for async-only call sites.
   *
   * @typeParam T - The resolved type of the returned Promise.
   * @param scope - The scope to activate for the duration of the callback.
   * @param fn - The async callback to execute under the given scope.
   * @returns A Promise resolving to the callback result.
   */
  async runWithScopeAsync<T>(scope: EventScope<E>, fn: () => Promise<T>): Promise<T> {
    return this.runWithScope(scope, fn);
  }
}
