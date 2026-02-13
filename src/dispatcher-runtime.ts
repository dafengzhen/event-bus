import type { EventScope } from './event-scope.ts';
import type { EventMap } from './types.ts';

/**
 * DispatcherRuntime class manages the stack of event scopes and provides the ability to execute functions within specific scopes.
 * It ensures that asynchronous operations also maintain the proper scope context.
 *
 * @template E - Event map type that must extend the EventMap interface
 * @author dafengzhen
 */
export class DispatcherRuntime<E extends EventMap> {
  /**
   * Stack of scopes used to store nested event scopes.
   * The top of the stack represents the currently active scope.
   *
   * @private
   * @type {EventScope<E>[]}
   */
  private scopeStack: EventScope<E>[] = [];

  /**
   * Returns the current active scope (the top of the scope stack).
   *
   * @returns {EventScope<E> | undefined} The current scope, or undefined if the stack is empty
   */
  getScope(): EventScope<E> | undefined {
    return this.scopeStack[this.scopeStack.length - 1];
  }

  /**
   * Executes a function within a specified scope context.
   * Automatically handles both synchronous and asynchronous functions,
   * ensuring the scope is properly popped after execution.
   *
   * @template T - Return type of the function
   * @param {EventScope<E>} scope - The scope to push onto the stack during execution
   * @param {() => T} fn - The function to execute within the scope context
   * @returns {T} The return value of the function (preserves Promise if async)
   *
   * @example
   * ```typescript
   * const result = runtime.runWithScope(scope, () => {
   *   // Code running within the scope context
   *   return someValue;
   * });
   * ```
   */
  runWithScope<T>(scope: EventScope<E>, fn: () => T): T {
    this.scopeStack.push(scope);

    const expectedDepth = this.scopeStack.length;

    const popIfTop = () => {
      if (
        this.scopeStack.length === expectedDepth &&
        this.scopeStack[expectedDepth - 1] === scope
      ) {
        this.scopeStack.pop();
      }
    };

    try {
      const ret = fn();

      if (ret instanceof Promise) {
        return ret.finally(popIfTop) as T;
      }

      return ret;
    } finally {
      popIfTop();
    }
  }

  /**
   * Executes an asynchronous function within a specified scope context.
   * This is a convenience wrapper around runWithScope for async operations.
   *
   * @template T - Return type of the async function
   * @param {EventScope<E>} scope - The scope to push onto the stack during execution
   * @param {() => Promise<T>} fn - The async function to execute within the scope context
   * @returns {Promise<T>} A promise that resolves with the function's return value
   *
   * @example
   * ```typescript
   * const result = await runtime.runWithScopeAsync(scope, async () => {
   *   // Async code running within the scope context
   *   const data = await fetchData();
   *   return processData(data);
   * });
   * ```
   */
  runWithScopeAsync<T>(scope: EventScope<E>, fn: () => Promise<T>): Promise<T> {
    return this.runWithScope(scope, fn);
  }
}
