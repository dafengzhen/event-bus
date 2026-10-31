import type { EventBusPlatform } from './types.js';

interface AbortListener {
  readonly callback: EventListenerOrEventListenerObject;
  readonly capture: boolean;
  readonly once: boolean;
  readonly type: string;
}

/** Local cancellation fallback; does not depend on DOM globals or patch the host. */
export function createAbortController(
  createError: EventBusPlatform['createError'],
): AbortController {
  let aborted = false;
  let reason: unknown;
  const listeners = new Set<AbortListener>();
  const signal: AbortSignal = {
    get aborted() {
      return aborted;
    },
    addEventListener(
      type: string,
      callback: EventListenerOrEventListenerObject | null,
      options?: AddEventListenerOptions | boolean,
    ) {
      const capture = typeof options === 'boolean' ? options : (options?.capture ?? false);
      if (
        callback &&
        ![...listeners].some(
          (entry) =>
            entry.type === type && entry.callback === callback && entry.capture === capture,
        )
      ) {
        listeners.add({
          callback,
          capture,
          once: typeof options === 'object' && (options.once ?? false),
          type,
        });
      }
    },
    dispatchEvent(event: Event) {
      const callbacks: (() => void)[] = [];
      for (const entry of listeners) {
        if (entry.type === event.type) {
          callbacks.push(() => {
            if (!listeners.has(entry)) {
              return;
            }
            if (entry.once) {
              listeners.delete(entry);
            }
            if (typeof entry.callback === 'function') {
              entry.callback.call(signal, event);
            } else {
              entry.callback.handleEvent(event);
            }
          });
        }
      }
      if (event.type === 'abort') {
        callbacks.push(() => signal.onabort?.call(signal, event));
      }
      for (const callback of callbacks) {
        try {
          callback();
        } catch (error) {
          // One observer must not prevent cancellation of the remaining scopes.
          setTimeout(() => {
            throw error;
          }, 0);
        }
      }
      return !event.defaultPrevented;
    },
    onabort: null,
    get reason() {
      return reason;
    },
    removeEventListener(
      type: string,
      callback: EventListenerOrEventListenerObject | null,
      options?: boolean | EventListenerOptions,
    ) {
      const capture = typeof options === 'boolean' ? options : (options?.capture ?? false);
      for (const entry of listeners) {
        if (entry.type === type && entry.callback === callback && entry.capture === capture) {
          listeners.delete(entry);
        }
      }
    },
    throwIfAborted() {
      if (aborted) {
        throw reason;
      }
    },
  };
  return {
    abort(value?: unknown) {
      if (aborted) {
        return;
      }
      reason = value === undefined ? createError('The operation was aborted', 'AbortError') : value;
      aborted = true;
      const event = typeof Event === 'function' ? new Event('abort') : ({ type: 'abort' } as Event);
      Object.defineProperties(event, {
        currentTarget: { configurable: true, value: signal },
        target: { configurable: true, value: signal },
      });
      signal.dispatchEvent(event);
    },
    signal,
  };
}
