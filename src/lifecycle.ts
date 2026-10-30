import type { EventBusPlatform, Unsubscribe } from './types.js';

/** Internal ownership travels alongside the caller's signal, without composing signals. */
export const scopeSignal = Symbol('scopeSignal');

export interface LifecycleOptions {
  readonly [scopeSignal]?: AbortSignal;
  readonly signal?: AbortSignal;
}

export function abortReason(signal: AbortSignal, platform: EventBusPlatform): unknown {
  return signal.reason === undefined
    ? platform.createError('The operation was aborted', 'AbortError')
    : signal.reason;
}

export function onAbort(
  options: LifecycleOptions,
  callback: (signal: AbortSignal) => void,
): Unsubscribe {
  const signals = [...new Set([options.signal, options[scopeSignal]])].filter(
    (signal): signal is AbortSignal => signal !== undefined,
  );
  const aborted = signals.find((signal) => signal.aborted);
  if (aborted) {
    callback(aborted);
    return () => {};
  }
  const detach: Unsubscribe[] = [];
  const cleanup = () => {
    for (const off of detach) {
      off();
    }
    detach.length = 0;
  };
  for (const signal of signals) {
    const listener = () => {
      cleanup();
      callback(signal);
    };
    signal.addEventListener('abort', listener);
    detach.push(() => signal.removeEventListener('abort', listener));
  }
  return cleanup;
}
