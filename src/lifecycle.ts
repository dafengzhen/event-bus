import type { Unsubscribe } from './types.js';

export function abortReason(signal: AbortSignal): unknown {
  return signal.reason ?? new DOMException('The operation was aborted', 'AbortError');
}

export function onAbort(signal: AbortSignal | undefined, callback: Unsubscribe): Unsubscribe {
  if (!signal) {
    return () => {};
  }
  if (signal.aborted) {
    callback();
    return () => {};
  }
  signal.addEventListener('abort', callback, { once: true });
  return () => signal.removeEventListener('abort', callback);
}
