import type { EventBusPlatform } from './types.js';

export function createPlatform(options: Partial<EventBusPlatform> = {}): EventBusPlatform {
  return {
    clearTimeout: (timer) => clearTimeout(timer as ReturnType<typeof setTimeout>),
    createAbortController: () => {
      if (typeof AbortController === 'undefined') {
        throw new Error(
          'AbortController is unavailable. Provide platform.createAbortController or load a polyfill.',
        );
      }
      return new AbortController();
    },
    createError: (message, name) => {
      if (typeof DOMException !== 'undefined') {
        return new DOMException(message, name);
      }
      const error = new Error(message);
      error.name = name;
      return error;
    },
    setTimeout: (callback, timeout) => setTimeout(callback, timeout),
    ...options,
  };
}
