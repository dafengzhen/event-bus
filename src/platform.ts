import type { EventBusPlatform } from './types.js';

import { createAbortController } from './abort-controller.js';

export function createPlatform(options: Partial<EventBusPlatform> = {}): EventBusPlatform {
  const platform: EventBusPlatform = {
    clearTimeout: (timer) => clearTimeout(timer as ReturnType<typeof setTimeout>),
    createAbortController: () => {
      return typeof AbortController === 'function'
        ? new AbortController()
        : createAbortController(platform.createError);
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
  return platform;
}
