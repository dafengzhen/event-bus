import type { EventBus } from './event-bus.js';
import type { LifecycleOptions } from './lifecycle.js';
import type {
  AsyncMiddleware,
  EventBusPlatform,
  EventContext,
  EventPattern,
  Listener,
  Middleware,
  MiddlewareOptions,
  Observer,
  SubscriptionOptions,
  Unsubscribe,
  WaitForOptions,
} from './types.js';

import { abortReason, onAbort, scopeSignal } from './lifecycle.js';
import { createPlatform } from './platform.js';

/** Explicit ownership: disposing a parent also disposes its children. */
export class EventScope<E extends object> {
  get disposed(): boolean {
    return this.signal.aborted;
  }
  get signal(): AbortSignal {
    return this.controller.signal;
  }
  private readonly bus: EventBus<E>;

  private readonly controller: AbortController;

  private readonly detachParent: Unsubscribe;
  private readonly platform: EventBusPlatform;

  /** Prefer bus.scope() or parent.scope(). */
  constructor(bus: EventBus<E>, parent: AbortSignal, platform?: Partial<EventBusPlatform>) {
    this.bus = bus;
    this.platform = createPlatform(platform);
    this.controller = this.platform.createAbortController();
    this.detachParent = onAbort({ signal: parent }, () => this.dispose());
  }

  dispose(): void {
    if (!this.disposed) {
      this.controller.abort(this.platform.createError('The operation was aborted', 'AbortError'));
      this.detachParent?.();
    }
  }

  on<K extends keyof E>(
    type: K,
    listener: Listener<E, NoInfer<K>>,
    options?: SubscriptionOptions,
  ): Unsubscribe {
    return this.bus.on(type, listener, this.bind(options));
  }

  onAny(observer: Observer<E>, options?: SubscriptionOptions): Unsubscribe {
    return this.bus.onAny(observer, this.bind(options));
  }

  once<K extends keyof E>(
    type: K,
    listener: Listener<E, NoInfer<K>>,
    options?: Omit<SubscriptionOptions, 'once'>,
  ): Unsubscribe {
    return this.on(type, listener, { ...options, once: true });
  }

  onPattern(
    pattern: EventPattern,
    observer: Observer<E>,
    options?: SubscriptionOptions,
  ): Unsubscribe {
    return this.bus.onPattern(pattern, observer, this.bind(options));
  }

  scope(): EventScope<E> {
    this.assertActive();
    return new EventScope(this.bus, this.signal, this.platform);
  }

  use(middleware: Middleware<E>, options?: MiddlewareOptions): Unsubscribe {
    return this.bus.use(middleware, this.bind(options));
  }

  useAsync(middleware: AsyncMiddleware<E>, options?: MiddlewareOptions): Unsubscribe {
    return this.bus.useAsync(middleware, this.bind(options));
  }

  waitFor<K extends keyof E>(
    type: K,
    options: WaitForOptions<E, NoInfer<K>> = {},
  ): Promise<EventContext<E, K>> {
    if (this.disposed) {
      return Promise.reject(abortReason(this.signal, this.platform));
    }
    return this.bus.waitFor(type, this.bind(options));
  }

  private assertActive(): void {
    if (this.disposed) {
      throw new Error('EventScope has been disposed.');
    }
  }

  private bind<T extends { readonly signal?: AbortSignal }>(options?: T): LifecycleOptions & T {
    this.assertActive();
    return { ...options, [scopeSignal]: this.signal } as LifecycleOptions & T;
  }
}
