import type { EventMap } from '../src/index.ts';
import { EventBus } from '../src/index.ts';

type Events = EventMap & {
  'order.created': { orderId: string };
  'user.created': { id: string };
  'user.deleted': { id: string };
};

const bus = new EventBus<Events>();

bus.on('user.created', async (payload) => {
  console.log(payload);
});

bus.onPattern('**', async (payload) => {
  console.log('any user event', payload);
});

bus.use(async (ctx, next) => {
  const start = Date.now();
  console.log('[event]', ctx.event, ctx.payload);
  try {
    await next();
    console.log('[event-ok]', ctx.event, Date.now() - start, 'ms');
  } catch (e) {
    console.error('[event-error]', ctx.event, e);
    throw e;
  }
});

bus.emit('user.created', { id: '123' });
