import { createEventBus } from '../src/index.js';

interface Events {
  ready: void;
  'user:login': { id: string };
  'user:logout': string;
}

const bus = createEventBus<Events>();
const scope = bus.scope();
const output = document.querySelector<HTMLPreElement>('#output')!;
const append = (message: string) => {
  output.textContent += `${message}\n`;
};

scope.onPattern('user:.*', (event) => {
  if (event.type === 'user:login') {
    append(`Pattern observer: ${event.payload.id} logged in`);
  } else if (event.type === 'user:logout') {
    append(`Pattern observer: ${event.payload} logged out`);
  }
});
scope.on(
  'user:login',
  ({ id }) => {
    append(`Hello, ${id}!`);
  },
  { priority: 10 },
);
scope.use((event, next) => {
  append(`Dispatch #${event.id}: ${event.type}`);
  next();
});

bus.emit('ready', undefined, { retain: true });
scope.once(
  'ready',
  (_, event) => {
    append(`Ready (replayed: ${event.replayed})`);
  },
  { replay: true },
);

document.querySelector('#login')!.addEventListener('click', () => {
  bus.emit('user:login', { id: 'Ada' });
});
document.querySelector('#logout')!.addEventListener('click', () => {
  bus.emit('user:logout', 'Ada');
});
document.querySelector<HTMLButtonElement>('#dispose')!.addEventListener('click', (event) => {
  scope.dispose();
  append(`Scope disposed. Remaining listeners: ${bus.listenerCount()}`);
  (event.currentTarget as HTMLButtonElement).disabled = true;
});
append('Click a button to dispatch an event.');
