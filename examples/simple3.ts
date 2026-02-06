import { EventBus } from '../src/index.ts';

const bus = new EventBus<{
  'a:b:c': 'payload123';
  'a:b': 'payload456';
  'a:b:c:d': 'payload789';
  'x:y:z': 'payloadXYZ';
}>();

bus.on('a:b:c', (payload) => {
  console.log(`on: a:b:c, event: a:b:c, payload: ${payload}`);
});

bus.on('a:b:*', (event, payload) => {
  console.log(`on: a:b:*, event: ${event}, payload: ${payload}`);
});

bus.on('a:*', (event, payload) => {
  console.log(`on: a:*, event: ${event}, payload: ${payload}`);
});

bus.on('a:**', (event, payload) => {
  console.log(`on: a:**, event: ${event}, payload: ${payload}`);
});

bus.on('**', (event, payload) => {
  console.log(`on: **, event: ${event}, payload: ${payload}`);
});

bus.on('*', (event, payload) => {
  console.log(`on: *, event: ${event}, payload: ${payload}`);
});

bus.on('*:*:*', (event, payload) => {
  console.log(`on: *:*:*, event: ${event}, payload: ${payload}`);
});

bus.emit('a:b:c', 'payload123');
// on: a:b:c, event: a:b:c, payload: payload123
// on: a:b:*, event: a:b:c, payload: payload123
// on: a:**, event: a:b:c, payload: payload123
// on: *:*:*, event: a:b:c, payload: payload123
// on: **, event: a:b:c, payload: payload123

// bus.emit('a:b', 'payload456');
// bus.emit('a:b:c:d', 'payload789');
// bus.emit('x:y:z', 'payloadXYZ');
