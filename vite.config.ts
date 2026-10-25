/** Serves and builds the examples/ playground; scripts/build.mjs builds the library. */

import { resolve } from 'node:path';
import { defineConfig } from 'vite';

const root = import.meta.dirname;
const examples = resolve(root, 'examples');

export default defineConfig({
  // Relative asset URLs support deployment under a repository subpath.
  base: './',
  build: {
    emptyOutDir: true,
    outDir: 'dist',
    rolldownOptions: {
      input: resolve(examples, 'index.html'),
    },
    sourcemap: true,
  },
  root: 'examples',
  server: {
    open: '/',
    port: 5173,
  },
});
