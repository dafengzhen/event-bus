#!/usr/bin/env node
/**
 * Emits ESM declarations and mirrors them as .d.cts for CommonJS consumers.
 * NodeNext compilation enforces explicit extensions in relative source imports.
 * Relative .js specifiers become .cjs so NodeNext resolves the matching
 * declaration files instead of treating require() imports as ESM.
 */

import { spawnSync } from 'node:child_process';
import { readdirSync, readFileSync, writeFileSync } from 'node:fs';
import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const root = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const typesDirectory = resolve(root, 'dist/types');

const result = spawnSync(
  process.execPath,
  [resolve(root, 'node_modules/typescript/bin/tsc'), '-p', 'tsconfig.types.json'],
  {
    cwd: root,
    encoding: 'utf8',
    stdio: 'inherit',
  },
);

if (result.status !== 0) {
  process.exit(result.status ?? 1);
}

let emitted = 0;

// Keep the source directory structure: entry declarations reference nested modules.
for (const name of readdirSync(typesDirectory, { recursive: true })) {
  if (!name.endsWith('.d.ts')) {
    continue;
  }

  const source = readFileSync(resolve(typesDirectory, name), 'utf8');
  const commonjs = source.replace(
    /((?:from\s*|import\s*\()\s*['"])(\.{1,2}\/[^'"]+)\.js(?=['"])/g,
    '$1$2.cjs',
  );

  writeFileSync(resolve(typesDirectory, name.replace(/\.d\.ts$/, '.d.cts')), commonjs);
  emitted += 1;
}

console.log(`declarations: ${emitted} .d.ts + ${emitted} .d.cts files in dist/types`);
