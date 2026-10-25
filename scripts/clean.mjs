#!/usr/bin/env node
/** Removes library output, playground output and coverage within the project root. */

import { rm } from 'node:fs/promises';
import { dirname, resolve, sep } from 'node:path';
import { fileURLToPath } from 'node:url';

const root = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const targets = ['coverage', 'dist', 'examples/dist'];

for (const target of targets) {
  const targetPath = resolve(root, target);
  if (!targetPath.startsWith(`${root}${sep}`)) {
    throw new Error(`Cleanup target must stay within the project: ${targetPath}`);
  }
  await rm(targetPath, { force: true, recursive: true });
  console.log(`removed ${target}`);
}
