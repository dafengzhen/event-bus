#!/usr/bin/env node
/**
 * Library build pipeline.
 *
 * Uses the Vite JavaScript API to build each target, format and minification
 * combination with its own output directory.
 *
 * Variants: modern | legacy × esm | cjs × optimized | dev — eight bundles in total.
 *
 * Usage:
 *   node scripts/build.mjs                 # every variant
 *   node scripts/build.mjs --group modern  # only the modern bundles
 *   node scripts/build.mjs --group legacy
 *   node scripts/build.mjs --group dev
 *   node scripts/build.mjs --watch
 *   node scripts/build.mjs --list
 */

import { readdirSync, readFileSync, statSync } from 'node:fs';
import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { gzipSync } from 'node:zlib';
import { build } from 'vite';

const root = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const pkg = JSON.parse(readFileSync(resolve(root, 'package.json'), 'utf8'));

/** Syntax targets for the legacy bundles; runtime APIs still need browser support. */
const LEGACY_TARGET = ['chrome87', 'edge87', 'firefox83', 'safari14'];

/** Explicit modern syntax target; independent of any future Vite default. */
const MODERN_TARGET = 'baseline-widely-available';

const GROUPS = [
  { minify: true, name: 'modern', target: MODERN_TARGET },
  { minify: false, name: 'modern-dev', target: MODERN_TARGET },
  { minify: true, name: 'legacy', target: LEGACY_TARGET },
  { minify: false, name: 'legacy-dev', target: LEGACY_TARGET },
];

const FORMATS = [
  { extension: 'js', format: 'es', name: 'esm' },
  { extension: 'cjs', format: 'cjs', name: 'cjs' },
];

const VARIANTS = GROUPS.flatMap((group) =>
  FORMATS.map((format) => ({
    extension: format.extension,
    format: format.format,
    group: group.name,
    minify: group.minify,
    name: `${group.name}-${format.name}`,
    outDir: `dist/${format.name}/${group.name}`,
    target: group.target,
  })),
);

const GROUP_FILTERS = {
  dev: (group) => group.endsWith('-dev'),
  legacy: (group) => group.startsWith('legacy'),
  modern: (group) => group.startsWith('modern'),
};

/** Collects raw and gzip sizes of files directly inside a variant directory. */
function collectFiles(directory) {
  const entries = [];

  for (const name of readdirSync(directory).sort()) {
    const path = resolve(directory, name);
    const stats = statSync(path);

    if (stats.isFile()) {
      entries.push({ gzip: gzipSync(readFileSync(path)).length, name, size: stats.size });
    }
  }

  return entries;
}

/**
 * Each variant owns its output directory, so emptyOutDir cannot erase sibling builds.
 * Dependencies, peer dependencies and node: imports remain external.
 */
function createConfig(variant, watch) {
  return {
    build: {
      copyPublicDir: false,
      emptyOutDir: true,
      lib: {
        entry: resolve(root, 'src/index.ts'),
        fileName: () => `index.${variant.extension}`,
        formats: [variant.format],
      },
      minify: variant.minify ? 'oxc' : false,
      outDir: resolve(root, variant.outDir),
      reportCompressedSize: false,
      rolldownOptions: {
        external: [
          ...Object.keys(pkg.dependencies ?? {}),
          ...Object.keys(pkg.peerDependencies ?? {}),
          /^node:/,
        ],
      },
      sourcemap: true,
      target: variant.target,
      ...(watch ? { watch: {} } : {}),
    },
    configFile: false,
    logLevel: 'warn',
    root,
  };
}

function formatSize(bytes) {
  return `${(bytes / 1024).toFixed(2)} kB`;
}

function readArguments() {
  const args = process.argv.slice(2);
  const group = args.includes('--group') ? args[args.indexOf('--group') + 1] : undefined;

  return {
    group,
    list: args.includes('--list'),
    watch: args.includes('--watch'),
  };
}

const { group, list, watch } = readArguments();

if (group !== undefined && GROUP_FILTERS[group] === undefined) {
  console.error(
    `Unknown group "${group}", expected one of: ${Object.keys(GROUP_FILTERS).join(', ')}`,
  );
  process.exit(1);
}

const selected =
  group === undefined
    ? VARIANTS
    : VARIANTS.filter((variant) => GROUP_FILTERS[group](variant.group));

if (list) {
  for (const variant of selected) {
    console.log(
      `${variant.name.padEnd(14)} ${variant.format.padEnd(4)} ${variant.outDir.padEnd(24)} minify=${
        variant.minify ? 'yes' : 'no '
      } target=${Array.isArray(variant.target) ? variant.target.join(',') : variant.target}`,
    );
  }

  process.exit(0);
}

console.log(
  `Building ${selected.length} bundle(s) with Vite ${pkg.devDependencies.vite} (Rolldown)\n`,
);

for (const variant of selected) {
  const started = Date.now();

  await build(createConfig(variant, watch));

  if (!watch) {
    const files = collectFiles(resolve(root, variant.outDir));
    const bundle = files.find((file) => file.name.startsWith('index.'));

    console.log(
      `${variant.name.padEnd(14)} ${String(Date.now() - started).padStart(5)} ms  ${
        bundle === undefined ? '' : `${formatSize(bundle.size)} (gzip ${formatSize(bundle.gzip)})`
      }  ${variant.outDir}`,
    );
  } else {
    console.log(`watching ${variant.name} -> ${variant.outDir}`);
  }
}

if (!watch) {
  console.log('\nDone. Types are emitted by `npm run build:types`.');
}
