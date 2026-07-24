/**
 * Post-build step: compiles a CommonJS bundle (dist/index.cjs)
 * from the same source using tsconfig.cjs.json.
 *
 * Run after the ESM tsc build:
 *   tsc && node scripts/build-cjs.mjs
 */

import { readFileSync, writeFileSync, rmSync } from 'node:fs';
import { execSync } from 'node:child_process';
import { resolve, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = dirname(fileURLToPath(import.meta.url));
const root = resolve(__dirname, '..');

// Compile to a temp folder using the CJS tsconfig
execSync('npx tsc -p tsconfig.cjs.json', { cwd: root, stdio: 'inherit' });

// Copy compiled index.js → dist/index.cjs
const src  = resolve(root, 'dist/_cjs_tmp/index.js');
const dest = resolve(root, 'dist/index.cjs');
writeFileSync(dest, readFileSync(src, 'utf8'));

// Clean up temp folder
rmSync(resolve(root, 'dist/_cjs_tmp'), { recursive: true, force: true });

console.log('✓ CJS build complete → dist/index.cjs');
