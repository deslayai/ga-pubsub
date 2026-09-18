import { mkdirSync, rmSync, writeFileSync } from 'node:fs';
import { execSync } from 'node:child_process';
import { resolve } from 'node:path';

const root = resolve(import.meta.dirname, '..');
const outputDirectory = resolve(root, 'dist/cjs');
rmSync(outputDirectory, { recursive: true, force: true });
execSync('tsc -p tsconfig.cjs.json', {
  cwd: root,
  stdio: 'inherit',
});
mkdirSync(outputDirectory, { recursive: true });
writeFileSync(
  resolve(outputDirectory, 'package.json'),
  `${JSON.stringify({ type: 'commonjs' }, null, 2)}\n`,
  'utf8',
);

console.log('CommonJS build complete: dist/cjs');
