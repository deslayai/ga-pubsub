import { existsSync, readFileSync } from 'node:fs';
import { execSync } from 'node:child_process';
import { resolve } from 'node:path';

const packageJson = JSON.parse(readFileSync('package.json', 'utf8'));
const targets = new Set();
function collect(value) {
  if (typeof value === 'string') {
    if (value.startsWith('./')) targets.add(value);
    return;
  }
  if (!value || typeof value !== 'object') return;
  for (const child of Object.values(value)) collect(child);
}
for (const field of ['main', 'module', 'types']) collect(packageJson[field]);
collect(packageJson.exports);
targets.add('./dist/cjs/package.json');

if ([...targets].some((target) => !existsSync(resolve(target)))) {
  execSync('npm run build', { stdio: 'inherit' });
}

