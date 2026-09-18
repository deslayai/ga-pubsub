import { existsSync, readFileSync } from 'node:fs';
import { execSync } from 'node:child_process';
import { resolve } from 'node:path';

const root = process.cwd();
const packageJson = JSON.parse(readFileSync(resolve(root, 'package.json'), 'utf8'));
const exportedTargets = new Set();

for (const field of ['main', 'module', 'types']) {
  if (typeof packageJson[field] === 'string') exportedTargets.add(packageJson[field]);
}

function collectTargets(value) {
  if (typeof value === 'string') {
    if (value.startsWith('./')) exportedTargets.add(value);
    return;
  }
  if (!value || typeof value !== 'object') return;
  for (const child of Object.values(value)) collectTargets(child);
}

collectTargets(packageJson.exports);
exportedTargets.add('./dist/cjs/package.json');
exportedTargets.add('./scripts/prepare.mjs');
exportedTargets.add('./README.md');
exportedTargets.add('./LICENSE');

const missingOnDisk = [...exportedTargets].filter(
  (target) => !existsSync(resolve(root, target)),
);
if (missingOnDisk.length > 0) {
  throw new Error(`Missing package entry points:\n${missingOnDisk.join('\n')}`);
}

if (
  [...exportedTargets].some((target) => target.startsWith('./dist/cjs/')) &&
  !existsSync(resolve(root, 'dist/cjs/package.json'))
) {
  throw new Error('Missing dist/cjs/package.json; CommonJS consumers would load ESM.');
}

for (const requiredFile of ['README.md', 'LICENSE']) {
  if (!existsSync(resolve(root, requiredFile))) {
    throw new Error(`Missing required package file: ${requiredFile}`);
  }
}

const packOutput = execSync(
  'npm pack --dry-run --ignore-scripts --json',
  {
    cwd: root,
    encoding: 'utf8',
    env: { ...process.env, npm_config_ignore_scripts: 'true' },
    stdio: ['ignore', 'pipe', 'inherit'],
  },
);
const jsonStart = packOutput.search(/\[\s*\{/);
if (jsonStart === -1) {
  throw new Error(`npm pack did not return JSON:\n${packOutput}`);
}
const packResult = JSON.parse(packOutput.slice(jsonStart));
const packedFiles = new Set(
  (packResult[0]?.files ?? []).map(({ path }) => path.replaceAll('\\', '/')),
);
const missingFromTarball = [...exportedTargets]
  .map((target) => target.replace(/^\.\//, ''))
  .filter((target) => !packedFiles.has(target));
if (missingFromTarball.length > 0) {
  throw new Error(
    `Entry points excluded from package tarball:\n${missingFromTarball.join('\n')}`,
  );
}

console.log(
  `Package verified: ${packageJson.name}@${packageJson.version} (${packedFiles.size} files)`,
);
