import { defineConfig } from 'vitest/config';
import { resolve } from 'node:path';

export default defineConfig({
  test: {
    environment: 'node',
    globals: true,
    include: [
      'tests/unit/**/*.test.ts',
    ],
    exclude: [
      '**/node_modules/**',
      '**/dist/**',
    ],
    setupFiles: ['tests/setup.ts'],
    testTimeout: 30_000,
    hookTimeout: 10_000,
    reporters: process.env['CI'] ? ['verbose', 'junit'] : ['verbose'],
    outputFile: { junit: './test-results/junit.xml' },
    coverage: {
      provider: 'v8',
      reporter: ['text', 'text-summary', 'html', 'lcov', 'json'],
      reportsDirectory: './coverage',
      include: ['packages/core/src/**/*.ts'],
      exclude: ['packages/core/src/index.ts', 'docs/**', '**/*.d.ts', '**/node_modules/**'],
      thresholds: { lines: 70, functions: 70, branches: 75, statements: 70 },
      all: true,
    },
    pool: 'threads',
    isolate: true,
    sequence: { shuffle: false },
  },
  resolve: {
    alias: {
      // This repository resolves only the free browser core. PRO and its
      // adapters are built and tested in their separate commercial repositories.
      'ga-pubsub':                    resolve(import.meta.dirname, 'packages/core/src/index.ts'),
      'ga-pubsub/validators':         resolve(import.meta.dirname, 'packages/core/src/validators.ts'),
      'ga-pubsub/integrations':       resolve(import.meta.dirname, 'packages/core/src/integrations.ts'),
    },
  },
});
