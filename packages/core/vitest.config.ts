import { defineConfig } from 'vitest/config';
import { resolve } from 'node:path';

export default defineConfig({
  test: {
    environment: 'node',
    globals: true,
    include: ['tests/**/*.test.ts'],
    testTimeout: 30_000,
    hookTimeout: 10_000,
    reporters: process.env['CI'] ? ['verbose', 'junit'] : ['verbose'],
    pool: 'threads',
    poolOptions: { threads: { singleThread: false, isolate: true } },
  },
  resolve: {
    alias: {
      // ga-pubsub resolves to this package's own source
      'ga-pubsub': resolve(__dirname, 'src/index.ts'),
    },
  },
});
