import { defineConfig } from 'vitest/config';
import { resolve } from 'node:path';

export default defineConfig({
  test: {
    environment: 'node',
    globals: true,
    include: [
      'tests/**/*.test.ts',
    ],
    exclude: [
      '**/node_modules/**',
      '**/dist/**',
      'tests/integration/transports.test.ts',
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
      include: ['packages/*/src/**/*.ts'],
      exclude: ['packages/*/src/index.ts', 'docs/**', '**/*.d.ts', '**/node_modules/**'],
      thresholds: { lines: 95, functions: 95, branches: 90, statements: 95 },
      all: true,
    },
    pool: 'threads',
    poolOptions: { threads: { singleThread: false, isolate: true, useAtomics: true } },
    sequence: { shuffle: false },
  },
  resolve: {
    alias: {
      // Core resolves from ga-pubsub-core (so pro's internal imports work)
      'ga-pubsub-core':               resolve(__dirname, 'packages/core/src/index.ts'),
      // ga-pubsub resolves to pro so all 134 existing tests exercise PRO features
      'ga-pubsub':                    resolve(__dirname, 'packages/pro/src/index.ts'),
      'ga-pubsub/validators':         resolve(__dirname, 'packages/core/src/validators.ts'),
      'ga-pubsub/integrations':       resolve(__dirname, 'packages/core/src/integrations.ts'),
      '@ga-pubsub/websocket':         resolve(__dirname, 'packages/websocket/src/index.ts'),
      '@ga-pubsub/sse':               resolve(__dirname, 'packages/sse/src/index.ts'),
      '@ga-pubsub/broadcast-channel': resolve(__dirname, 'packages/broadcast-channel/src/index.ts'),
      '@ga-pubsub/redis':             resolve(__dirname, 'packages/redis/src/index.ts'),
      '@ga-pubsub/kafka':             resolve(__dirname, 'packages/kafka/src/index.ts'),
      '@ga-pubsub/nats':              resolve(__dirname, 'packages/nats/src/index.ts'),
      '@ga-pubsub/rabbitmq':          resolve(__dirname, 'packages/rabbitmq/src/index.ts'),
    },
  },
});
