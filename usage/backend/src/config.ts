// ─── Redis Config ─────────────────────────────────────────────────────────────
export const redisConfig = {
  host: process.env['REDIS_HOST'] ?? 'localhost',
  port: parseInt(process.env['REDIS_PORT'] ?? '6379', 10),
  password: process.env['REDIS_PASSWORD'] ?? undefined,
  lazyConnect: true,
};

// ─── Kafka Config ─────────────────────────────────────────────────────────────
export const kafkaConfig = {
  brokers: (process.env['KAFKA_BROKERS'] ?? 'localhost:9092').split(','),
  clientId: 'ga-pubsub-demo',
  groupId: 'ga-pubsub-demo-group',
  topic: 'ga-pubsub-events',
};

// ─── Server Config ────────────────────────────────────────────────────────────
export const serverConfig = {
  port: parseInt(process.env['PORT'] ?? '3001', 10),
  // SIGNING_SECRET: set this in .env — must be 32+ chars, never use the default in prod
  signingSecret: process.env['SIGNING_SECRET'] ?? (() => {
    if (process.env['NODE_ENV'] === 'production') {
      throw new Error('SIGNING_SECRET env var must be set in production');
    }
    return 'demo-secret-key-32-chars-minimum!!';
  })(),
  corsOrigin: process.env['CORS_ORIGIN'] ?? 'http://localhost:5173',
};
