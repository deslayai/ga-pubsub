/**
 * GA-PubSub Core Demo — Backend Server
 *
 * Express HTTP + Socket.io bridge to frontend.
 * Uses ONLY ga-pubsub (no PRO features).
 *
 * Start: npx vite-node src/index.ts
 */

import { createServer } from 'http';
import express from 'express';
import cors from 'cors';
import { initBus } from './bus.js';
import { initSocket, broadcast } from './socket.js';
import {
  demoBasicPubSub, demoWildcardSingle, demoWildcardMulti,
  demoPriority, demoOnce, demoMiddleware, demoValidation,
  demoReplay, demoRPC, demoCancel, demoTTL, demoMetrics,
} from './demo/features.js';
import { serverConfig } from './config.js';

async function bootstrap() {
  await initBus();

  const app = express();
  app.use(cors({ origin: serverConfig.corsOrigin }));
  app.use(express.json({ limit: '2mb' }));

  const httpServer = createServer(app);
  initSocket(httpServer);

  app.get('/api/health', (_req, res) => {
    res.json({ status: 'ok', package: 'ga-pubsub', timestamp: Date.now() });
  });

  const demos: Record<string, () => Promise<unknown>> = {
    'basic':          demoBasicPubSub,
    'wildcard-single': demoWildcardSingle,
    'wildcard-multi': demoWildcardMulti,
    'priority':       demoPriority,
    'once':           demoOnce,
    'middleware':     demoMiddleware,
    'validation':     demoValidation,
    'replay':         demoReplay,
    'rpc':            demoRPC,
    'cancel':         demoCancel,
    'ttl':            demoTTL,
    'metrics':        demoMetrics,
  };

  Object.entries(demos).forEach(([key, fn]) => {
    app.post(`/api/demo/${key}`, async (_req, res) => {
      try {
        const result = await fn();
        res.json(result);
      } catch (e) {
        const errResult = { feature: key, status: 'error', error: (e as Error).message };
        broadcast('demo:result', errResult);
        res.status(500).json(errResult);
      }
    });
  });

  httpServer.listen(serverConfig.port, () => {
    console.log(`\n🚀 GA-PubSub Core Demo running on http://localhost:${serverConfig.port}`);
    console.log(`   Package: ga-pubsub (free, MIT)\n`);
  });
}

bootstrap().catch((e) => { console.error('[FATAL]', e); process.exit(1); });
