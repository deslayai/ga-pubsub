/**
 * GA AUTOMATIONS — SOCIAL MEDIA AUTOMATION ENGINE (Enhanced Backend)
 *
 * Setup:
 *   npm init -y
 *   npm install express cors firebase-admin axios bullmq ioredis
 *   npm install -D typescript @types/express @types/node @types/cors ts-node
 *   export GOOGLE_APPLICATION_CREDENTIALS="./serviceAccountKey.json"
 *   export CORS_ORIGIN="http://localhost:5173"
 *   npx ts-node automation-engine-backend.ts
 *
 * Bug Fixes from original:
 *  [BUG 1] Fixed: res.status(200) sent BEFORE actionQueue.add() — if Redis
 *          was down, client got false success while message was dropped.
 *          Now: queue first → 200 on success, 500 on failure.
 *  [BUG 2] Added: CORS middleware (browser cross-origin requests were blocked).
 *  [BUG 3] Added: Content-Type validation.
 *  [BUG 4] Fixed: parseTemplate now resolves {{step_ID.field}} cross-step refs.
 *  [BUG 5] Added: Worker error/failed event handlers.
 *  [BUG 6] Added: Graceful SIGTERM/SIGINT shutdown.
 *  [BUG 7] Added: Actual Instagram, LinkedIn, Gemini API implementations.
 *  [BUG 8] Added: Sequential step execution with per-step output context.
 */

import express, { Request, Response } from 'express';
import cors from 'cors';
import * as admin from 'firebase-admin';
import axios from 'axios';
import { Queue, Worker, Job } from 'bullmq';
import IORedis from 'ioredis';

// ─── Types ──────────────────────────────────────────────────────────────────

interface WorkflowStep {
  id: string;
  type: 'ai_gemini' | 'instagram' | 'linkedin' | 'log' | string;
  name: string;
  config: Record<string, any>;
}

interface ExecutionContext {
  triggerPayload: Record<string, any>;
  stepOutputs: Record<string, any>;
}

// ─── Core Services ───────────────────────────────────────────────────────────

const app = express();

// [BUG 2 FIX] CORS — required for browser frontend calls
app.use(cors({
  origin: process.env['CORS_ORIGIN'] || '*',
  methods: ['GET', 'POST'],
  allowedHeaders: ['Content-Type', 'Authorization']
}));

app.use(express.json({ limit: '2mb' }));

admin.initializeApp();
const db = admin.firestore();

const redisConnection = new IORedis({
  host: process.env['REDIS_HOST'] || 'localhost',
  port: parseInt(process.env['REDIS_PORT'] || '6379', 10),
  password: process.env['REDIS_PASSWORD'] || undefined,
  maxRetriesPerRequest: null
});

const actionQueue = new Queue('workflow-actions', { connection: redisConnection });

// ─── Health Check ────────────────────────────────────────────────────────────

app.get('/health', (_req, res) => {
  res.json({ status: 'ok', service: 'ga-automations-engine', timestamp: Date.now() });
});

// ─── Webhook Receiver ─────────────────────────────────────────────────────────
// [BUG 1 FIX] Original: sent 200 → then tried to queue (failure = silent data loss)
// Fixed: queue → then send 200 (or 500 if queue fails)

app.post('/webhook/:appId/:userId/:workflowId', async (req: Request, res: Response) => {
  if (!req.is('application/json')) {
    return res.status(400).json({ error: 'Content-Type must be application/json' });
  }

  const { appId, userId, workflowId } = req.params;

  try {
    // QUEUE FIRST — if this throws, we can still send 500
    await actionQueue.add('execute', {
      appId,
      userId,
      workflowId,
      triggerPayload: req.body,
      receivedAt: Date.now()
    }, {
      attempts: 3,
      backoff: { type: 'exponential', delay: 2000 },
      removeOnComplete: { age: 86_400 },
      removeOnFail:    { age: 604_800 }
    });

    // Only AFTER successful queue, respond with 200
    return res.status(200).json({ status: 'queued', timestamp: Date.now() });

  } catch (error) {
    console.error('[Webhook] Queue failed:', error);
    return res.status(500).json({ error: 'Failed to queue. Redis may be unavailable.' });
  }
});

// ─── Background Worker ────────────────────────────────────────────────────────
// [BUG 7+8 FIX] Each step type now has its own executor with real API calls.
// Steps run sequentially; each step's output is available to subsequent steps.

const worker = new Worker('workflow-actions', async (job: Job) => {
  const { appId, userId, workflowId, triggerPayload } = job.data;

  console.log(`[Worker] Job ${job.id} — Workflow [${workflowId}]`);

  const workflowRef = db
    .collection('artifacts').doc(appId)
    .collection('users').doc(userId)
    .collection('workflows').doc(workflowId);

  const docSnap = await workflowRef.get();
  if (!docSnap.exists) throw new Error(`Workflow [${workflowId}] not found.`);

  const workflow = docSnap.data()!;
  const steps: WorkflowStep[] = workflow.steps || [];
  const stepOutputs: Record<string, any> = {};
  const context: ExecutionContext = { triggerPayload, stepOutputs };

  for (const step of steps) {
    console.log(`  => [${step.type}] ${step.name}`);
    let output: any = {};

    switch (step.type) {
      case 'ai_gemini':   output = await executeGeminiStep(step, context);                    break;
      case 'instagram':   output = await executeInstagramStep(step, context);                 break;
      case 'linkedin':    output = await executeLinkedInStep(step, context);                  break;
      case 'log':         output = await executeLogStep(step, context, workflowRef);          break;
      default:
        if (step.config?.actionUrl) {
          const payload = resolveTemplate(step.config.actionPayloadTemplate || '{}', context);
          const resp = await axios.post(step.config.actionUrl, payload, {
            headers: { 'Content-Type': 'application/json' },
            timeout: 10_000
          });
          output = resp.data;
        }
    }

    stepOutputs[step.id] = output;
  }

  await workflowRef.collection('logs').add({
    status: 'success',
    jobId: job.id,
    stepsExecuted: steps.length,
    timestamp: admin.firestore.FieldValue.serverTimestamp()
  });

  return { success: true, stepsExecuted: steps.length };

}, { connection: redisConnection, concurrency: 5 });

// [BUG 5 FIX] Worker error events were missing
worker.on('failed', (job, err) => {
  console.error(`[Worker] Job ${job?.id} FAILED: ${err.message}`);
});
worker.on('error', (err) => {
  console.error('[Worker] Uncaught error:', err);
});

// ─── Step Executors ───────────────────────────────────────────────────────────

async function executeGeminiStep(step: WorkflowStep, ctx: ExecutionContext): Promise<any> {
  const apiKey = process.env['GEMINI_API_KEY'];
  if (!apiKey) throw new Error('GEMINI_API_KEY not set.');

  const resp = await axios.post(
    `https://generativelanguage.googleapis.com/v1beta/models/gemini-1.5-flash:generateContent?key=${apiKey}`,
    {
      contents: [{ parts: [{
        text: `[SYSTEM]: ${step.config.systemPrompt || 'You are a social media expert.'}\n\n[USER]: ${step.config.userPrompt || 'Write an engaging post.'}`
      }] }],
      generationConfig: {
        responseMimeType: 'application/json',
        responseSchema: {
          type: 'object',
          properties: {
            text:                 { type: 'string' },
            image_url:            { type: 'string' },
            is_scheduled_content: { type: 'boolean' },
            scheduled_time:       { type: 'string' }
          },
          required: ['text']
        }
      }
    },
    { timeout: 30_000 }
  );

  const parsed = JSON.parse(resp.data.candidates?.[0]?.content?.parts?.[0]?.text || '{"text":""}');

  if (step.config.enableDelay && parsed.is_scheduled_content && parsed.scheduled_time) {
    const delayMs = new Date(parsed.scheduled_time).getTime() - Date.now();
    if (delayMs > 0 && delayMs < 3_600_000) {
      console.log(`    Waiting ${Math.round(delayMs / 1000)}s for scheduled_time...`);
      await new Promise(r => setTimeout(r, delayMs));
    }
  }

  return parsed;
}

async function executeInstagramStep(step: WorkflowStep, ctx: ExecutionContext): Promise<any> {
  const { accountId, token } = step.config;
  if (!accountId || !token) throw new Error('Instagram: missing accountId or token.');

  const imageUrl = resolveStringTemplate(step.config.imageUrl || '', ctx);
  const caption  = resolveStringTemplate(step.config.caption  || '', ctx);

  if (!imageUrl) throw new Error('Instagram: imageUrl resolved to empty string.');

  // Step 1: Create media container
  const container = await axios.post(
    `https://graph.facebook.com/v18.0/${accountId}/media`,
    { image_url: imageUrl, caption, access_token: token },
    { timeout: 15_000 }
  );

  // Step 2: Publish
  const publish = await axios.post(
    `https://graph.facebook.com/v18.0/${accountId}/media_publish`,
    { creation_id: container.data.id, access_token: token },
    { timeout: 15_000 }
  );

  return { postId: publish.data.id, platform: 'instagram', imageUrl, caption };
}

async function executeLinkedInStep(step: WorkflowStep, ctx: ExecutionContext): Promise<any> {
  const { authorUrn, token } = step.config;
  if (!authorUrn || !token) throw new Error('LinkedIn: missing authorUrn or token.');

  const text     = resolveStringTemplate(step.config.text     || '', ctx);
  const mediaUrl = resolveStringTemplate(step.config.mediaUrl || '', ctx);

  const body: any = {
    author: authorUrn,
    lifecycleState: 'PUBLISHED',
    specificContent: {
      'com.linkedin.ugc.ShareContent': {
        shareCommentary: { text },
        shareMediaCategory: mediaUrl ? 'IMAGE' : 'NONE',
        ...(mediaUrl && { media: [{ status: 'READY', originalUrl: mediaUrl, description: { text } }] })
      }
    },
    visibility: { 'com.linkedin.ugc.MemberNetworkVisibility': 'PUBLIC' }
  };

  const resp = await axios.post('https://api.linkedin.com/v2/ugcPosts', body, {
    headers: {
      Authorization: `Bearer ${token}`,
      'Content-Type': 'application/json',
      'X-Restli-Protocol-Version': '2.0.0'
    },
    timeout: 15_000
  });

  return { postId: resp.headers['x-restli-id'] || resp.data.id, platform: 'linkedin', text };
}

async function executeLogStep(
  step: WorkflowStep,
  ctx: ExecutionContext,
  workflowRef: FirebaseFirestore.DocumentReference
): Promise<any> {
  const message = resolveStringTemplate(step.config.message || 'Pipeline completed.', ctx);
  await workflowRef.collection('logs').add({
    type: 'step_log',
    stepId: step.id,
    stepName: step.name,
    message,
    timestamp: admin.firestore.FieldValue.serverTimestamp()
  });
  return { logged: true, message };
}

// ─── Template Engine ──────────────────────────────────────────────────────────
// [BUG 4 FIX] Now resolves both:
//   {{trigger.field}}    → from webhook payload
//   {{step_ID.field}}    → from a previous step's output

function resolveStringTemplate(template: string, ctx: ExecutionContext): string {
  return template.replace(/\{\{([\w.]+)\}\}/g, (_match, path) => {
    const keys = path.split('.');

    if (keys[0].startsWith('step_')) {
      const stepId = keys[0].slice(5);
      let val: any = ctx.stepOutputs[stepId];
      for (const k of keys.slice(1)) { if (val == null) return ''; val = val[k]; }
      return val == null ? '' : String(val);
    }

    let val: any = ctx.triggerPayload;
    for (const k of keys) { if (val == null) return ''; val = val[k]; }
    return val == null ? '' : String(val);
  });
}

function resolveTemplate(template: string, ctx: ExecutionContext): any {
  try {
    return JSON.parse(resolveStringTemplate(template, ctx));
  } catch (e) {
    throw new Error(`Template parse failed: ${e}`);
  }
}

// ─── Server & Graceful Shutdown ───────────────────────────────────────────────
// [BUG 6 FIX] SIGTERM was unhandled — in-flight jobs could be killed abruptly.

const PORT = parseInt(process.env['PORT'] || '3000', 10);
const server = app.listen(PORT, () => {
  console.log(`\n🚀 GA Automations Engine on http://localhost:${PORT}`);
  console.log(`   Redis queue active. Waiting for webhooks...\n`);
});

async function shutdown(signal: string) {
  console.log(`\n[${signal}] Shutting down gracefully...`);
  server.close();
  await worker.close();
  await actionQueue.close();
  redisConnection.disconnect();
  process.exit(0);
}

process.on('SIGTERM', () => shutdown('SIGTERM'));
process.on('SIGINT',  () => shutdown('SIGINT'));
