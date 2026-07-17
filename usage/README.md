# GA-PubSub — Payment Platform Demo

Full-stack interactive demo showing every GA-PubSub feature through a live payment platform.

## Quick Start

### 1. Backend (Terminal 1)
```bash
cd usage/backend
npm install
npx vite-node src/index.ts
# → http://localhost:3001
```

### 2. Frontend (Terminal 2)
```bash
cd usage/frontend
npm install
npm run dev
# → http://localhost:5173
```

---

## Architecture

```
Frontend (React + Vite)
  └── Socket.io client  ──────────────────────────────┐
  └── Axios REST client ─────────────────────────┐    │
                                                  │    │
Backend (Express + Firebase Functions)           │    │
  ├── GA-PubSub EventBus ◄────────────────────────┘    │
  │     ├── Redis Transport   (real or simulated)       │
  │     ├── Kafka Transport   (real or simulated)       │
  │     ├── HMAC Signing                                │
  │     ├── Replay Engine                               │
  │     ├── Rate Limiter                                │
  │     └── Schema Validators                           │
  └── Socket.io Server ────────────────────────────────┘
        (bridges all bus events to frontend in real-time)
```

## Features Demonstrated

| # | Feature | Payment Scenario |
|---|---------|-----------------|
| 1 | Basic Pub/Sub | Send & receive payment events |
| 2 | Wildcard `*` | `payments.*` matches created, failed |
| 3 | Wildcard `**` | `payments.**` matches deep invoice paths |
| 4 | Priority | Fraud alerts before receipts |
| 5 | subscribeOnce | One-time receipt notification |
| 6 | Middleware | Add processing fee (2.9%) to payload |
| 7 | Schema Validation | Reject invalid payment amounts |
| 8 | Authorization | Block anonymous admin deletes |
| 9 | Replay Engine | Late subscribers get payment history |
| 10 | RPC Request/Reply | Query payment status |
| 11 | Request Cancel | Cancel pending status check |
| 12 | HMAC Signing | Sign + detect tampered envelopes |
| 13 | Rate Limiting | Block payment floods (20/sec) |
| 14 | Payload Size Limit | Reject payloads > 64 KB |
| 15 | TTL Expiry | Session events auto-expire |
| 16 | Replay Attack | Block double-spend attempts |
| 17 | Live Metrics | p95 latency, auth denials, publish count |

## Transports

Both Redis and Kafka auto-detect availability:
- **Connected**: uses real Redis/Kafka
- **Simulated**: in-process mock with identical interface (no Redis/Kafka needed)

Status shown live in the UI header.

## Firebase Deployment

1. Update `backend/firebase.json` with your project ID
2. Update `backend/src/config.ts` with real Firebase config
3. `cd backend && firebase deploy`

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `PORT` | `3001` | Backend HTTP port |
| `REDIS_HOST` | `localhost` | Redis hostname |
| `REDIS_PORT` | `6379` | Redis port |
| `KAFKA_BROKERS` | `localhost:9092` | Comma-separated brokers |
| `SIGNING_SECRET` | *(demo key)* | HMAC signing secret — **change in production** |
| `CORS_ORIGIN` | `http://localhost:5173` | Allowed frontend origin |
