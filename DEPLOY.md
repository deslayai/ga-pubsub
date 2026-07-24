# GA-PubSub — Deployment Guide

**Zero paid services. Zero external API keys.**
Everything uses `GITHUB_TOKEN` (automatically provided by GitHub Actions — no manual setup).

---

## Overview

| What | How | Cost | API key? |
|------|-----|------|----------|
| Distribute `ga-pubsub` | GitHub Release `.tgz` (public) | Free | None — auto `GITHUB_TOKEN` |
| Distribute `ga-pubsub-pro` | GitHub Release `.tgz` (private repo) | Free | None — auto `GITHUB_TOKEN` |
| Core demo (React SPA + backend) | GitHub Pages + Docker Compose | Free | None |
| PRO demo (Angular SPA) | GitHub Pages | Free | None |
| CI / tests | GitHub Actions | Free | None |

---

## 1. One-time repository setup

### Enable GitHub Pages

In your GitHub repo: **Settings → Pages → Source → Deploy from branch → `gh-pages`**

That's it. The `deploy-demos.yml` workflow creates and updates the `gh-pages` branch automatically on every push to `main`.

### Set the one self-managed secret

The PRO publish workflow needs the license signing key (used to mint customer license keys).
Generate it once:

```bash
openssl rand -hex 32
# e.g. a3f8c1d2e4b7...
```

Add it in: **GitHub repo → Settings → Secrets and variables → Actions → New repository secret**
- Name: `GA_PUBSUB_LICENSE_SECRET`
- Value: the hex string above

This is the **only** secret you ever need to create manually. `GITHUB_TOKEN` everywhere else is automatic.

---

## 2. Releasing packages

### Release ga-pubsub (free, public)

```bash
git tag core-v1.0.0
git push origin core-v1.0.0
```

The `publish-core.yml` workflow runs automatically:
1. Installs dependencies
2. Runs all tests (root + core package)
3. Builds the package
4. Runs `npm pack` → creates `ga-pubsub-1.0.0.tgz`
5. Creates a public GitHub Release and attaches the tarball

Customers install it directly from the release URL — no npm account needed:

```bash
npm install https://github.com/YOUR_ORG/ga-pubsub/releases/download/core-v1.0.0/ga-pubsub-1.0.0.tgz
```

Or if you also publish to npm (optional, free for public packages):
```bash
npm install ga-pubsub
```

### Release ga-pubsub-pro (paid, private repo)

Keep `packages/pro/` in a **separate private GitHub repository** (`ga-pubsub-pro`). Only the monorepo needs to be private — the core repo stays public.

```bash
git tag pro-v1.0.0
git push origin pro-v1.0.0
```

The `publish-pro.yml` workflow:
1. Verifies `GA_PUBSUB_LICENSE_SECRET` is set
2. Runs all tests
3. Builds and packs the package
4. Creates a **private** GitHub Release

Licensed customers get added as collaborators on the private repo (GitHub account, free tier). They install via:

```bash
# Using a GitHub personal access token (free, read-only scope is enough)
npm install https://TOKEN@github.com/YOUR_ORG/ga-pubsub-pro/releases/download/pro-v1.0.0/ga-pubsub-pro-1.0.0.tgz
```

Or pin in `package.json`:
```json
"ga-pubsub-pro": "github:YOUR_ORG/ga-pubsub-pro#pro-v1.0.0"
```

---

## 3. Demo apps → GitHub Pages

### How it works

On every push to `main` (when demo or library source changes), `deploy-demos.yml` runs:
1. Builds `usage/frontend` → core demo SPA
2. Builds `packages/pro/usage` → PRO demo Angular SPA
3. Assembles both into a single `gh-pages` branch:
   - `https://YOUR_ORG.github.io/ga-pubsub/` → core demo
   - `https://YOUR_ORG.github.io/ga-pubsub/pro/` → PRO demo
4. Pushes to `gh-pages` using `GITHUB_TOKEN` — no external deploy service

### Core demo backend (optional)

The React core demo can run fully in-browser (no backend needed). Set `VITE_API_URL=''` in the workflow to skip it.

If you want the live Socket.io event stream, run the backend yourself with Docker (free):

```bash
cd usage/backend
cp .env.example .env       # edit CORS_ORIGINS if needed
docker compose up -d       # starts on port 3001
```

Docker and Docker Compose are free and open source. No account needed.

### PRO demo access control

The PRO Angular demo validates a `licenseKey` inside `ProEventBus` using HMAC-SHA256. Without a valid key, PRO features are blocked at the library level — no external gate needed. The demo is public; the library protects itself.

---

## 4. Local development

```bash
# Install everything
npm install

# Run all 134 tests
npm test

# Per-package tests
cd packages/core && node ../../node_modules/vitest/vitest.mjs run --config ./vitest.config.ts
cd packages/pro  && node ../../node_modules/vitest/vitest.mjs run --config ./vitest.config.ts

# Core demo
cd usage/backend  && npm install && npm run dev   # :3001
cd usage/frontend && npm install && npm run dev   # :5173

# PRO demo
cd packages/pro/usage && npm install && npm run dev   # :5174
```

---

## 5. Generating customer license keys

```bash
# From repo root — uses the same secret as GA_PUBSUB_LICENSE_SECRET
GA_PUBSUB_LICENSE_SECRET=<your-secret> \
  npx vite-node packages/pro/scripts/keygen.ts \
  --cid acme-corp \
  --plan professional \
  --seats 10 \
  --days 365
```

Output:
```
lk_eyJjaWQiOiJhY21lLWNvcnAiLCAicGxhbiI6InByb2Zlc3Npb25hbCJ9.abc123...
```

**Rules:**
- `GA_PUBSUB_LICENSE_SECRET` must stay server-side only — never in browser bundles
- Customers store their `licenseKey` in server-side env vars and pass it to `new ProEventBus({ licenseKey })`
- Rotate the secret immediately if compromised (all previously issued keys become invalid)
- Per the PRO license: customers must notify `https://deslay-ai.web.app/community` within 48 hours of any suspected key exposure

---

## 6. Secrets reference

| Secret | Where | How created |
|--------|-------|-------------|
| `GA_PUBSUB_LICENSE_SECRET` | GitHub repo secrets | `openssl rand -hex 32` — you create once |
| `GITHUB_TOKEN` | All other workflows | **Auto-provided by GitHub Actions — nothing to do** |

That's the entire secrets list. Two entries, one of which requires zero action from you.

---

## 7. Release flow diagram

```
feature/* → PR → CI (ci.yml) — Node 18/20/22, zero external calls
                  ↓ merge to main
            deploy-demos.yml → GitHub Pages (auto GITHUB_TOKEN)
                  ↓ tag core-v*
            publish-core.yml → public GitHub Release .tgz
                  ↓ tag pro-v*
            publish-pro.yml  → private GitHub Release .tgz
```
