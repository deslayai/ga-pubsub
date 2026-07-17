# syntax=docker/dockerfile:1
FROM node:20-alpine AS base
WORKDIR /app

# Install monorepo deps
COPY package*.json ./
COPY packages/core/package*.json ./packages/core/
COPY packages/pro/package*.json ./packages/pro/
RUN npm ci --ignore-scripts

# Copy source
COPY packages/core ./packages/core
COPY usage/backend ./usage/backend

# Build core package
RUN cd packages/core && npm run build

# Build backend
FROM base AS builder
WORKDIR /app/usage/backend
RUN npm run build 2>/dev/null || npx tsc --outDir dist --module node16 --moduleResolution node16

# Production image
FROM node:20-alpine AS runner
WORKDIR /app
ENV NODE_ENV=production

COPY --from=builder /app/packages/core/dist ./packages/core/dist
COPY --from=builder /app/packages/core/package.json ./packages/core/package.json
COPY --from=builder /app/usage/backend/dist ./backend/dist
COPY --from=builder /app/usage/backend/package.json ./backend/package.json

RUN cd backend && npm ci --omit=dev --ignore-scripts

EXPOSE 3001
CMD ["node", "backend/dist/index.js"]
