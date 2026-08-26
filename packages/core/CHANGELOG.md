# Changelog

## 3.1.0

- Replay is disabled by default and stored envelopes are isolated copies.
- RPC requests are never retained or replayed.
- Inbound envelopes are namespace-checked, deduplicated, and validated before middleware.
- Wildcard, TTL, middleware, validator, lifecycle, and CommonJS correctness fixes.
