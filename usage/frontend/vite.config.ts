import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';

export default defineConfig({
  // VITE_BASE_URL is set in CI for GitHub Pages (e.g. /ga-pubsub/).
  // Defaults to '/' for local dev.
  base: process.env['VITE_BASE_URL'] ?? '/',
  plugins: [react()],
  server: {
    port: 5173,
    proxy: {
      '/api': {
        target: 'http://localhost:3001',
        changeOrigin: true,
      },
    },
  },
});
