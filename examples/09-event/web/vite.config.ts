import { defineConfig } from 'vite';

/// The page talks only to its own origin: the NATS websocket is proxied at `/nats` and the
/// creds are served from `public/creds` (a symlink to scripts/native/creds), as in 08-map/web.
const NATS_WS_ORIGIN = process.env.ZB_NATS_WS_ORIGIN ?? 'ws://127.0.0.1:8080';

export default defineConfig({
  server: {
    port: 5176, // 5173 web-consumer, 5174 08-map, 5175 06-large-table: all can run at once
    proxy: {
      '/nats': { target: NATS_WS_ORIGIN, ws: true, changeOrigin: true, rewrite: (p: string) => p.replace(/^\/nats/, '/') },
    },
    // zb-client-ts keeps its local store in OPFS, which needs cross-origin isolation.
    headers: { 'Cross-Origin-Opener-Policy': 'same-origin', 'Cross-Origin-Embedder-Policy': 'require-corp' },
  },
  optimizeDeps: { exclude: ['sqlocal'] }, // sqlocal fetches its worker relative to its module
});
