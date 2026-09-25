import { defineConfig } from 'vite';

/// Where the stack is. The page talks only to its own origin: the websocket and the
/// creds are proxied, so the ports live here and nowhere else (the web-consumer's
/// config says the same, for the same reasons).
const NATS_WS_ORIGIN = process.env.ZB_NATS_WS_ORIGIN ?? 'ws://127.0.0.1:8080';

export default defineConfig({
  server: {
    port: 5174, // 5173 is the web-consumer's: both can run at once
    proxy: {
      // `ws: true` forwards the Upgrade instead of answering it; nats-server serves
      // its websocket at the root (NOTES §10ak).
      '/nats': {
        target: NATS_WS_ORIGIN,
        ws: true,
        changeOrigin: true,
        rewrite: (path: string) => path.replace(/^\/nats/, '/'),
      },
    },
    // OPFS needs cross-origin isolation.
    headers: {
      'Cross-Origin-Opener-Policy': 'same-origin',
      'Cross-Origin-Embedder-Policy': 'require-corp',
    },
  },
  optimizeDeps: {
    // sqlocal fetches its worker relative to its own module URL; pre-bundling breaks that.
    exclude: ['sqlocal'],
  },
  resolve: {
    // zb-client-ts is linked: without dedupe its imports resolve inside its own
    // node_modules and are served over /@fs/ URLs, where sqlocal's extensionless
    // worker URL silently returns the SPA's index.html and every DB call hangs.
    dedupe: ['sqlocal', 'fzstd', '@nats-io/nats-core', '@nats-io/jetstream', '@nats-io/kv', '@nats-io/obj', '@msgpack/msgpack', 'uuid'],
  },
});
