import { defineConfig } from 'vite';

/// Where the stack is: the page talks only to its own origin, and these two lines are
/// the only place a port is written. Defaults: the SUPABASE_TEST.md stack.
///   ZB_NATS_WS_ORIGIN=ws://127.0.0.1:8080 ZB_BRIDGE_ORIGIN=http://127.0.0.1:27434 pnpm dev
const NATS_WS_ORIGIN = process.env.ZB_NATS_WS_ORIGIN ?? 'ws://127.0.0.1:8082';
const BRIDGE_ORIGIN = process.env.ZB_BRIDGE_ORIGIN ?? 'http://127.0.0.1:27434';

export default defineConfig({
  server: {
    port: 5175,
    proxy: {
      // nats-server serves its websocket at the root
      '/nats': { target: NATS_WS_ORIGIN, ws: true, changeOrigin: true, rewrite: (p: string) => p.replace(/^\/nats/, '/') },
      // /enroll and /renew
      '/bridge': { target: BRIDGE_ORIGIN, changeOrigin: true, rewrite: (p: string) => p.replace(/^\/bridge/, '') },
    },
    // The replica's storage (OPFS) needs a cross-origin isolated page.
    headers: {
      'Cross-Origin-Opener-Policy': 'same-origin',
      'Cross-Origin-Embedder-Policy': 'require-corp',
    },
  },
  // `pnpm build && pnpm preview`: the built page, with the same two headers
  // (a static host sends them from public/_headers).
  preview: {
    port: 5176,
    headers: {
      'Cross-Origin-Opener-Policy': 'same-origin',
      'Cross-Origin-Embedder-Policy': 'require-corp',
    },
  },
  optimizeDeps: { exclude: ['sqlocal'] },
  resolve: {
    dedupe: ['sqlocal', 'fzstd', '@nats-io/nats-core', '@nats-io/jetstream', '@nats-io/kv', '@nats-io/obj', '@msgpack/msgpack', 'uuid'],
  },
});
