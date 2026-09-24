import { createLogger, defineConfig } from 'vite';
import solidPlugin from 'vite-plugin-solid';

// Same dev-server shape as 05-tables/web-consumer, whose vite.config.ts explains every
// line: the bridge and NATS are proxied so the page is same-origin (COEP for OPFS),
// the stack's ports live here and nowhere else, and the linked zb-client-ts needs its
// runtime deps deduped onto this package's copies. Only the port and the missing
// PGlite differ — this example is SQLite on OPFS only.
const logger = createLogger();
const warn = logger.warn;
logger.warn = (msg, opts) => {
  if (msg.includes('points to missing source files') && msg.includes('zstd-wasm')) return;
  warn(msg, opts);
};

const BRIDGE_ORIGIN = process.env.ZB_BRIDGE_ORIGIN ?? 'http://127.0.0.1:27434';
const NATS_WS_ORIGIN = process.env.ZB_NATS_WS_ORIGIN ?? 'ws://127.0.0.1:8080';

export default defineConfig({
  customLogger: logger,
  plugins: [solidPlugin()],
  server: {
    port: 5175,
    proxy: {
      '/bridge': {
        target: BRIDGE_ORIGIN,
        changeOrigin: true,
        rewrite: (path: string) => path.replace(/^\/bridge/, ''),
      },
      '/nats': {
        target: NATS_WS_ORIGIN,
        ws: true,
        changeOrigin: true,
        rewrite: (path: string) => path.replace(/^\/nats/, '/'),
      },
    },
    headers: {
      'Cross-Origin-Opener-Policy': 'same-origin',
      'Cross-Origin-Embedder-Policy': 'require-corp',
    },
  },
  optimizeDeps: {
    exclude: ['sqlocal', '@bokuweb/zstd-wasm'],
  },
  resolve: {
    dedupe: ['sqlocal', '@bokuweb/zstd-wasm', '@nats-io/nats-core', '@nats-io/jetstream', '@nats-io/kv', '@nats-io/obj', '@msgpack/msgpack', 'uuid'],
  },
});
