import { defineConfig } from 'vite';
import solidPlugin from 'vite-plugin-solid';

/// Where the stack actually is. Defaults are the NATIVE dev loop's ports; the
/// compose stack publishes elsewhere and is free to move, so both are env vars:
///
///   ZB_BRIDGE_ORIGIN=http://127.0.0.1:9098 \
///   ZB_NATS_WS_ORIGIN=ws://127.0.0.1:8081  \
///     npm run dev
///
/// ⚠️ These are the ONLY two places a port is written down. The client used to
/// hardcode `ws://localhost:8080` and `http://localhost:9090`; compose published the
/// same services on 8081 and 9098 and the page simply failed to connect, with
/// nothing a browser can report beyond "connection refused". Measured 2026-08-28:
/// both of those host ports were FREE while the services ran one number away.
const BRIDGE_ORIGIN = process.env.ZB_BRIDGE_ORIGIN ?? 'http://127.0.0.1:9090';
const NATS_WS_ORIGIN = process.env.ZB_NATS_WS_ORIGIN ?? 'ws://127.0.0.1:8080';

export default defineConfig({
  plugins: [solidPlugin()],
  server: {
    port: 5173,
    // ⚠️ The bridge's HTTP endpoints are fetched through here, not directly.
    //
    // Two reasons, and the second is the hard one. Most of the bridge's endpoints send
    // no CORS headers (only `/enroll` does) — but this page also sets
    // `Cross-Origin-Embedder-Policy: require-corp` below, which OPFS needs, and under
    // COEP a cross-origin response must either carry `Cross-Origin-Resource-Policy` or
    // be fetched with CORS. Making `fetch('http://127.0.0.1:9090/health')` work would
    // mean putting those headers on an endpoint set that includes an unauthenticated
    // `/metrics`.
    //
    // Proxying makes it same-origin for the browser, costs the bridge nothing, and ships
    // nothing to production — this block only exists in the dev server.
    //
    // NATS goes through here for a THIRD reason on top of those: it deletes the last
    // port from the client. With both proxied, the page talks only to its own origin
    // and the stack's real ports live in exactly one file — this one.
    proxy: {
      '/bridge': {
        target: BRIDGE_ORIGIN,
        changeOrigin: true,
        rewrite: (path: string) => path.replace(/^\/bridge/, ''),
      },
      // `ws: true` is what makes vite forward the Upgrade rather than answering it.
      // The rewrite drops the prefix: nats-server serves its websocket at the root,
      // and a Go-based client will ask for `/` regardless of the path it was given
      // (see NOTES §10ak), so the two ends agree on `/` and nothing else.
      '/nats': {
        target: NATS_WS_ORIGIN,
        ws: true,
        changeOrigin: true,
        rewrite: (path: string) => path.replace(/^\/nats/, '/'),
      },
    },
    headers: {
      "Cross-Origin-Opener-Policy": "same-origin",
      "Cross-Origin-Embedder-Policy": "require-corp"
    }
  },
  optimizeDeps: {
    // PGlite ships its WASM/fs assets next to its module; pre-bundling breaks the
    // relative asset URLs, same class of problem as sqlocal's worker below.
    exclude: ['sqlocal', '@electric-sql/pglite']
  },
  resolve: {
    // zb-client-ts is a linked (symlinked) package; without dedupe its imports
    // resolve inside zb-client-ts/node_modules, served over /@fs/ URLs. sqlocal
    // spawns its worker with an EXTENSIONLESS `new URL('./worker', import.meta.url)`
    // — vite resolves that under /node_modules/ but serves the SPA index.html
    // (200, text/html) for the /@fs/ form, so the worker dies silently and every
    // DB call hangs. Dedupe pins the shared runtime deps to this package's copies.
    dedupe: ['sqlocal', '@electric-sql/pglite', '@nats-io/nats-core', '@nats-io/jetstream', '@nats-io/kv', '@nats-io/obj', '@msgpack/msgpack', 'uuid'],
  }
});
