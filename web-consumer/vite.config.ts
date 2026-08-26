import { defineConfig } from 'vite';
import solidPlugin from 'vite-plugin-solid';

export default defineConfig({
  plugins: [solidPlugin()],
  server: {
    port: 5173,
    // ⚠️ The bridge's HTTP endpoints are fetched through here, not directly.
    //
    // Two reasons, and the second is the hard one. The bridge sends no CORS headers — but
    // this page also sets `Cross-Origin-Embedder-Policy: require-corp` below, which OPFS
    // needs, and under COEP a cross-origin response must additionally carry
    // `Cross-Origin-Resource-Policy`. So making `fetch('http://127.0.0.1:9090/health')`
    // work would mean putting CORS *and* CORP headers on an endpoint set that includes an
    // unauthenticated `/metrics`.
    //
    // Proxying makes it same-origin for the browser, costs the bridge nothing, and ships
    // nothing to production — this block only exists in the dev server.
    proxy: {
      '/bridge': {
        target: 'http://127.0.0.1:9090',
        changeOrigin: true,
        rewrite: (path: string) => path.replace(/^\/bridge/, ''),
      },
    },
    headers: {
      "Cross-Origin-Opener-Policy": "same-origin",
      "Cross-Origin-Embedder-Policy": "require-corp"
    }
  },
  optimizeDeps: {
    exclude: ['sqlocal']
  },
  resolve: {
    // zb-client-ts is a linked (symlinked) package; without dedupe its imports
    // resolve inside zb-client-ts/node_modules, served over /@fs/ URLs. sqlocal
    // spawns its worker with an EXTENSIONLESS `new URL('./worker', import.meta.url)`
    // — vite resolves that under /node_modules/ but serves the SPA index.html
    // (200, text/html) for the /@fs/ form, so the worker dies silently and every
    // DB call hangs. Dedupe pins the shared runtime deps to this package's copies.
    dedupe: ['sqlocal', '@nats-io/nats-core', '@nats-io/jetstream', '@nats-io/kv', '@nats-io/obj', '@msgpack/msgpack', 'uuid'],
  }
});
