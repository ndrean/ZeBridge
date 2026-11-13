import { defineConfig } from 'vite';

const proxy = {
  '/nats': {
    target: process.env.ZB_NATS_WS_ORIGIN ?? 'ws://127.0.0.1:8082',
    ws: true, changeOrigin: true,
    rewrite: (p: string) => p.replace(/^\/nats/, '/'),
  },
  '/bridge': {
    target: process.env.ZB_BRIDGE_ORIGIN ?? 'http://127.0.0.1:27434',
    changeOrigin: true,
    rewrite: (p: string) => p.replace(/^\/bridge/, ''),
  },
};
const headers = {
  'Cross-Origin-Opener-Policy': 'same-origin',
  'Cross-Origin-Embedder-Policy': 'require-corp',
};

export default defineConfig({
  server: { port: 5181, strictPort: true, proxy, headers },
  preview: { port: 5182, strictPort: true, proxy, headers },
  optimizeDeps: { exclude: ['sqlocal'] },
  resolve: { dedupe: ['yjs', '@codemirror/state', '@codemirror/view'] },
});
