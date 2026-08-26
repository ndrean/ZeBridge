/**
 * A Vite plugin that tweaks some Vite settings for building apps
 * that use SQLocal.
 * @see {@link https://sqlocal.dev/guide/setup#vite-configuration}
 */
export default function vitePluginSQLocal(config = { coi: true }) {
    return {
        name: 'vite-plugin-sqlocal',
        enforce: 'pre',
        config(config) {
            return {
                optimizeDeps: {
                    ...config.optimizeDeps,
                    exclude: [
                        ...(config.optimizeDeps?.exclude ?? []),
                        'sqlocal',
                        '@sqlite.org/sqlite-wasm',
                    ],
                },
                worker: {
                    ...config.worker,
                    format: 'es',
                },
            };
        },
        configureServer(server) {
            if (config.coi !== false) {
                server.middlewares.use((_, res, next) => {
                    res.setHeader('Cross-Origin-Embedder-Policy', 'require-corp');
                    res.setHeader('Cross-Origin-Opener-Policy', 'same-origin');
                    next();
                });
            }
        },
    };
}
//# sourceMappingURL=index.js.map