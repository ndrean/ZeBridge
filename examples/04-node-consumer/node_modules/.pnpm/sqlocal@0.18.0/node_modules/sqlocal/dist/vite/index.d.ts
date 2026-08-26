import type { Plugin, UserConfig } from 'vite';
/**
 * Represents the configuration that SQLocal's Vite plugin accepts.
 * @see {@link https://sqlocal.dev/guide/setup#vite-configuration}
 */
export type VitePluginConfig = {
    /**
     * If set to `false`, the plugin will not add the
     * HTTP response headers required for
     * [cross-origin isolation](https://sqlocal.dev/guide/setup#cross-origin-isolation)
     * to the Vite development server.
     * @default true
     */
    coi?: boolean;
};
/**
 * A Vite plugin that tweaks some Vite settings for building apps
 * that use SQLocal.
 * @see {@link https://sqlocal.dev/guide/setup#vite-configuration}
 */
export default function vitePluginSQLocal(config?: VitePluginConfig): Plugin<UserConfig>;
