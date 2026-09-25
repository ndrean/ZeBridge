// zb-client-ts is LINKED from outside this project and ships raw TypeScript (its
// exports point at ./src/*.ts). Two things follow, and both bit (§10ij):
//
//   1. Metro only watches the project root, so the link has to be named in
//      `watchFolders` or the bundler cannot see the files it resolves to.
//   2. A file INSIDE the linked package resolves its own imports by walking up from
//      its own directory — into pnpm's symlinked store, which Metro does not follow.
//      `Unable to resolve "@nats-io/nats-core" from "../../../zb-client-ts/src/transport.ts"`
//      even though the package sits in both node_modules trees.
//
// `extraNodeModules` is the deterministic answer: ANY bare specifier that normal
// resolution misses is served from THIS app's node_modules. One copy of each package,
// chosen here rather than by whichever directory happened to be walked first — which
// also rules out the duplicate-React class of bug that plagues linked packages.
const { getDefaultConfig } = require('expo/metro-config');
const path = require('node:path');

const client = path.resolve(__dirname, '../../../zb-client-ts');
const appModules = path.resolve(__dirname, 'node_modules');

const config = getDefaultConfig(__dirname);
config.watchFolders = [client];
config.resolver.unstable_enableSymlinks = true;
// ⚠️ THE ACTUAL CAUSE of `Unable to resolve "@nats-io/nats-core"`. That package declares
// NO `main` and no `module` — only an `exports` map — and Metro does not read `exports`
// by default at this version, so it finds no entry point at all and reports the module
// as missing rather than as unreadable. Vite reads exports, which is why the browser app
// never hit this. Every @nats-io package is shaped the same way.
config.resolver.unstable_enablePackageExports = true;
config.resolver.nodeModulesPaths = [appModules];
config.resolver.extraNodeModules = new Proxy(
  {},
  { get: (_, name) => path.join(appModules, String(name)) },
);

// Node built-ins reached from libraries that only use them on a branch this app never
// takes. Metro resolves `await import("node:crypto")` statically, so an unreachable
// line still breaks the bundle (§10ij). The stub throws if anything actually calls it.
const stub = path.resolve(__dirname, 'src/node-builtin-stub.js');
const defaultResolve = config.resolver.resolveRequest;
const fs = require('node:fs');
// A bare specifier that THIS app's node_modules holds resolves from here, whichever
// file asked. The linked library's own `import('js-sha256')` (the streaming digest,
// §10ix) otherwise resolved into zb-client-ts/node_modules/.pnpm — outside the project
// root — and Android's bundle then asked for a relative path that does not exist
// ("Unable to resolve module ./zb-client-ts/node_modules/.pnpm/js-sha256…", 2026-09-24);
// iOS had happened to load the same path. `extraNodeModules` only catches what normal
// resolution MISSES, and the walk up from zb-client-ts/src does not miss it.
const bare = /^(?!\.|\/|node:)[^/]+(\/[^/]+)?/;
const browserOnly = /^(@bokuweb\/zstd-wasm|\.\/pglite-storage\.ts|\.\/browser-storage\.ts)$/;
config.resolver.resolveRequest = (context, moduleName, platform) => {
  if (moduleName.startsWith('node:')) return { type: 'sourceFile', filePath: stub };
  // The library's browser-only defaults: the zstd fallback and the storage picked when
  // the host passes none. This app passes `zstdDecompress` (fzstd) and its own storage,
  // so these imports are never reached, but Metro bundles them anyway: zstd-wasm
  // resolved to its Node entry (`fs/promises`), and the Release step's Hermes compiler
  // rejects the Emscripten `import.meta.url` in PGlite/sqlite-wasm (2026-09-25).
  if (browserOnly.test(moduleName)) return { type: 'sourceFile', filePath: stub };
  const m = bare.exec(moduleName);
  if (m && !context.originModulePath.startsWith(appModules) && fs.existsSync(path.join(appModules, m[0]))) {
    return (defaultResolve ?? context.resolveRequest)({ ...context, originModulePath: path.join(__dirname, 'index.js') }, moduleName, platform);
  }
  return (defaultResolve ?? context.resolveRequest)(context, moduleName, platform);
};

module.exports = config;
