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
config.resolver.resolveRequest = (context, moduleName, platform) => {
  if (moduleName.startsWith('node:')) return { type: 'sourceFile', filePath: stub };
  return (defaultResolve ?? context.resolveRequest)(context, moduleName, platform);
};

module.exports = config;
