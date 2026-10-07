// zb-react-native (libzb's Expo module) is LINKED from outside this project and ships raw
// TypeScript. Two things follow (§10ij):
//
//   1. Metro only watches the project root, so the link has to be named in
//      `watchFolders` or the bundler cannot see the files it resolves to.
//   2. A file INSIDE the linked package resolves its own imports (`expo`) by walking up
//      from its own directory — into another node_modules, or none. A bare specifier this
//      app's node_modules holds resolves from HERE instead, whichever file asked: one copy
//      of each package, which also rules out the duplicate-React class of bug.
const { getDefaultConfig } = require('expo/metro-config');
const path = require('node:path');
const fs = require('node:fs');

const native = path.resolve(__dirname, '../../../zb-react-native');
const appModules = path.resolve(__dirname, 'node_modules');

const config = getDefaultConfig(__dirname);
config.watchFolders = [native];
config.resolver.unstable_enableSymlinks = true;
config.resolver.unstable_enablePackageExports = true;
config.resolver.nodeModulesPaths = [appModules];

const defaultResolve = config.resolver.resolveRequest;
const bare = /^(?!\.|\/|node:)[^/]+(\/[^/]+)?/;
config.resolver.resolveRequest = (context, moduleName, platform) => {
  const m = bare.exec(moduleName);
  if (m && !context.originModulePath.startsWith(appModules) && fs.existsSync(path.join(appModules, m[0]))) {
    return (defaultResolve ?? context.resolveRequest)({ ...context, originModulePath: path.join(__dirname, 'index.js') }, moduleName, platform);
  }
  return (defaultResolve ?? context.resolveRequest)(context, moduleName, platform);
};

module.exports = config;
