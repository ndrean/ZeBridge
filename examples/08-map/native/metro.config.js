// zb-client-ts is LINKED from outside this project and ships raw TypeScript
// (its exports point at ./src/*.ts). Metro only watches the project root by
// default, so the link has to be named here or the bundler cannot see the files
// it resolves to.
const { getDefaultConfig } = require('expo/metro-config');
const path = require('node:path');

const client = path.resolve(__dirname, '../../../zb-client-ts');
const config = getDefaultConfig(__dirname);
config.watchFolders = [client];
config.resolver.nodeModulesPaths = [
  path.resolve(__dirname, 'node_modules'),
  path.resolve(client, 'node_modules'),
];
module.exports = config;
