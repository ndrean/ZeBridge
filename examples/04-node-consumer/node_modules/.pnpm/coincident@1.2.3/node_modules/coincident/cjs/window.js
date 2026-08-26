'use strict';
const {MAIN, THREAD} = require('./channel.js');
const $coincident = (require('./index.js'));
const main = (require('./window/main.js'));
const thread = (require('./window/thread.js'));
const Worker = (require('./shared/worker.js'));

const proxies = new WeakMap;

/**
 * @typedef {object} Coincident
 * @property {ProxyHandler<globalThis>} proxy
 * @property {ProxyHandler<Window>} window
 * @property {(value: any) => boolean} isWindowProxy
 */

/**
 * Create once a `Proxy` able to orchestrate synchronous `postMessage` out of the box.
 * In workers, returns a `{proxy, window, isWindowProxy}` namespace to reach main globals synchronously.
 * @param {Worker | globalThis} self the context in which code should run
 * @returns {ProxyHandler<Worker> | Coincident}
 */
const coincident = (self, ...args) => {
  const proxy = $coincident(self, ...args);
  if (!proxies.has(proxy)) {
    const util = self instanceof Worker ? main : thread;
    proxies.set(proxy, util.call(args.at(0), proxy, MAIN, THREAD));
  }
  return proxies.get(proxy);
}

coincident.transfer = $coincident.transfer;

module.exports = coincident;
