'use strict';
const JSON = (require('./json.js'));
const $coincident = (require('./index.js'));

/**
 * Create once a `Proxy` able to orchestrate synchronous `postMessage` out of the box.
 * @param {globalThis | Worker} self the context in which code should run
 * @returns {ProxyHandler<globalThis> | ProxyHandler<Worker>}
 */
const coincident = (self, options) => $coincident(self, {...JSON, ...options});

coincident.transfer = $coincident.transfer;

module.exports = coincident;
