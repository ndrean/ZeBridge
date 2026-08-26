'use strict';
const {MAIN, THREAD} = require('./channel.js');
const $coincident = require('./structured.js');
const main = require('./window/main.js');
const thread = require('./window/thread.js');
const be = require('./ws/be.js');
const fe = require('./ws/fe.js');
const server = require('./ws/server.js');

const proxies = new WeakMap;

const coincident = (self, ...args) => {
  if (self.process) {
    if (!proxies.has(self))
        proxies.set(self, server(...args));
  }
  else {
    const proxy = $coincident(self);
    if (!proxies.has(proxy)) {
        const isWorker = self instanceof Worker;
        const util = isWorker ? main : thread;
        proxies.set(proxy, (isWorker ? be : fe)(self, util(proxy, MAIN, THREAD), ...args));
    }
  }
  return proxies.get(proxy);
}

module.exports = coincident;
