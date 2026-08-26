'use strict';
const main = require('../shared/main.js');

module.exports = main(
  false,
  {
    server: globalThis,
    isServerProxy: () => false
  }
);
