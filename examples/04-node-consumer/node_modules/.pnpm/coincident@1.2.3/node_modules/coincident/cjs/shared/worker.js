'use strict';
const {FUNCTION} = require('proxy-target/types');
module.exports = typeof Worker === FUNCTION ? Worker : class {};
