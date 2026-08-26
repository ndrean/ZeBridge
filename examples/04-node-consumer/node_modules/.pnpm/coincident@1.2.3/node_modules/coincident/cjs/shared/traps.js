'use strict';
(m => Object.keys(m).map(k => k !== 'default' && (exports[k] = m[k])))
(require('proxy-target/traps'));
const DELETE = 'delete';
exports.DELETE = DELETE;
