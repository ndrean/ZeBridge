'use strict';
const { target: tv, wrap } = require('proxy-target/array');

const {
  ARRAY,
  OBJECT,
  FUNCTION,
  BOOLEAN,
  NUMBER,
  STRING,
  UNDEFINED,
  BIGINT,
  SYMBOL,
  NULL
} = require('proxy-target/types');

const {
  defineProperty,
  deleteProperty,
  getOwnPropertyDescriptor,
  getPrototypeOf,
  isExtensible,
  ownKeys,
  preventExtensions,
  set,
  setPrototypeOf
} = Reflect;

const { assign, create } = Object;

const TypedArray = getPrototypeOf(Int8Array);
exports.TypedArray = TypedArray;

exports.assign = assign;
exports.create = create;
exports.defineProperty = defineProperty;
exports.deleteProperty = deleteProperty;
exports.getOwnPropertyDescriptor = getOwnPropertyDescriptor;
exports.getPrototypeOf = getPrototypeOf;
exports.isExtensible = isExtensible;
exports.ownKeys = ownKeys;
exports.preventExtensions = preventExtensions;
exports.set = set;
exports.setPrototypeOf = setPrototypeOf;

const augment = (descriptor, how) => {
  const {get, set, value} = descriptor;
  if (get) descriptor.get = how(get);
  if (set) descriptor.set = how(set);
  if (value) descriptor.value = how(value);
  return descriptor;
};
exports.augment = augment;

const asEntry = transform => value => wrap(value, (type, value) => {
  switch (type) {
    case NULL:
      return tv(NULL, value);
    case OBJECT:
      if (value === globalThis)
        return tv(type, null);
    case ARRAY:
    case FUNCTION:
      return transform(type, value);
    case BOOLEAN:
    case NUMBER:
    case STRING:
    case UNDEFINED:
    case BIGINT:
      return tv(type, value);
    case SYMBOL: {
      // handle known symbols
      if (symbols.has(value))
        return tv(type, symbols.get(value));
      // handle `Symbol.for('...')` cases
      let key = Symbol.keyFor(value);
      if (key)
        return tv(type, `.${key}`);
    }
  }
  throw new TypeError(`Unable to handle this ${type}: ${String(value)}`);
});
exports.asEntry = asEntry;

const symbols = new Map(
  ownKeys(Symbol)
    .filter(s => typeof Symbol[s] === SYMBOL)
    .map(s => [Symbol[s], s])
);

const symbol = value => {
  if (value.startsWith('.'))
    return Symbol.for(value.slice(1));
  for (const [symbol, name] of symbols) {
    if (name === value)
      return symbol;
  }
};
exports.symbol = symbol;

const transform = o => o;
exports.transform = transform;
