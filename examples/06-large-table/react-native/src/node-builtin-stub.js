// §10ij: a stand-in for the Node built-ins React Native does not have.
//
// `@nats-io/obj` picks its SHA-256 backend at run time: the default is `"js"`, a pure-JS
// implementation that is browser-safe and the one this app uses. Only
// `setSha256Backend("native")` reaches for Node's crypto, and it does so through
// `await import("node:crypto")` — which Metro resolves STATICALLY, so the bundle fails
// on a branch that never executes.
//
// Mapping the specifier here satisfies the bundler. It throws rather than returning a
// silent no-op: if some future code path really does want the native backend, it should
// fail loudly rather than hash nothing.
const missing = (name) => {
  throw new Error(
    `node:${name} is not available in React Native — this import should be unreachable. ` +
      'If a library now needs it for real, give it a proper shim rather than this stub.',
  );
};

module.exports = new Proxy(
  {},
  { get: (_t, prop) => (prop === '__esModule' ? true : () => missing(String(prop))) },
);
