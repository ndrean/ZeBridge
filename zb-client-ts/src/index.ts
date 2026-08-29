/// zb-client-ts — the ZeBridge client library (NOTES.md §10, extraction step 2).
/// One core: schema watch, chain-first seeding (snapshot fallback), CDC apply,
/// the §7 write path (outbox, optimistic apply, verdicts, echo-confirm).
/// Consumers: web-consumer (browser, #1), Node microservice (#2, forces the
/// storage and transport seams).
export * from './core.ts';
export * from './transport.ts';
export * from './libzb.ts';
export * from './dialect.ts';
// The PGlite adapter is NOT re-exported here: importing it pulls the WASM engine
// into every consumer. It lives at `zb-client-ts/pglite`, opt-in per host.
