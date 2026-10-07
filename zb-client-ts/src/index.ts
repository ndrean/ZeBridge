/// zb-client-ts — the ZeBridge client library (NOTES.md §10, extraction step 2).
/// One core: schema watch, chain-first seeding (snapshot fallback), CDC apply,
/// the §7 write path (outbox, optimistic apply, verdicts, echo-confirm).
/// Consumers: web-consumer (browser, #1), Node microservice (#2, forces the
/// storage and transport seams).
export * from './core.ts';
export { loadCore, scopeSeeding, caughtUpPosition, type CoreSource, type StreamGap } from './wasm-core.ts';
export * from './transport.ts';
export * from './libzb.ts';
export * from './dialect.ts';
export type { Platform, PlatformName } from './platform.ts';
// The platform itself (storage, zstd, transport) comes from the entry the bundler
// picks — entry-node.ts or entry-browser.ts — never from here.
