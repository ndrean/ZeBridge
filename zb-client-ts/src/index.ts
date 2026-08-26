/// zb-client-ts — the ZeBridge client library (NOTES.md §10, extraction step 2).
/// One core: schema watch, chain-first seeding (snapshot fallback), CDC apply,
/// the §7 write path (outbox, optimistic apply, verdicts, echo-confirm).
/// Consumers: web-consumer (browser, #1), Node microservice (#2, forces the
/// storage and transport seams).
export * from './libzb.ts';
