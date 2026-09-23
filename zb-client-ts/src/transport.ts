/// The transport seam (NOTES §10s) — the second of the core's two walls, next
/// to storage.ts. The sans-I/O core decides; the shell executes through THIS.
/// A port maps it to its own client (Zig → nats.zig); a test injects a mock and
/// drives the whole client without a server. Handles (connections, consumers,
/// KV and object stores) stay structural: the seam's job is factory injection,
/// not retyping a wire library.
import { wsconnect, headers, credsAuthenticator } from '@nats-io/nats-core';
import { jetstream, jetstreamManager } from '@nats-io/jetstream';
import { Kvm } from '@nats-io/kv';
import { Objm } from '@nats-io/obj';

/// What the core-shell needs from a live connection — the dial may return any
/// object honouring this shape (structural, like the storage Exec).
export interface JetStreamOpts {
  domain?: string;
}

export interface TransportConnection {
  close(): Promise<void>;
  status(): AsyncIterable<unknown>;
  /// `opts.queue` puts this subscription in a QUEUE GROUP: one member of the group
  /// gets each message, which is how a responder scales and fails over (§10hp).
  subscribe(subject: string, opts?: { queue?: string }): AsyncIterable<any>;
  /// §10hn: request/reply — the on-demand `request` (a `query.<tenant>.<name>` ask).
  request(subject: string, data: Uint8Array, opts?: { timeout?: number }): Promise<{ data: Uint8Array }>;
  rtt(): Promise<number>;
}

/// NATS wire constants, spelled once. These are protocol tokens, identical in
/// every client library (nats.js, nats.zig, nats.py) — the seam carries them so
/// no @nats-io import leaks into libzb.
export const DELIVER_POLICY = {
  all: 'all',
  byStartSequence: 'by_start_sequence',
  lastPerSubject: 'last_per_subject',
} as const;

export interface Transport {
  /// The dial. `config.connect` still overrides just this (the Node adapter's
  /// TCP dial); a full `config.transport` replaces everything.
  connect(opts: Record<string, unknown>): Promise<any>;
  credsAuthenticator(creds: Uint8Array): unknown;
  headers(): any;
  /// `js.domain`: the JetStream domain to address — `$JS.<domain>.API.` instead of
  /// `$JS.API.` — when JetStream is reached across a leaf link. Absent is the
  /// server's own JetStream. Every factory takes it, since every one of them talks
  /// to the API (a KV or object store is a stream underneath).
  jetstream(nc: any, js?: JetStreamOpts): any;
  jetstreamManager(nc: any, js?: JetStreamOpts): Promise<any>;
  kv(nc: any, bucket: string, opts?: Record<string, unknown>, js?: JetStreamOpts): Promise<any>;
  objectStore(nc: any, bucket: string, js?: JetStreamOpts): Promise<any>;
  /// §10hq: the answer bucket of a tenant, created on first use with a `max_age` so
  /// the answers in it expire on their own. Opening an existing one is not an error.
  objectStoreCreate(nc: any, bucket: string, opts: { max_age_ns: number }, js?: JetStreamOpts): Promise<any>;
  deliverPolicy: typeof DELIVER_POLICY;
}

export const natsTransport: Transport = {
  connect: (opts) => wsconnect(opts as any),
  credsAuthenticator: (creds) => credsAuthenticator(creds),
  headers: () => headers(),
  jetstream: (nc, js) => jetstream(nc, js),
  jetstreamManager: (nc, js) => jetstreamManager(nc, js),
  // Kvm and Objm take a connection OR a JetStream client; handing them the client
  // built with the domain is how the domain reaches the bucket's own API calls.
  kv: (nc, bucket, opts, js) => new Kvm(jetstream(nc, js)).open(bucket, opts as any),
  objectStore: (nc, bucket, js) => new Objm(jetstream(nc, js)).open(bucket),
  objectStoreCreate: (nc, bucket, opts, js) => new Objm(jetstream(nc, js)).create(bucket, { max_age: opts.max_age_ns, description: 'ZeBridge answers (§10hq)' } as any),
  deliverPolicy: DELIVER_POLICY,
};
