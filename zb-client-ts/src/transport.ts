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
import { createUser as nkeysCreateUser, fromSeed as nkeysFromSeed } from '@nats-io/nkeys';

/// What the core-shell needs from a live connection — the dial may return any
/// object honouring this shape (structural, like the storage Exec).
/// The nkey pair a client enrols with: `publicKey` is what `GET /enroll` is asked for,
/// `seed` is the private half the host keeps. libzb's `zb_create_user()` returns the
/// same two fields.
export interface UserKeyPair {
  publicKey: string;
  seed: string;
}

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
  /// The creds bytes, or a function read at EVERY handshake (§10jt: a renewed JWT
  /// reaches the next reconnect without rebuilding the connection).
  credsAuthenticator(creds: Uint8Array | (() => Uint8Array)): unknown;
  headers(): any;
  /// Generate an enrolment key pair. On the seam because the key format is NATS's,
  /// not ZeBridge's — and because a port may have to reach its platform's own crypto
  /// (React Native has no WebCrypto until a shim provides one, §10hs).
  createUser(): UserKeyPair;
  /// §10jt: sign `data` with an nkey seed; the seed's public key comes back with it
  /// (renewal proves possession of the key the device enrolled with).
  nkeySign(seed: string, data: Uint8Array): { publicKey: string; signature: Uint8Array };
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
  nkeySign: (seed, data) => {
    const kp = nkeysFromSeed(new TextEncoder().encode(seed));
    try { return { publicKey: kp.getPublicKey(), signature: kp.sign(data) }; } finally { kp.clear(); }
  },
  createUser: () => {
    const kp = nkeysCreateUser();
    return { publicKey: kp.getPublicKey(), seed: new TextDecoder().decode(kp.getSeed()) };
  },
  deliverPolicy: DELIVER_POLICY,
};
