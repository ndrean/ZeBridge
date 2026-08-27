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
export interface TransportConnection {
  close(): Promise<void>;
  status(): AsyncIterable<unknown>;
  subscribe(subject: string): AsyncIterable<any>;
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
  jetstream(nc: any): any;
  jetstreamManager(nc: any): Promise<any>;
  kv(nc: any, bucket: string, opts?: Record<string, unknown>): Promise<any>;
  objectStore(nc: any, bucket: string): Promise<any>;
  deliverPolicy: typeof DELIVER_POLICY;
}

export const natsTransport: Transport = {
  connect: (opts) => wsconnect(opts as any),
  credsAuthenticator: (creds) => credsAuthenticator(creds),
  headers: () => headers(),
  jetstream: (nc) => jetstream(nc),
  jetstreamManager: (nc) => jetstreamManager(nc),
  kv: (nc, bucket, opts) => new Kvm(nc).open(bucket, opts as any),
  objectStore: (nc, bucket) => new Objm(nc).open(bucket),
  deliverPolicy: DELIVER_POLICY,
};
