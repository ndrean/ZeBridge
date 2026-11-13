import * as Y from 'yjs';
import type { ZeBridge } from '@zebridge/client';

export const TABLE = 'yjs_updates';
// A merged row must fit ZeBridge's row slot (4 KB by default, BASE_BUF): the width guard refuses a
// wider one. Inputs are chosen so their sum stays under this; a Yjs merge is no larger than its inputs.
export const COMPACT_INPUT_BYTES = 3000;
export const COMPACT_MAX_INPUTS = 256; // yjs_updates_replace()'s bound
export type CompactResult =
  | { compacted: false; reason: string }
  | { compacted: true; id: string; replaced: number; bytesBefore: number; bytesAfter: number };
type Client = Pick<ZeBridge, 'query' | 'mutateWithId' | 'onChange' | 'onVerdict'>;

// No Yjs server: ZeBridge persists opaque, immutable update rows. This adapter
// interprets their bytes and suppresses the echo of updates loaded from SQLite.
export class ZeBridgeYjsProvider {
  readonly doc = new Y.Doc();
  readonly text = this.doc.getText('content');
  private seen = new Set<string>();
  // This session's uploads awaiting a verdict. `seen` alone misses one: a refresh rebuilds
  // it from the replica, and a rejected row is reverted locally before its verdict. An
  // upload from before a reload is still in the replica, so `seen` covers that one.
  private mine = new Set<string>();
  // Compactions awaiting a verdict. A refused one loses nothing (its inputs stay active and the
  // Y.Doc already holds their content), so it is reported, never fatal. Rebuilt from the replica's
  // pending rows (`replaces` set) after a reload.
  private compactions = new Set<string>();
  private work: Promise<void> = Promise.resolve();
  private unsubscribe: (() => void)[] = [];
  private stopped = false;
  private failure: Error | null = null;
  private client: Client;
  readonly documentId: string;
  private tenantId: string;
  private onError: (error: Error) => void;
  private onCompactionRefused: (detail: string) => void;

  constructor(
    client: Client,
    documentId: string,
    tenantId: string,
    onError: (error: Error) => void,
    onCompactionRefused: (detail: string) => void = () => {},
  ) {
    this.client = client;
    this.documentId = documentId;
    this.tenantId = tenantId;
    this.onError = onError;
    this.onCompactionRefused = onCompactionRefused;
  }

  async start(): Promise<void> {
    if (this.unsubscribe.length) throw new Error('Provider already started');
    this.unsubscribe.push(this.client.onChange(TABLE, () => this.refresh()));
    this.unsubscribe.push(this.client.onVerdict((v) => {
      if (v.table !== TABLE) return;
      const id = String(v.key.id);
      if (this.compactions.delete(id)) {
        if (v.outcome !== 'applied' && v.outcome !== 'reconciled') {
          this.onCompactionRefused(`${v.outcome}. ${v.detail ?? v.reason ?? ''}`.trim());
        }
        return;
      }
      if (!this.mine.delete(id) && !this.seen.has(id)) return;
      if (v.outcome !== 'applied' && v.outcome !== 'reconciled') {
        this.fail(new Error(`Update ${v.key.id}: ${v.outcome}. ${v.detail ?? v.reason ?? ''} ` +
          'Copy your text before reloading; rejected CRDT edits cannot simply be undone.'));
      }
    }));
    this.refresh();
    await this.flush();
    this.doc.on('update', this.save);
  }

  private fail(error: unknown): void {
    if (this.failure || this.stopped) return;
    this.failure = error instanceof Error ? error : new Error(String(error));
    this.onError(this.failure);
  }

  private enqueue(fn: () => Promise<void>): void {
    this.work = this.work.then(async () => {
      if (!this.stopped && !this.failure) await fn();
    }).catch((e: unknown) => this.fail(e));
  }

  private save = (update: Uint8Array, origin: unknown): void => {
    if (origin === this || this.stopped || this.failure) return;
    const id = crypto.randomUUID();
    const bytes = update.slice();
    this.enqueue(async () => {
      // mutateWithId rejects if nothing was durably queued. INSERT includes the
      // primary key in values too: PostgreSQL must store this exact update ID.
      this.seen.add(id);
      this.mine.add(id);
      await this.client.mutateWithId(TABLE, 'INSERT', { id }, {
        id, document_id: this.documentId, tenant_id: this.tenantId, update_data: bytes,
      }, { independent: true });
    });
  };

  refresh(): void {
    this.enqueue(async () => {
      const rows = await this.client.query(
        `SELECT id, update_data, replaces FROM yjs_updates WHERE document_id = ? AND tenant_id = ? AND deleted_at IS NULL`,
        this.documentId, this.tenantId,
      );
      for (const row of rows) {
        // Stored rows have NULL here; only this device's unconfirmed compaction carries the list.
        if (row.replaces != null) this.compactions.add(row.id);
        if (this.seen.has(row.id)) continue;
        if (!(row.update_data instanceof Uint8Array)) throw new Error('Expected a binary Yjs update');
        Y.applyUpdate(this.doc, row.update_data, this);
        this.seen.add(row.id);
      }
      // Compaction retires old row IDs. Their CRDT information stays in the Y.Doc,
      // but only currently active row IDs need to remain in this delivery cache.
      this.seen = new Set(rows.map((row) => row.id));
    });
  }

  // Merge confirmed updates of this document into one row that replaces them. Unconfirmed edits
  // (still in the outbox or awaiting their echo) are left out: PostgreSQL may not hold them yet.
  // Returns once the replacement is durably queued; its verdict arrives later.
  async compact(): Promise<CompactResult> {
    let result: CompactResult = { compacted: false, reason: 'provider stopped' };
    this.enqueue(async () => { result = await this.compactNow(); });
    await this.flush();
    return result;
  }

  private async compactNow(): Promise<CompactResult> {
    // shortcut: reads the client's private ledgers (_zebridge_outbox, _zebridge_undo) to tell a
    // confirmed row from an unconfirmed one; replace with a public client call if one is added.
    const rows = await this.client.query(
      `SELECT id, update_data FROM yjs_updates
       WHERE document_id = ? AND tenant_id = ? AND deleted_at IS NULL AND replaces IS NULL
         AND id NOT IN (SELECT row_id FROM _zebridge_outbox WHERE tbl = ?)
         AND id NOT IN (SELECT row_id FROM _zebridge_undo WHERE tbl = ?)
       ORDER BY id`,
      this.documentId, this.tenantId, TABLE, TABLE,
    );
    const inputs: { id: string; update_data: Uint8Array }[] = [];
    let bytesBefore = 0;
    for (const row of rows) {
      if (inputs.length === COMPACT_MAX_INPUTS) break;
      if (bytesBefore + row.update_data.byteLength > COMPACT_INPUT_BYTES) continue;
      inputs.push(row); bytesBefore += row.update_data.byteLength;
    }
    if (inputs.length < 2) {
      return { compacted: false, reason: rows.length < 2 ? 'fewer than two confirmed updates' : 'no two confirmed updates fit one row' };
    }
    const merged = Y.mergeUpdates(inputs.map((row) => row.update_data));
    if (merged.byteLength > COMPACT_INPUT_BYTES) return { compacted: false, reason: 'merged update too large for one row' };
    const id = crypto.randomUUID();
    this.seen.add(id);
    this.compactions.add(id);
    await this.client.mutateWithId(TABLE, 'INSERT', { id }, {
      id, document_id: this.documentId, tenant_id: this.tenantId, update_data: merged,
      replaces: JSON.stringify(inputs.map((row) => row.id)),
    }, { independent: true });
    return { compacted: true, id, replaced: inputs.length, bytesBefore, bytesAfter: merged.byteLength };
  }

  // Await local durability, not server acceptance. Used before hanging up or
  // navigating away; ZeBridge's outbox owns retransmission after this point.
  async flush(): Promise<void> {
    let current: Promise<void>;
    do { current = this.work; await current; } while (current !== this.work);
    if (this.failure) throw this.failure;
  }

  destroy(): void {
    if (this.stopped) return;
    this.stopped = true;
    for (const off of this.unsubscribe.splice(0)) off();
    this.doc.off('update', this.save);
    this.doc.destroy();
  }
}
