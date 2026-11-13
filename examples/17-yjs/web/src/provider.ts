import * as Y from 'yjs';
import type { ZeBridge } from '@zebridge/client';

export const TABLE = 'yjs_updates';
type Client = Pick<ZeBridge, 'query' | 'mutateWithId' | 'onChange' | 'onVerdict'>;

// No Yjs server: ZeBridge persists opaque, immutable update rows. This adapter
// interprets their bytes and suppresses the echo of updates loaded from SQLite.
export class ZeBridgeYjsProvider {
  readonly doc = new Y.Doc();
  readonly text = this.doc.getText('content');
  private seen = new Set<string>();
  private work: Promise<void> = Promise.resolve();
  private unsubscribe: (() => void)[] = [];
  private stopped = false;
  private failure: Error | null = null;
  private client: Client;
  readonly documentId: string;
  private tenantId: string;
  private onError: (error: Error) => void;

  constructor(
    client: Client,
    documentId: string,
    tenantId: string,
    onError: (error: Error) => void,
  ) {
    this.client = client;
    this.documentId = documentId;
    this.tenantId = tenantId;
    this.onError = onError;
  }

  async start(): Promise<void> {
    if (this.unsubscribe.length) throw new Error('Provider already started');
    this.unsubscribe.push(this.client.onChange(TABLE, () => this.refresh()));
    this.unsubscribe.push(this.client.onVerdict((v) => {
      if (v.table !== TABLE || !this.seen.has(String(v.key.id))) return;
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
      await this.client.mutateWithId(TABLE, 'INSERT', { id }, {
        id, document_id: this.documentId, tenant_id: this.tenantId, update_data: bytes,
      }, { independent: true });
    });
  };

  refresh(): void {
    this.enqueue(async () => {
      const rows = await this.client.query(
        `SELECT id, update_data FROM yjs_updates WHERE document_id = ? AND tenant_id = ? AND deleted_at IS NULL`,
        this.documentId, this.tenantId,
      );
      for (const row of rows) {
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
