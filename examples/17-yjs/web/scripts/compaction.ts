import { randomUUID } from 'node:crypto';
import type { Client } from 'pg';
import * as Y from 'yjs';

// Use a dedicated connection as the table owner, not the bridge's writer role.
// A normal merged update is the checkpoint; clients need no new protocol or flag.
export async function compactDocument(client: Client, tenant: string, documentId: string, minUpdates = 1000) {
  if (!Number.isSafeInteger(minUpdates) || minUpdates < 2) throw new Error('min-updates must be an integer >= 2');
  await client.query('BEGIN');
  try {
    await client.query("SET LOCAL lock_timeout = '5s'");
    // Serialize compactors of this document. Ordinary INSERTs take no such lock
    // and keep flowing while the selected update set is merged.
    await client.query('SELECT pg_advisory_xact_lock(hashtextextended($1, 0))',
      [JSON.stringify(['yjs_updates', tenant, documentId])]);
    const { rows } = await client.query<{ id: string; update_data: Buffer }>(
      `SELECT id, update_data FROM public.yjs_updates
       WHERE tenant_id = $1 AND document_id = $2 AND deleted_at IS NULL`, [tenant, documentId]);
    if (rows.length < minUpdates) {
      await client.query('COMMIT');
      return { tenant, documentId, compacted: false, updates: rows.length };
    }
    // Binary merging preserves even operations whose causal predecessors have
    // not arrived yet. Rebuilding from visible text would lose that information.
    const merged = Y.mergeUpdates(rows.map((row) => row.update_data));
    const id = randomUUID();
    await client.query(`INSERT INTO public.yjs_updates (id, tenant_id, document_id, update_data, last_writer)
      VALUES ($1, $2, $3, $4, 'yjs-compactor')`, [id, tenant, documentId, Buffer.from(merged)]);
    const retired = await client.query(`UPDATE public.yjs_updates SET deleted_at = clock_timestamp()
      WHERE tenant_id = $1 AND document_id = $2 AND id = ANY($3::uuid[]) AND deleted_at IS NULL`,
    [tenant, documentId, rows.map((row) => row.id)]);
    if (retired.rowCount !== rows.length) throw new Error('Compaction inputs changed; rolling back');
    await client.query('COMMIT');
    return { tenant, documentId, compacted: true, replacement: id, retired: rows.length,
      bytesBefore: rows.reduce((n, row) => n + row.update_data.byteLength, 0), bytesAfter: merged.byteLength };
  } catch (e) {
    await client.query('ROLLBACK');
    throw e;
  }
}
