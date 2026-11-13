import { ZeBridge, NotEnrolled } from '@zebridge/client';
import { Compartment } from '@codemirror/state';
import { EditorView, keymap, lineNumbers, drawSelection } from '@codemirror/view';
import { defaultKeymap } from '@codemirror/commands';
import { yCollab, yUndoManagerKeymap } from 'y-codemirror.next';
import { ZeBridgeYjsProvider, TABLE } from './provider.ts';
import './style.css';

const qs = new URLSearchParams(location.search);
const el = (id: string) => document.getElementById(id)!;
const net = el('network') as HTMLButtonElement;
const compactButton = el('compact') as HTMLButtonElement;
const editorName = qs.get('as') ?? 'alice';
const documentId = qs.get('doc') ?? '11111111-1111-4111-8111-111111111117';
if (!/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(documentId)) {
  throw new Error('The doc URL parameter must be a UUID');
}
const deployed = import.meta.env.VITE_ZB_BRIDGE_URL as string | undefined;
const zb = new ZeBridge({
  bridgeUrl: deployed ?? `${location.origin}/bridge`,
  natsUrl: import.meta.env.VITE_ZB_NATS_URL ?? (deployed ? undefined : `${location.origin.replace(/^http/, 'ws')}/nats`),
  invite: qs.get('invite') ?? undefined,
  principal: qs.has('invite') ? undefined : editorName,
  dbPath: `yjs-${encodeURIComponent(editorName)}.sqlite3`,
  tables: [TABLE],
});

let provider: ZeBridgeYjsProvider | undefined;
let view: EditorView | undefined;
const editable = new Compartment();
let offline = false;
let fatal = false;
function fail(error: unknown): void {
  fatal = true;
  el('error').textContent = error instanceof Error ? error.message : String(error);
  view?.dispatch({ effects: editable.reconfigure(EditorView.editable.of(false)) });
}

async function stats(): Promise<void> {
  if (!provider) return;
  const rows = await zb.query(`SELECT count(*) AS n, coalesce(sum(length(update_data)), 0) AS bytes
    FROM yjs_updates WHERE document_id = ? AND tenant_id = ?`, documentId, zb.tenant);
  el('stats').textContent = `${rows[0].n} update rows · ${rows[0].bytes} payload bytes · ${await zb.pending()} pending uploads`;
}

zb.onLog((topic, data, level) => console.info(`[zb ${level}] ${topic}`, data));
zb.onStatus((s) => { el('status').textContent = offline ? 'Offline · edits stay on this device' : s; });
zb.onChange(TABLE, () => { void stats().catch(fail); });
zb.onVerdict(() => { void stats().catch(fail); });

el('document').textContent = documentId;
const next = new URL(location.href);
next.searchParams.delete('invite');
next.searchParams.set('doc', crypto.randomUUID());
(el('new-document') as HTMLAnchorElement).href = next.href;
el('download').addEventListener('click', () => {
  if (!provider) return;
  const url = URL.createObjectURL(new Blob([provider.text.toString()], { type: 'text/plain;charset=utf-8' }));
  const link = document.createElement('a');
  link.href = url; link.download = `document-${documentId}.txt`; link.click();
  setTimeout(() => URL.revokeObjectURL(url), 1000);
});

compactButton.addEventListener('click', () => {
  void (async () => {
    if (!provider) return;
    compactButton.disabled = true;
    try {
      const r = await provider.compact();
      el('status').textContent = r.compacted
        ? `Compaction queued: ${r.replaced} updates → 1 (${r.bytesBefore} → ${r.bytesAfter} bytes)`
        : `Nothing compacted: ${r.reason}`;
      await stats();
    } catch (e) { fail(e); }
    finally { compactButton.disabled = fatal; }
  })();
});

net.addEventListener('click', () => {
  void (async () => {
    net.disabled = true;
    try {
      await provider?.flush();
      if (!offline) {
        await zb.close(); offline = true;
        el('status').textContent = 'Offline · edits stay on this device';
      } else {
        await zb.connect(); offline = false;
        if (zb.tenant !== tenant) throw new Error('Tenant changed; reload before editing');
        provider?.refresh(); await provider?.flush();
        el('status').textContent = 'Connected';
      }
      net.textContent = offline ? 'Go online' : 'Go offline';
      await stats();
    } catch (e) { fail(e); }
    finally { net.disabled = fatal; }
  })();
});

let tenant = '';
try {
  el('status').textContent = 'Connecting and loading local updates…';
  await zb.connect();
  tenant = zb.tenant;
  if (!tenant) throw new Error('This example requires a tenant-scoped identity');
  provider = new ZeBridgeYjsProvider(zb, documentId, tenant, fail,
    (detail) => { el('status').textContent = `Compaction refused, nothing lost: ${detail}`; });
  await provider.start();
  view = new EditorView({
    doc: provider.text.toString(),
    extensions: [
      lineNumbers(), drawSelection(), EditorView.lineWrapping,
      keymap.of([...yUndoManagerKeymap, ...defaultKeymap]),
      yCollab(provider.text, null),
      editable.of(EditorView.editable.of(!fatal)),
      EditorView.contentAttributes.of({ 'aria-label': 'Shared document', spellcheck: 'true' }),
    ],
    parent: el('editor'),
  });
  el('who').textContent = `${editorName} · principal ${zb.principal} · tenant ${tenant}`;
  el('status').textContent = 'Connected';
  net.disabled = false;
  compactButton.disabled = false;
  await stats();
} catch (e) {
  fail(e instanceof NotEnrolled
    ? new Error('Open this editor with its own invite link: ?as=alice&invite=…') : e);
}

// Warn only while edits have not yet reached ZeBridge's durable local outbox.
let saving = false;
provider?.doc.on('update', () => {
  saving = true;
  void provider!.flush().then(() => { saving = false; return stats(); }).catch(fail);
});
window.addEventListener('beforeunload', (event) => {
  if (saving || fatal) { event.preventDefault(); event.returnValue = ''; }
});
