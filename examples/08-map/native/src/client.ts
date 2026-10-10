/// The client for this app: what is about the APP — the bridge, the invite, which tables —
/// handed to libzb, the C client, through zb-react-native's Expo module. The first run
/// enrolls with the invite; libzb keeps the identity beside the replica, and later runs
/// need neither. libzb moves only when called: a loop polls (CDC, verdicts, the JWT's
/// renewal) and flushes the outbox; a command from the screen ends the poll's wait.
import * as FileSystem from 'expo-file-system/legacy';
import { Libzb } from '@zebridge/react-native';

// Expo inlines EXPO_PUBLIC_* at bundle time.
export const BRIDGE_URL = process.env.EXPO_PUBLIC_ZB_BRIDGE_URL ?? 'https://bridge.zebridge.eu';
/// Another NATS address than the one the bridge names, such as a leaf.
export const NATS_URL = process.env.EXPO_PUBLIC_ZB_NATS_URL || undefined;
const INVITE = process.env.EXPO_PUBLIC_ZB_INVITE || undefined;
/// The tenant whose map services answer (`query.<tenant>.chargers_near`).
export const TENANT = process.env.EXPO_PUBLIC_ZB_TENANT ?? '_default';

/// One replica and identity per bridge, and per NATS server when one is named (a leaf).
const host = (u: string) => u.replace(/^[a-z]+:\/\//, '').split(/[:/]/)[0];
const DB_FILE = `zemap-${host(BRIDGE_URL)}${NATS_URL ? `-${host(NATS_URL)}` : ''}.sqlite3`;

export type Row = Record<string, any>;

export class MapClient {
  principal = '';
  private zb: Libzb | null = null;
  private listeners = new Map<string, (() => void)[]>();
  private running = false;

  async connect() {
    // An absolute path: libzb keeps the replica and, beside it, the identity.
    const dir = (FileSystem.documentDirectory ?? '').replace(/^file:\/\//, '');
    this.zb = await Libzb.connect({
      bridgeUrl: BRIDGE_URL,
      invite: INVITE,
      natsUrl: NATS_URL,
      dbPath: `${dir}${DB_FILE}`,
      // §10ic: the markers are the ANSWER, never a stored table. `charge_points` stays
      // DECLARED so a mutation has its descriptor; only `routes` is followed.
      tables: ['routes'],
      ondemandTables: ['charge_points'],
      heartbeatMs: 0,
      // §10ix: seed large tables as they arrive, a window at a time — bounded memory.
      seedStreaming: true,
    });
    const r = await this.zb.sync();
    if (r.error) throw new Error(r.detail ?? r.error);
    this.principal = r.principal;
    this.running = true;
    void this.loop();
  }

  private async loop() {
    while (this.running && this.zb) {
      try {
        const r = await this.zb.poll(500);
        for (const t of r.changed_tables ?? []) this.notify(t);
        await this.zb.flush(0);
      } catch {
        await new Promise((ok) => setTimeout(ok, 1000)); // the connection is down: retry
      }
    }
  }

  private notify(table: string) {
    for (const cb of this.listeners.get(table) ?? []) cb();
  }

  /// Called after `table` changed in the replica: a write here, or anyone's.
  onChange(table: string, cb: () => void) {
    this.listeners.set(table, [...(this.listeners.get(table) ?? []), cb]);
  }

  async query(sql: string, params: unknown[] = []): Promise<Row[]> {
    const r: any = await this.zb!.query(sql, params);
    if (r.error) throw new Error(r.detail ?? r.error);
    return r.rows.map((row: any[]) => Object.fromEntries(r.columns.map((c: string, i: number) => [c, row[i]])));
  }

  async request(subject: string, payload: unknown, timeoutMs: number) {
    const a = await this.zb!.request(subject, payload, timeoutMs);
    if (a?.error && !a.columns) throw new Error(a.detail ?? a.error);
    return a;
  }

  async mutate(table: string, op: 'INSERT' | 'UPDATE' | 'DELETE', key: Row, values: Row) {
    const r = await this.zb!.mutate(table, op, key, values);
    if (r?.error) throw new Error(r.detail ?? r.error);
    this.notify(table); // applied locally at once
  }

  /// A register stamp on the bridge's clock.
  stamp() {
    return this.zb!.stamp();
  }

  close() {
    this.running = false;
    void this.zb?.close();
  }
}
