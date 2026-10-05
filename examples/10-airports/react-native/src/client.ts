/// The few calls the screen makes, over either client library: libzb (the C client,
/// through zb-react-native) when the app was built with it, else zb-client-ts (TypeScript).
/// EXPO_PUBLIC_ZB_ENGINE=ts forces zb-client-ts, to compare the two on one phone. Both enroll
/// with the same invite flow, keep the same registers and talk to the same bridge.
import * as FileSystem from 'expo-file-system';
import { ZeBridge } from 'zb-client-ts';
import { Libzb, libzbAvailable } from 'zb-react-native';

export type Row = Record<string, any>;

export interface AirportsClient {
  readonly engine: 'zb-client-ts' | 'libzb';
  principal: string;
  tenant: string;
  /// Enrolls on the first run (the invite), then connects and syncs `flights`.
  connect(): Promise<void>;
  request(subject: string, payload: unknown): Promise<any>;
  query(sql: string, params?: unknown[]): Promise<Row[]>;
  mutate(table: string, op: 'INSERT' | 'UPDATE', key: Row, values: Row): Promise<void>;
  stamp(): Promise<string>;
  /// Called after the replica's `flights` changed (a write here, or anyone's).
  onFlights(cb: () => void): void;
  close(): void;
}

export type Settings = { bridgeUrl: string; invite?: string; natsUrl?: string; dbName: string };

declare const process: { env: Record<string, string | undefined> };

export function makeClient(s: Settings): AirportsClient {
  return libzbAvailable && process.env.EXPO_PUBLIC_ZB_ENGINE !== 'ts' ? new LibzbClient(s) : new TsClient(s);
}

class TsClient implements AirportsClient {
  readonly engine = 'zb-client-ts' as const;
  private zb: ZeBridge;
  constructor(s: Settings) {
    this.zb = new ZeBridge({ bridgeUrl: s.bridgeUrl, invite: s.invite, natsUrl: s.natsUrl, dbPath: s.dbName, tables: ['flights'] });
  }
  get principal() { return this.zb.principal ?? ''; }
  get tenant() { return this.zb.tenant ?? ''; }
  async connect() { await this.zb.connect(); }
  request(subject: string, payload: unknown) { return this.zb.request(subject, payload as any); }
  query(sql: string, params: unknown[] = []) { return this.zb.query(sql, ...(params as any[])); }
  async mutate(table: string, op: 'INSERT' | 'UPDATE', key: Row, values: Row) { await this.zb.mutate(table, op, key, values); }
  async stamp() { return this.zb.stamp(); }
  onFlights(cb: () => void) { this.zb.onChange('flights', cb); }
  close() { void this.zb.close(); }
}

/// libzb moves only when called: a loop polls (CDC, verdicts, the JWT's renewal) and flushes
/// the outbox; a command from the screen ends the poll's wait (the module wakes it).
class LibzbClient implements AirportsClient {
  readonly engine = 'libzb' as const;
  principal = '';
  tenant = '';
  private zb: Libzb | null = null;
  private listeners: (() => void)[] = [];
  private running = false;
  constructor(private s: Settings) {}

  async connect() {
    // An absolute path: libzb keeps the replica and, beside it, the identity.
    const dir = (FileSystem.documentDirectory ?? '').replace(/^file:\/\//, '');
    this.zb = await Libzb.connect({
      bridgeUrl: this.s.bridgeUrl, invite: this.s.invite, natsUrl: this.s.natsUrl,
      dbPath: `${dir}${this.s.dbName}`, tables: ['flights'],
    });
    const r = await this.zb.sync();
    if (r.error) throw new Error(r.error);
    this.tenant = r.tenant;
    this.principal = r.principal;
    this.running = true;
    void this.loop();
  }

  private async loop() {
    while (this.running && this.zb) {
      try {
        const r = await this.zb.poll(500);
        if ((r.changed_tables ?? []).includes('flights')) this.listeners.forEach((cb) => cb());
        await this.zb.flush(0);
      } catch {
        await new Promise((ok) => setTimeout(ok, 1000)); // the connection is down: retry
      }
    }
  }

  async request(subject: string, payload: unknown) {
    const a = await this.zb!.request(subject, payload);
    if (a?.error && !a.columns) throw new Error(a.detail ?? a.error);
    return a;
  }
  async query(sql: string, params: unknown[] = []) {
    const r = await this.zb!.query(sql, params);
    if ((r as any).error) throw new Error((r as any).detail ?? (r as any).error);
    return r.rows.map((row) => Object.fromEntries(r.columns.map((c, i) => [c, row[i]])));
  }
  async mutate(table: string, op: 'INSERT' | 'UPDATE', key: Row, values: Row) {
    const r = await this.zb!.mutate(table, op, key, values);
    if (r?.error) throw new Error(r.detail ?? r.error);
    this.listeners.forEach((cb) => cb()); // applied locally at once, as zb-client-ts does
  }
  stamp() { return this.zb!.stamp(); }
  onFlights(cb: () => void) { this.listeners.push(cb); }
  close() { this.running = false; void this.zb?.close(); }
}
