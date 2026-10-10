/// The few calls the screen makes, over libzb (the C client, through zb-react-native): it
/// enrolls with the invite flow, keeps the same registers as the web page and the Flutter
/// app, and talks to the same bridge.
import * as FileSystem from 'expo-file-system/legacy';
import { Libzb } from '@zebridge/react-native';

export type Row = Record<string, any>;

export interface AirportsClient {
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

export function makeClient(s: Settings): AirportsClient {
  return new LibzbClient(s);
}

/// libzb moves only when called: a loop polls (CDC, verdicts, the JWT's renewal) and flushes
/// the outbox; a command from the screen ends the poll's wait (the module wakes it).
class LibzbClient implements AirportsClient {
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
    this.listeners.forEach((cb) => cb()); // applied locally at once
  }
  stamp() { return this.zb!.stamp(); }
  onFlights(cb: () => void) { this.listeners.push(cb); }
  close() { this.running = false; void this.zb?.close(); }
}
