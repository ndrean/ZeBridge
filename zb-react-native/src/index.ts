/// libzb for React Native: the ZeBridge C client (Zig inside) behind an Expo module. The
/// same option names as zb-client-ts (CLIENTS.md); every call is one C call on the
/// module's own queue, never the JS thread.
///
///   const zb = await Libzb.connect({ natsUrl, creds, principal, tables: ['orders'] });
///   await zb.sync();                         // schema, seed, positions
///   const { rows } = await zb.query('SELECT count(*) FROM orders');
///
/// ⚠️ The library in the app is a COPY, built by scripts/build-ios.sh. The first call
/// compares its ABI with this code's (src/abi.ts): a copy older or newer than the code is
/// refused with the fix, not left to misbehave — an app once kept a libzb built before
/// `creds` existed, and it sat in connect for minutes (NOTES §10ja).
import { requireNativeModule } from 'expo';
import { ZB_ABI } from './abi';

type Native = {
  abiVersion(): number;
  connect(optsJson: string): Promise<string>;
  sync(handle: string): Promise<string>;
  poll(handle: string, waitMs: number): Promise<string>;
  query(handle: string, sql: string, paramsJson: string): Promise<string>;
  close(handle: string): Promise<number>;
  zstdNew(): number;
  zstdPush(id: number, chunk: Uint8Array): number;
  zstdTake(id: number, dest: Uint8Array): void;
  zstdFree(id: number): void;
};

export const ZbNative = requireNativeModule<Native>('ZbNative');

let abiChecked = false;
/// Throws when the embedded libzb is not the ABI this package was written for.
export function assertAbi(): void {
  if (abiChecked) return;
  const got = ZbNative.abiVersion();
  if (got !== ZB_ABI) {
    throw new Error(`zb-react-native: the embedded libzb is ABI ${got}, this package is ABI ${ZB_ABI} — rebuild it (zb-react-native/scripts/build-ios.sh), then the app`);
  }
  abiChecked = true;
}

/// libzb's connect options — the names zb-client-ts uses too.
export type LibzbOptions = {
  natsUrl: string;
  principal: string;
  /// The .creds text (what /enroll returns), or `credsPath` to a file.
  creds?: string;
  credsPath?: string;
  tables?: string[] | '*';
  ondemandTables?: string[];
  /// Default `zebridge_<principal>.sqlite3` (a relative path lands in the app's
  /// working directory: pass an absolute one, e.g. under FileSystem.documentDirectory).
  dbPath?: string;
  clientId?: string;
  engine?: 'sqlite' | 'duckdb';
  grammarHash?: string;
  jsDomain?: string;
  heartbeatMs?: number;
  seedChunkRows?: number;
  seedStreaming?: boolean;
  seedStreamingAboveBytes?: number;
};

export type QueryResult = { columns: string[]; rows: any[][] };

export class Libzb {
  private constructor(private readonly handle: string) {}

  static async connect(opts: LibzbOptions): Promise<Libzb> {
    assertAbi();
    return new Libzb(await ZbNative.connect(JSON.stringify(opts)));
  }

  /// The first sync is schema, seed and positions: the table is usable after it.
  async sync(): Promise<any> {
    return JSON.parse(await ZbNative.sync(this.handle));
  }

  /// The live tail: applies what arrived, waiting up to `waitMs` for it.
  async poll(waitMs = 500): Promise<any> {
    return JSON.parse(await ZbNative.poll(this.handle, waitMs));
  }

  async query(sql: string, params: unknown[] = []): Promise<QueryResult> {
    return JSON.parse(await ZbNative.query(this.handle, sql, JSON.stringify(params)));
  }

  async close(): Promise<void> {
    await ZbNative.close(this.handle);
  }
}
