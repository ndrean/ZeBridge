import { NativeModules, DeviceEventEmitter } from 'react-native';

const { ZeBridgeNative } = NativeModules;

export interface PollReport {
  applied: number;
  settled: number;
  changedTables: string[];
  seeded: string[];
}

/**
 * JS Wrapper around the libzb C ABI, communicating via a React Native TurboModule or NativeModule.
 */
export class ZeBridge {
  private handle: number = 0;
  public tenant: string = '';

  constructor(options: {
    url: string;
    dbPath: string;
    principal: string;
    tables: string[];
    clientId?: string;
  }) {
    if (!ZeBridgeNative) {
      console.warn("ZeBridgeNative module not found! Using mock for UI development.");
      this.handle = -1;
      return;
    }
    this.handle = ZeBridgeNative.open(JSON.stringify(options));
    if (!this.handle) throw new Error("Failed to open ZeBridge client");
  }

  public sync(): { tenant: string; first: boolean } {
    if (this.handle === -1) return { tenant: 'mock-tenant', first: true };
    const res = ZeBridgeNative.sync(this.handle);
    const decoded = JSON.parse(res);
    if (decoded.error) throw new Error(decoded.error);
    this.tenant = decoded.tenant;
    return decoded;
  }

  public query(sql: string, params: any[] = []): any[] {
    if (this.handle === -1) return []; // Mock
    const res = ZeBridgeNative.query(this.handle, sql, JSON.stringify(params));
    const decoded = JSON.parse(res);
    if (decoded.error) throw new Error(decoded.error);
    
    if (decoded.columns && decoded.rows) {
      return decoded.rows.map((row: any[]) => {
        const map: any = {};
        decoded.columns.forEach((col: string, i: number) => {
          map[col] = row[i];
        });
        return map;
      });
    }
    return [];
  }

  public mutate(table: string, op: 'INSERT' | 'UPDATE' | 'DELETE', key: any, values?: any): void {
    if (this.handle === -1) return; // Mock
    const res = ZeBridgeNative.mutate(
      this.handle,
      table,
      op,
      JSON.stringify(key),
      values ? JSON.stringify(values) : ""
    );
    const decoded = JSON.parse(res);
    if (decoded.error) throw new Error(decoded.error);
  }

  /**
   * Polls for new changes. This should ideally be called in a background thread by the Native Module,
   * but can be exposed as an async function if the native module wraps it in a Promise.
   */
  public async poll(waitMs: number): Promise<PollReport> {
    if (this.handle === -1) return { applied: 0, settled: 0, changedTables: [], seeded: [] };
    const res = await ZeBridgeNative.pollAsync(this.handle, waitMs);
    const decoded = JSON.parse(res);
    if (decoded.error) throw new Error(decoded.error);
    
    return {
      applied: decoded.applied || 0,
      settled: decoded.settled || 0,
      changedTables: decoded.changed_tables || [],
      seeded: decoded.seeded || []
    };
  }

  public async flush(waitMs: number): Promise<any> {
    if (this.handle === -1) return {};
    const res = await ZeBridgeNative.flushAsync(this.handle, waitMs);
    const decoded = JSON.parse(res);
    if (decoded.error) throw new Error(decoded.error);
    return decoded;
  }

  public close(): void {
    if (this.handle === -1) return;
    ZeBridgeNative.close(this.handle);
    this.handle = 0;
  }
}
