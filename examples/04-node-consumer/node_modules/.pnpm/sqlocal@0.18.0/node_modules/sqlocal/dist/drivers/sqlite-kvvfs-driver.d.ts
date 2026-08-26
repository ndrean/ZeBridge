import type { JsStorageDb } from '@sqlite.org/sqlite-wasm';
import type { DriverConfig, Sqlite3InitModule, SQLocalDriver } from '../types.js';
import { SQLiteMemoryDriver } from './sqlite-memory-driver.js';
/**
 * A SQLocal driver that implements the interface needed for
 * interacting with SQLite databases in localStorage or sessionStorage.
 */
export declare class SQLiteKvvfsDriver extends SQLiteMemoryDriver implements SQLocalDriver {
    readonly storageType: 'local' | 'session';
    protected db?: JsStorageDb;
    constructor(storageType: 'local' | 'session', sqlite3InitModule?: Sqlite3InitModule);
    init(config: DriverConfig): Promise<void>;
    isDatabasePersisted(): Promise<boolean>;
    getDatabaseSizeBytes(): Promise<number>;
    import(database: ArrayBuffer | Uint8Array<ArrayBuffer> | ReadableStream<Uint8Array<ArrayBuffer>>): Promise<void>;
    clear(): Promise<void>;
    destroy(): Promise<void>;
}
