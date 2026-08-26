import { SQLiteMemoryDriver } from './sqlite-memory-driver.js';
/**
 * A SQLocal driver that implements the interface needed for
 * interacting with SQLite databases in localStorage or sessionStorage.
 */
export class SQLiteKvvfsDriver extends SQLiteMemoryDriver {
    constructor(storageType, sqlite3InitModule) {
        super(sqlite3InitModule);
        Object.defineProperty(this, "storageType", {
            enumerable: true,
            configurable: true,
            writable: true,
            value: storageType
        });
    }
    async init(config) {
        const flags = this.getFlags(config);
        if (config.readOnly) {
            throw new Error(`SQLite storage type "${this.storageType}" does not support read-only mode.`);
        }
        if (!this.sqlite3InitModule) {
            const { default: sqlite3InitModule } = await import('@sqlite.org/sqlite-wasm');
            this.sqlite3InitModule = sqlite3InitModule;
        }
        if (!this.sqlite3) {
            this.sqlite3 = await this.sqlite3InitModule();
        }
        if (this.db) {
            await this.destroy();
        }
        this.db = new this.sqlite3.oo1.JsStorageDb({
            filename: this.storageType,
            flags,
        });
        this.config = config;
        this.initWriteHook();
    }
    async isDatabasePersisted() {
        return navigator.storage?.persisted();
    }
    async getDatabaseSizeBytes() {
        if (!this.db)
            throw new Error('Driver not initialized');
        return this.db.storageSize();
    }
    async import(database) {
        const memdb = new SQLiteMemoryDriver();
        await memdb.init({});
        await memdb.import(database);
        await this.clear();
        await memdb.exec({
            sql: `VACUUM INTO 'file:${this.storageType}?vfs=kvvfs'`,
        });
        await memdb.destroy();
    }
    async clear() {
        if (!this.db)
            throw new Error('Driver not initialized');
        this.db.clearStorage();
    }
    async destroy() {
        this.closeDb();
        this.writeCallbacks.clear();
    }
}
//# sourceMappingURL=sqlite-kvvfs-driver.js.map