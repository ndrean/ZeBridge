import { CompiledQuery, SqliteAdapter, SqliteIntrospector, SqliteQueryCompiler, } from 'kysely';
import { SQLocal } from '../index.js';
import { convertRowsToObjects } from '../lib/convert-rows-to-objects.js';
import { sqlTag } from '../lib/sql-tag.js';
/**
 * A subclass of the `SQLocal` client that provides an additional property
 * for using SQLocal as a dialect for the Kysely query builder.
 * @see {@link https://sqlocal.dev/kysely/setup}
 */
export class SQLocalKysely extends SQLocal {
    constructor() {
        super(...arguments);
        /**
         * A Kysely dialect that implements the interface needed for
         * Kysely to interact with databases through SQLocal.
         * @see {@link https://sqlocal.dev/kysely/setup}
         */
        Object.defineProperty(this, "dialect", {
            enumerable: true,
            configurable: true,
            writable: true,
            value: {
                createAdapter: () => new SqliteAdapter(),
                createDriver: () => new SQLocalKyselyDriver(this),
                createIntrospector: (db) => new SqliteIntrospector(db),
                createQueryCompiler: () => new SqliteQueryCompiler(),
            }
        });
    }
}
class SQLocalKyselyDriver {
    constructor(client) {
        Object.defineProperty(this, "client", {
            enumerable: true,
            configurable: true,
            writable: true,
            value: client
        });
    }
    async init() { }
    async acquireConnection() {
        return new SQLocalKyselyConnection(this.client);
    }
    async releaseConnection() { }
    async beginTransaction(connection) {
        connection.transaction = await this.client.beginTransaction();
    }
    async commitTransaction(connection) {
        await connection.transaction?.commit();
        connection.transaction = null;
    }
    async rollbackTransaction(connection) {
        await connection.transaction?.rollback();
        connection.transaction = null;
    }
    async destroy() {
        await this.client.destroy();
    }
}
class SQLocalKyselyConnection {
    constructor(client) {
        Object.defineProperty(this, "client", {
            enumerable: true,
            configurable: true,
            writable: true,
            value: client
        });
        Object.defineProperty(this, "transaction", {
            enumerable: true,
            configurable: true,
            writable: true,
            value: null
        });
    }
    async executeQuery(query) {
        let rows;
        let affectedRows;
        if (this.transaction === null) {
            const statement = sqlTag(query.sql, query.parameters);
            const result = await this.client.exec(statement.sql, statement.params, 'all');
            rows = convertRowsToObjects(result.rows, result.columns);
            affectedRows = result.numAffectedRows;
        }
        else {
            rows = await this.transaction.query(query);
            affectedRows = this.transaction.lastAffectedRows;
        }
        return {
            rows: rows,
            numAffectedRows: affectedRows,
        };
    }
    async *streamQuery() {
        throw new Error('SQLite3 does not support streaming.');
    }
}
//# sourceMappingURL=client.js.map