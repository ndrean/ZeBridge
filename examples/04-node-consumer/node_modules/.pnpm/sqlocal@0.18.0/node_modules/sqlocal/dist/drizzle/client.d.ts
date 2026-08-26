import { SQLocal } from '../index.js';
import type { RawResultData, Sqlite3Method } from '../types.js';
/**
 * A subclass of the `SQLocal` client that provides additional methods
 * for using SQLocal as a driver for Drizzle ORM.
 * @see {@link https://sqlocal.dev/drizzle/setup}
 */
export declare class SQLocalDrizzle extends SQLocal {
    /**
     * A driver function that Drizzle can use to query
     * databases through SQLocal.
     * @see {@link https://sqlocal.dev/drizzle/setup}
     */
    driver: (sql: string, params: unknown[], method: Sqlite3Method) => Promise<RawResultData>;
    /**
     * A driver function that Drizzle can use to make
     * batch queries to databases through SQLocal.
     * @see {@link https://sqlocal.dev/drizzle/setup}
     */
    batchDriver: (queries: {
        sql: string;
        params: unknown[];
        method: Sqlite3Method;
    }[]) => Promise<RawResultData[]>;
}
