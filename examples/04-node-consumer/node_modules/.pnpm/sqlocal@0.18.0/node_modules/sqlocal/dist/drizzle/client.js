import { SQLocal } from '../index.js';
/**
 * A subclass of the `SQLocal` client that provides additional methods
 * for using SQLocal as a driver for Drizzle ORM.
 * @see {@link https://sqlocal.dev/drizzle/setup}
 */
export class SQLocalDrizzle extends SQLocal {
    constructor() {
        super(...arguments);
        /**
         * A driver function that Drizzle can use to query
         * databases through SQLocal.
         * @see {@link https://sqlocal.dev/drizzle/setup}
         */
        Object.defineProperty(this, "driver", {
            enumerable: true,
            configurable: true,
            writable: true,
            value: async (sql, params, method) => {
                if (/^begin\b/i.test(sql) &&
                    typeof globalThis.sessionStorage !== 'undefined' &&
                    !sessionStorage._sqlocal_sent_drizzle_transaction_warning) {
                    console.warn("Drizzle's transaction method cannot isolate transactions from outside queries. It is recommended to use the transaction method of SQLocalDrizzle instead (See https://sqlocal.dev/api/transaction#drizzle).");
                    sessionStorage._sqlocal_sent_drizzle_transaction_warning = '1';
                }
                const transactionKey = this.transactionQueryKeyQueue.shift();
                return this.exec(sql, params, method, transactionKey);
            }
        });
        /**
         * A driver function that Drizzle can use to make
         * batch queries to databases through SQLocal.
         * @see {@link https://sqlocal.dev/drizzle/setup}
         */
        Object.defineProperty(this, "batchDriver", {
            enumerable: true,
            configurable: true,
            writable: true,
            value: async (queries) => {
                return this.execBatch(queries);
            }
        });
    }
}
//# sourceMappingURL=client.js.map