import type { Dialect } from 'kysely';
import { SQLocal } from '../index.js';
/**
 * A subclass of the `SQLocal` client that provides an additional property
 * for using SQLocal as a dialect for the Kysely query builder.
 * @see {@link https://sqlocal.dev/kysely/setup}
 */
export declare class SQLocalKysely extends SQLocal {
    /**
     * A Kysely dialect that implements the interface needed for
     * Kysely to interact with databases through SQLocal.
     * @see {@link https://sqlocal.dev/kysely/setup}
     */
    dialect: Dialect;
}
