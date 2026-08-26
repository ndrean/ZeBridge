import type { StatementInput, Statement } from '../types.js';
type NormalStatement = Statement & {
    exec?: <T extends Record<string, any>>() => Promise<T[]>;
};
export declare function normalizeStatement(statement: StatementInput): NormalStatement;
export {};
