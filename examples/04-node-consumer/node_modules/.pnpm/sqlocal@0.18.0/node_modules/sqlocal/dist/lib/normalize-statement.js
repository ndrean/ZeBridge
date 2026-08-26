import { sqlTag } from './sql-tag.js';
function isDrizzleStatement(statement) {
    return (typeof statement === 'object' &&
        statement !== null &&
        'getSQL' in statement &&
        typeof statement.getSQL === 'function');
}
function isStatement(statement) {
    return (typeof statement === 'object' &&
        statement !== null &&
        'sql' in statement === true &&
        typeof statement.sql === 'string' &&
        'params' in statement === true);
}
export function normalizeStatement(statement) {
    if (typeof statement === 'function') {
        statement = statement(sqlTag);
    }
    if (isDrizzleStatement(statement)) {
        try {
            if (!('toSQL' in statement && typeof statement.toSQL === 'function')) {
                throw 1;
            }
            const drizzleStatement = statement.toSQL();
            if (!isStatement(drizzleStatement)) {
                throw 2;
            }
            const exec = 'all' in statement && typeof statement.all === 'function'
                ? statement.all
                : undefined;
            return {
                ...drizzleStatement,
                exec: exec ? () => exec() : undefined,
            };
        }
        catch {
            throw new Error('The passed statement could not be parsed.');
        }
    }
    const sql = statement.sql;
    let params = [];
    if ('params' in statement) {
        params = statement.params;
    }
    else if ('parameters' in statement) {
        params = statement.parameters;
    }
    return { sql, params };
}
//# sourceMappingURL=normalize-statement.js.map