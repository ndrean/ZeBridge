export function sqlTag(queryTemplate, ...params) {
    const normalizedSql = typeof queryTemplate === 'string' ? queryTemplate : queryTemplate.join('?');
    const normalizedParams = params.length === 1 && Array.isArray(params[0]) ? params[0] : params;
    return {
        sql: normalizedSql,
        params: normalizedParams,
    };
}
//# sourceMappingURL=sql-tag.js.map