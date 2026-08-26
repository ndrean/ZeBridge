import { useCallback, useMemo, useState, useSyncExternalStore, } from 'react';
/**
 * A hook for using reactive SQL queries in React components.
 * @see {@link https://sqlocal.dev/api/reactivequery#react}
 */
export function useReactiveQuery(db, query) {
    const [error, setError] = useState(undefined);
    const [pending, setPending] = useState(true);
    const [dbValue, setDb] = useState(() => db);
    const [queryValue, setQuery] = useState(() => query);
    const reactiveQuery = useMemo(() => {
        setPending(true);
        return dbValue.reactiveQuery(queryValue);
    }, [dbValue, queryValue]);
    const get = useCallback(() => reactiveQuery.value, [reactiveQuery]);
    const subscribe = useCallback((cb) => {
        const subscription = reactiveQuery.subscribe(() => {
            cb();
            setError(undefined);
            setPending(false);
        }, (err) => {
            setError(err);
        });
        return () => {
            subscription.unsubscribe();
        };
    }, [reactiveQuery]);
    const data = useSyncExternalStore(subscribe, get);
    const status = !!error ? 'error' : pending ? 'pending' : 'ok';
    return {
        data,
        error,
        status,
        setDb,
        setQuery,
    };
}
//# sourceMappingURL=index.js.map