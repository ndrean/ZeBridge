import { computed, effect, isSignal, signal } from '@angular/core';
/**
 * A signal function for using reactive SQL queries in Angular components.
 * @see {@link https://sqlocal.dev/api/reactivequery#angular}
 */
export function useReactiveQuery(db, query) {
    const data = signal([]);
    const error = signal(undefined);
    const pending = signal(true);
    const dbValue = computed(() => (isSignal(db) ? db() : db));
    const queryValue = computed(() => (isSignal(query) ? query() : query));
    effect((onCleanup) => {
        const db = dbValue();
        const query = queryValue();
        pending.set(true);
        const subscription = db.reactiveQuery(query).subscribe((results) => {
            data.set(results);
            error.set(undefined);
            pending.set(false);
        }, (err) => {
            error.set(err);
        });
        onCleanup(() => {
            subscription.unsubscribe();
        });
    }, { allowSignalWrites: true });
    const status = computed(() => {
        const hasError = !!error();
        const isPending = pending();
        if (hasError)
            return 'error';
        if (isPending)
            return 'pending';
        return 'ok';
    });
    return {
        data: data.asReadonly(),
        error: error.asReadonly(),
        status,
    };
}
//# sourceMappingURL=index.js.map