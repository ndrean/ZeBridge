import { computed, isRef, readonly, shallowRef, watchEffect, } from 'vue';
/**
 * A composable for using reactive SQL queries in Vue components.
 * @see {@link https://sqlocal.dev/api/reactivequery#vue}
 */
export function useReactiveQuery(db, query) {
    const data = shallowRef([]);
    const error = shallowRef(undefined);
    const pending = shallowRef(true);
    const dbValue = computed(() => (isRef(db) ? db.value : db));
    const queryValue = computed(() => (isRef(query) ? query.value : query));
    watchEffect((onCleanup) => {
        const db = dbValue.value;
        const query = queryValue.value;
        pending.value = true;
        const subscription = db.reactiveQuery(query).subscribe((results) => {
            data.value = results;
            error.value = undefined;
            pending.value = false;
        }, (err) => {
            error.value = err;
        });
        onCleanup(() => {
            subscription.unsubscribe();
        });
    });
    const status = computed(() => {
        const hasError = !!error.value;
        const isPending = pending.value;
        if (hasError)
            return 'error';
        if (isPending)
            return 'pending';
        return 'ok';
    });
    return {
        data: readonly(data),
        error: readonly(error),
        status: readonly(status),
    };
}
//# sourceMappingURL=index.js.map