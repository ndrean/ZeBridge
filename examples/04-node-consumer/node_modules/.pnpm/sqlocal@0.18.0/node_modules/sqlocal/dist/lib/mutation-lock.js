export async function mutationLock(options, mutation) {
    if (!options.bypass && 'locks' in navigator) {
        return navigator.locks.request(`_sqlocal_mutation_(${options.key})`, { mode: options.mode }, mutation);
    }
    else {
        return mutation();
    }
}
//# sourceMappingURL=mutation-lock.js.map