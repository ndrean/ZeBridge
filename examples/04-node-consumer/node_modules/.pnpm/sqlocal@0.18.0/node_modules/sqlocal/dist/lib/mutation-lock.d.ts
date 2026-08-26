export type MutationLockOptions = {
    mode: LockMode;
    key: string;
    bypass: boolean;
};
export declare function mutationLock<T>(options: MutationLockOptions, mutation: () => Promise<T>): Promise<T>;
