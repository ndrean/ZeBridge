import { type DeepReadonly, type Ref } from 'vue';
import type { SQLocal } from '../client.js';
import type { ReactiveQueryStatus, StatementInput } from '../types.js';
/**
 * A composable for using reactive SQL queries in Vue components.
 * @see {@link https://sqlocal.dev/api/reactivequery#vue}
 */
export declare function useReactiveQuery<Result extends Record<string, any>>(db: SQLocal | Ref<SQLocal>, query: StatementInput<Result> | Ref<StatementInput<Result>>): {
    data: Readonly<Ref<DeepReadonly<Result[]>>>;
    error: Readonly<Ref<Error | undefined>>;
    status: Readonly<Ref<ReactiveQueryStatus>>;
};
