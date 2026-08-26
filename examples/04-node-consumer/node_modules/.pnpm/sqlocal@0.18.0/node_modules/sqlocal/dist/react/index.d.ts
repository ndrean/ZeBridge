import { type Dispatch, type SetStateAction } from 'react';
import type { SQLocal } from '../client.js';
import type { ReactiveQueryStatus, StatementInput } from '../types.js';
/**
 * A hook for using reactive SQL queries in React components.
 * @see {@link https://sqlocal.dev/api/reactivequery#react}
 */
export declare function useReactiveQuery<Result extends Record<string, any>>(db: SQLocal, query: StatementInput<Result>): {
    data: Result[];
    error: Error | undefined;
    status: ReactiveQueryStatus;
    setDb: Dispatch<SetStateAction<SQLocal>>;
    setQuery: Dispatch<SetStateAction<StatementInput<Result>>>;
};
