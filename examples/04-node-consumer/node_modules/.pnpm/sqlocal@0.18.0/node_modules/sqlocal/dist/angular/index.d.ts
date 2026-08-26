import { type Signal } from '@angular/core';
import type { SQLocal } from '../client.js';
import type { ReactiveQueryStatus, StatementInput } from '../types.js';
/**
 * A signal function for using reactive SQL queries in Angular components.
 * @see {@link https://sqlocal.dev/api/reactivequery#angular}
 */
export declare function useReactiveQuery<Result extends Record<string, any>>(db: SQLocal | Signal<SQLocal>, query: StatementInput<Result> | Signal<StatementInput<Result>>): {
    data: Signal<Result[]>;
    error: Signal<Error | undefined>;
    status: Signal<ReactiveQueryStatus>;
};
