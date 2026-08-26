type DatabaseFileInput = File | Blob | ArrayBuffer | Uint8Array<ArrayBuffer> | ReadableStream<Uint8Array<ArrayBuffer>>;
export declare function normalizeDatabaseFile(dbFile: DatabaseFileInput, convertStreamTo: 'callback'): Promise<ArrayBuffer | Uint8Array<ArrayBuffer> | (() => Promise<Uint8Array<ArrayBuffer> | undefined>)>;
export declare function normalizeDatabaseFile(dbFile: DatabaseFileInput, convertStreamTo: 'buffer'): Promise<ArrayBuffer | Uint8Array<ArrayBuffer>>;
export declare function normalizeDatabaseFile(dbFile: DatabaseFileInput, convertStreamTo?: undefined): Promise<ArrayBuffer | Uint8Array<ArrayBuffer> | ReadableStream<Uint8Array<ArrayBuffer>>>;
export {};
