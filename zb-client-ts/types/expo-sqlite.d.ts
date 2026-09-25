// The part of expo-sqlite (~15) that src/expo-storage.ts uses — for this library's own
// type-check only (tsconfig `paths`). An app type-checks against the real package.
export interface SQLiteDatabase {
  execAsync(source: string): Promise<void>;
  getAllAsync<T = any>(source: string, params?: any[]): Promise<T[]>;
  runAsync(source: string, params?: any[]): Promise<{ lastInsertRowId: number; changes: number }>;
  closeAsync(): Promise<void>;
}
export function openDatabaseAsync(databaseName: string): Promise<SQLiteDatabase>;
export function deleteDatabaseAsync(databaseName: string): Promise<void>;
