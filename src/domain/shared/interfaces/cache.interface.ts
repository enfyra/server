export interface ICache {
  get<T = any>(key: string): Promise<T | null>;
  set<T = any>(key: string, value: T, ttlMs: number): Promise<void>;
  deleteKey(key: string): Promise<void>;
  clearAll(): Promise<void>;
  acquire(key: string, value: any, ttlMs: number): Promise<boolean>;
  release(key: string, value: any): Promise<boolean>;
}
