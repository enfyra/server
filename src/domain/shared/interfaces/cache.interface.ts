export interface ICache {
  get<T = any>(key: string): Promise<T | null>;
  set<T = any>(key: string, value: T, ttlMs: number): Promise<void>;
  exists(key: string, value: any): Promise<boolean>;
  deleteKey(key: string): Promise<void>;
  setNoExpire<T = any>(key: string, value: T): Promise<void>;
  clearAll(): Promise<void>;
  acquire(key: string, value: any, ttlMs: number): Promise<boolean>;
  renew(key: string, value: any, ttlMs: number): Promise<boolean>;
  release(key: string, value: any): Promise<boolean>;
}
