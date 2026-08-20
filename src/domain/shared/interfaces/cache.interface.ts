export interface CacheSetEntry {
  key: string;
  value: unknown;
  ttlMs: number;
}

export interface ICache {
  get<T = any>(key: string): Promise<T | null>;
  set<T = any>(key: string, value: T, ttlMs?: number): Promise<void>;
  setManyIfKeyAbsent(
    guardKey: string,
    entries: CacheSetEntry[],
  ): Promise<boolean>;
  setManyAndDelete(
    entries: CacheSetEntry[],
    keysToDelete: string[],
  ): Promise<void>;
  compareAndSet<T = any>(
    key: string,
    expectedValue: T,
    value: T,
    ttlMs: number,
  ): Promise<boolean>;
  exists(key: string, value: any): Promise<boolean>;
  deleteKey(key: string): Promise<void>;
  setNoExpire<T = any>(key: string, value: T): Promise<void>;
  clearAll(): Promise<void>;
  acquire(key: string, value: any, ttlMs: number): Promise<boolean>;
  renew(key: string, value: any, ttlMs: number): Promise<boolean>;
  release(key: string, value: any): Promise<boolean>;
}
