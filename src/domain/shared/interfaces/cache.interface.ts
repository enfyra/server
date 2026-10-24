export interface CacheSetEntry {
  key: string;
  value: unknown;
  ttlMs: number;
}

export interface ICache {
  get<T = unknown>(key: string): Promise<T | null>;
  set<T = unknown>(key: string, value: T, ttlMs?: number): Promise<void>;
  setManyIfKeyAbsent(
    guardKey: string,
    entries: CacheSetEntry[],
  ): Promise<boolean>;
  setManyAndDelete(
    entries: CacheSetEntry[],
    keysToDelete: string[],
  ): Promise<void>;
  compareAndSet<T = unknown>(
    key: string,
    expectedValue: T,
    value: T,
    ttlMs: number,
  ): Promise<boolean>;
  exists(key: string, value: unknown): Promise<boolean>;
  deleteKey(key: string): Promise<void>;
  /** Reads and removes a key in one Redis round-trip, so a one-time value cannot be read twice. */
  getAndDelete<T = unknown>(key: string): Promise<T | null>;
  setNoExpire<T = unknown>(key: string, value: T): Promise<void>;
  clearAll(): Promise<void>;
  acquire(key: string, value: unknown, ttlMs: number): Promise<boolean>;
  renew(key: string, value: unknown, ttlMs: number): Promise<boolean>;
  release(key: string, value: unknown): Promise<boolean>;
}
