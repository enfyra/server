export interface CacheKeyOptions {
  /** Coordinates across every replica in the current NODE_NAME app namespace. */
  global?: boolean;
}

export interface ICache {
  get<T = any>(key: string, options?: CacheKeyOptions): Promise<T | null>;
  set<T = any>(key: string, value: T, ttlMs: number): Promise<void>;
  deleteKey(key: string): Promise<void>;
  clearAll(): Promise<void>;
  acquire(
    key: string,
    value: any,
    ttlMs: number,
    options?: CacheKeyOptions,
  ): Promise<boolean>;
  renew(
    key: string,
    value: any,
    ttlMs: number,
    options?: CacheKeyOptions,
  ): Promise<boolean>;
  release(
    key: string,
    value: any,
    options?: CacheKeyOptions,
  ): Promise<boolean>;
}
