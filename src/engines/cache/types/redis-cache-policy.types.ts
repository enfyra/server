export interface RedisCachePolicy {
  /** Key segment between NODE_NAME and logical key, e.g. 'user_cache:' */
  keyPrefix: string;
  /** When NODE_NAME is unset, fall back to the 'enfyra' namespace instead of no prefix */
  requireNamespace?: boolean;
  /** Enables size tracking + LRU eviction scoped to this prefix */
  quota?: {
    limitBytes: number;
    maxValueBytes: number;
  };
  /** clearAll scope */
  clearAllMode: 'namespace' | 'prefix';
}
