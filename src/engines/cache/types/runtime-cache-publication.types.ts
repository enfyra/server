export interface RuntimeCachePublicationScope {
  token?: string;
  snapshots: Map<string, Buffer>;
  auxiliary: Map<string, Buffer>;
  deletedAuxiliary: Set<string>;
  committed?: boolean;
}
