export interface RuntimeCachePublicationScope {
  active: boolean;
  token?: string;
  snapshots: Map<string, Buffer>;
  auxiliary: Map<string, Buffer>;
  deletedAuxiliary: Set<string>;
  committed?: boolean;
}

export interface RuntimeCacheCommittedReadScope {
  active: boolean;
  snapshots: Map<string, Buffer>;
}
