export interface IMetadataCache {
  getMetadata(): Promise<{ tables: Map<string, any> }>;
}
