export interface PackageRuntimeDescriptor {
  name: string;
  safeName: string;
  version?: string;
  filePath: string;
  fileUrl: string;
  size?: number;
  mtimeMs?: number;
  cacheKey?: string;
}
