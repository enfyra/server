export interface PackageRuntimeStatus {
  initialized: boolean;
  systemReady: boolean;
  preloadRunning: boolean;
  preloadRequested: boolean;
  lastPreload?: {
    status: 'running' | 'ok' | 'degraded';
    startedAt: string;
    completedAt?: string;
    loaded?: number;
    failed?: number;
    error?: string;
  };
}

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
