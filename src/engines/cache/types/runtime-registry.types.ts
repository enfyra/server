import type { CACHE_IDENTIFIERS } from '../../../shared/utils/cache-events.constants';

export type RuntimeCacheIdentifier =
  (typeof CACHE_IDENTIFIERS)[keyof typeof CACHE_IDENTIFIERS];

export type RuntimeRegistryPublishStatus = 'building' | 'activated' | 'failed';

export interface RuntimeRegistryEntry<T = unknown> {
  identifier: RuntimeCacheIdentifier;
  version: number;
  status: RuntimeRegistryPublishStatus;
  activatedAt?: string;
  failedAt?: string;
  error?: string;
  data?: T;
}

export interface RuntimeRegistrySnapshot<T = unknown> {
  identifier: RuntimeCacheIdentifier;
  version: number;
  activatedAt: string;
  data: T;
}

export * from './cache-data.types';
