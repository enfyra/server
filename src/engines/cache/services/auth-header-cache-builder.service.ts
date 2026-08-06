import { EventEmitter2 } from 'eventemitter2';
import { QueryBuilderService } from '@enfyra/kernel';
import { BaseCacheService, type CacheConfig } from './base-cache.service';
import { RedisRuntimeCacheStore } from './redis-runtime-cache-store.service';
import { CACHE_IDENTIFIERS } from '../../../shared/utils/cache-events.constants';
import { SYSTEM_TABLES } from '../../../shared/utils/system-tables.constants';
import {
  SYSTEM_AUTH_HEADER_CONFIGS,
  type AuthHeaderConfig,
  type AuthHeaderCredentialType,
  type AuthHeaderScheme,
} from '../../../domain/auth/types/auth.types';

const AUTH_HEADER_CACHE_CONFIG: CacheConfig = {
  cacheIdentifier: CACHE_IDENTIFIERS.AUTH_HEADER,
  colorCode: '\x1b[35m',
  cacheName: 'AuthHeaderCache',
};

export class AuthHeaderCacheBuilder extends BaseCacheService<AuthHeaderConfig[]> {
  private readonly queryBuilderService: QueryBuilderService;

  constructor(deps: {
    queryBuilderService: QueryBuilderService;
    eventEmitter?: EventEmitter2;
    redisRuntimeCacheStore?: RedisRuntimeCacheStore;
  }) {
    super(
      AUTH_HEADER_CACHE_CONFIG,
      deps.eventEmitter,
      deps.redisRuntimeCacheStore,
    );
    this.queryBuilderService = deps.queryBuilderService;
  }

  protected async loadFromDb(): Promise<any[]> {
    try {
      const result = await this.queryBuilderService.find({
        table: SYSTEM_TABLES.authHeader,
        limit: 100,
      });
      return Array.isArray(result.data) ? result.data : [];
    } catch (error) {
      this.logger.warn(
        `Auth header table is unavailable; using built-in records: ${String(error)}`,
      );
      return [];
    }
  }

  protected transformData(rawData: any[]): AuthHeaderConfig[] {
    const dynamicConfigs = rawData
      .map((record) => this.normalizeRecord(record))
      .filter((record): record is AuthHeaderConfig => record !== null)
      .filter((record) => record.isEnabled);

    const persistedSystemConfigs = new Map(
      dynamicConfigs
        .filter((config) => config.isSystem)
        .map((config) => [this.keyFor(config), config]),
    );
    const configs = dynamicConfigs.filter(
      (config) =>
        !config.isSystem ||
        !SYSTEM_AUTH_HEADER_CONFIGS.some(
          (systemConfig) => this.keyFor(systemConfig) === this.keyFor(config),
        ),
    );

    for (const systemConfig of SYSTEM_AUTH_HEADER_CONFIGS) {
      const persisted = persistedSystemConfigs.get(this.keyFor(systemConfig));
      configs.push({
        ...systemConfig,
        id: persisted?.id ?? systemConfig.id,
        priority: persisted?.priority ?? systemConfig.priority,
        description: persisted?.description ?? systemConfig.description,
      });
    }

    return configs.sort(
      (left, right) =>
        left.priority - right.priority ||
        left.headerKey.localeCompare(right.headerKey) ||
        left.scheme.localeCompare(right.scheme) ||
        left.credentialType.localeCompare(right.credentialType) ||
        String(left.id).localeCompare(String(right.id)),
    );
  }

  protected getLogCount(): string {
    return `${this.cache.length} auth header configs`;
  }

  private normalizeRecord(record: any): AuthHeaderConfig | null {
    const headerKey =
      typeof record?.headerKey === 'string'
        ? record.headerKey.trim().toLowerCase()
        : '';
    const credentialType = record?.credentialType as AuthHeaderCredentialType;
    const scheme = record?.scheme as AuthHeaderScheme;
    if (
      !headerKey ||
      (credentialType !== 'pat' && credentialType !== 'jwt') ||
      (scheme !== 'raw' && scheme !== 'bearer')
    ) {
      return null;
    }

    return {
      id: record.id ?? record._id,
      headerKey,
      credentialType,
      scheme,
      priority: Number.isFinite(Number(record.priority))
        ? Number(record.priority)
        : 0,
      isEnabled: record.isEnabled !== false,
      isSystem: record.isSystem === true,
      description: record.description ?? null,
    };
  }

  private keyFor(
    config: Pick<AuthHeaderConfig, 'headerKey' | 'scheme' | 'credentialType'>,
  ): string {
    return `${config.headerKey.toLowerCase()}::${config.scheme}::${config.credentialType}`;
  }
}
