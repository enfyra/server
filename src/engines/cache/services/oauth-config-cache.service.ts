import { EventEmitter2 } from 'eventemitter2';
import { QueryBuilderService } from '@enfyra/kernel';
import { BaseCacheService, CacheConfig } from './base-cache.service';
import { RedisRuntimeCacheStore } from './redis-runtime-cache-store.service';
import { CACHE_IDENTIFIERS } from '../../../shared/utils/cache-events.constants';
import { IOAuthConfigCache } from '../../../domain/shared/interfaces/oauth-config-cache.interface';
import { normalizeScriptRecord } from '../../../shared/utils/script-code.util';
import { RuntimeRegistryService } from './runtime-registry.service';

const OAUTH_CONFIG: CacheConfig = {
  cacheIdentifier: CACHE_IDENTIFIERS.OAUTH_CONFIG,
  colorCode: '\x1b[33m',
  cacheName: 'OAuthConfigCache',
};

export interface OAuthConfig {
  id: number;
  provider: 'google' | 'facebook' | 'github';
  clientId: string;
  clientSecret: string;
  redirectUri: string;
  appCallbackUrl?: string | null;
  autoSetCookies: boolean;
  sourceCode?: string | null;
  scriptLanguage?: string | null;
  compiledCode?: string | null;
  isEnabled: boolean;
  description?: string;
}

export class OAuthConfigCacheService
  extends BaseCacheService<Map<string, OAuthConfig>>
  implements IOAuthConfigCache
{
  private readonly queryBuilderService: QueryBuilderService;
  private readonly runtimeRegistryService?: RuntimeRegistryService;

  constructor(deps: {
    queryBuilderService: QueryBuilderService;
    eventEmitter?: EventEmitter2;
    runtimeRegistryService?: RuntimeRegistryService;
    redisRuntimeCacheStore?: RedisRuntimeCacheStore;
  }) {
    super(OAUTH_CONFIG, deps.eventEmitter, deps.redisRuntimeCacheStore);
    this.queryBuilderService = deps.queryBuilderService;
    this.runtimeRegistryService = deps.runtimeRegistryService;
  }

  protected async loadFromDb(): Promise<OAuthConfig[]> {
    const result = await this.queryBuilderService.find({
      table: 'enfyra_oauth_config',
      filter: { isEnabled: { _eq: true } },
    });
    if (!result.data || result.data.length === 0) {
      return [];
    }
    return result.data.map((config: any) => {
      const normalized = normalizeScriptRecord('enfyra_oauth_config', config);
      return {
        id: normalized.id,
        provider: normalized.provider,
        clientId: normalized.clientId,
        clientSecret: normalized.clientSecret,
        redirectUri: normalized.redirectUri,
        appCallbackUrl: normalized.appCallbackUrl ?? null,
        autoSetCookies: normalized.autoSetCookies === true,
        sourceCode: normalized.sourceCode ?? null,
        scriptLanguage: normalized.scriptLanguage ?? 'typescript',
        compiledCode: normalized.compiledCode ?? null,
        isEnabled: normalized.isEnabled !== false,
        description: normalized.description,
      };
    });
  }

  protected transformData(configs: OAuthConfig[]): Map<string, OAuthConfig> {
    const map = new Map<string, OAuthConfig>();
    for (const config of configs) {
      map.set(config.provider, config);
    }
    return map;
  }

  protected getLogCount(): string {
    return `${this.cache.size} OAuth configs`;
  }

  async getConfigByProvider(provider: string): Promise<OAuthConfig | null> {
    const cache = await this.getActiveOauthConfigs();
    return cache.get(provider) || null;
  }

  async getDirectConfigByProvider(
    provider: string,
  ): Promise<OAuthConfig | null> {
    return this.getConfigByProvider(provider);
  }

  async getAllProviders(): Promise<string[]> {
    const cache = await this.getActiveOauthConfigs();
    return Array.from(cache.keys());
  }

  private async getActiveOauthConfigs(): Promise<Map<string, OAuthConfig>> {
    return this.requireRuntimeRegistry().requireActiveData<
      Map<string, OAuthConfig>
    >(CACHE_IDENTIFIERS.OAUTH_CONFIG);
  }

  private requireRuntimeRegistry(): RuntimeRegistryService {
    if (!this.runtimeRegistryService) {
      throw new Error('Runtime registry service is required for OAuth reads');
    }
    return this.runtimeRegistryService;
  }
}
