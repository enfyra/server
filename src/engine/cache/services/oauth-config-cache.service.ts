import { EventEmitter2 } from 'eventemitter2';
import { QueryBuilderService } from '../../../kernel/query';
import { BaseCacheService, CacheConfig } from './base-cache.service';
import { CACHE_IDENTIFIERS } from '../../../shared/utils/cache-events.constants';
import { IOAuthConfigCache } from '../../../domain/shared/interfaces/oauth-config-cache.interface';
import { normalizeScriptRecord } from '../../../kernel/execution';

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

  constructor(deps: {
    queryBuilderService: QueryBuilderService;
    eventEmitter?: EventEmitter2;
  }) {
    super(OAUTH_CONFIG, deps.eventEmitter);
    this.queryBuilderService = deps.queryBuilderService;
  }

  protected async loadFromDb(): Promise<OAuthConfig[]> {
    const result = await this.queryBuilderService.find({
      table: 'oauth_config_definition',
      filter: { isEnabled: { _eq: true } },
    });
    if (!result.data || result.data.length === 0) {
      return [];
    }
    return result.data.map((config: any) => {
      const normalized = normalizeScriptRecord('oauth_config_definition', config);
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
    await this.ensureLoaded();
    return this.cache.get(provider) || null;
  }

  getDirectConfigByProvider(provider: string): OAuthConfig | null {
    return this.cache.get(provider) || null;
  }

  getAllProviders(): string[] {
    return Array.from(this.cache.keys());
  }
}
