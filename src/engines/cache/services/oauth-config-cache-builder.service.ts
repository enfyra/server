import { EventEmitter2 } from 'eventemitter2';
import { QueryBuilderService } from '@enfyra/kernel';
import { BaseCacheService, CacheConfig } from './base-cache.service';
import { RedisRuntimeCacheStore } from './redis-runtime-cache-store.service';
import { CACHE_IDENTIFIERS } from '../../../shared/utils/cache-events.constants';
import { normalizeScriptRecord } from '../../../shared/utils/script-code.util';
import type { TCacheInvalidationPayload } from '../../../shared/types/cache.types';

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

export class OAuthConfigCacheBuilder extends BaseCacheService<
  Map<string, OAuthConfig>
> {
  private readonly queryBuilderService: QueryBuilderService;

  constructor(deps: {
    queryBuilderService: QueryBuilderService;
    eventEmitter?: EventEmitter2;
    redisRuntimeCacheStore?: RedisRuntimeCacheStore;
  }) {
    super(OAUTH_CONFIG, deps.eventEmitter, deps.redisRuntimeCacheStore);
    this.queryBuilderService = deps.queryBuilderService;
  }

  override supportsPartialReload(): boolean {
    return true;
  }

  protected override async applyPartialUpdate(
    payload: TCacheInvalidationPayload,
  ): Promise<void> {
    if (payload.table !== 'enfyra_oauth_config') {
      await this.reloadFromPartial();
      return;
    }
    const ids = payload.ids ?? [];
    if (!ids.length) return;
    const updates = await this.loadConfigs(ids);
    const changed = new Set(ids.map(String));
    const next = new Map(
      [...this.cache].filter(([, config]) => !changed.has(String(config.id))),
    );
    for (const config of updates) next.set(config.provider, config);
    this.cache = next;
  }

  protected async loadFromDb(): Promise<OAuthConfig[]> {
    return this.loadConfigs();
  }

  private async loadConfigs(ids?: (string | number)[]): Promise<OAuthConfig[]> {
    const result = await this.queryBuilderService.find({
      table: 'enfyra_oauth_config',
      filter: ids
        ? { _and: [{ isEnabled: { _eq: true } }, { id: { _in: ids } }] }
        : { isEnabled: { _eq: true } },
      limit: -1,
    });
    if (!result.data || result.data.length === 0) {
      return [];
    }
    return result.data.map((config: any) => {
      const normalized = normalizeScriptRecord('enfyra_oauth_config', config);
      return {
        id: normalized._id ?? normalized.id,
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

  protected override getLogCount(): string {
    return `${this.cache.size} OAuth configs`;
  }
}
