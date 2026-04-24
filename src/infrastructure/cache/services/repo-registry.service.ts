import { DynamicRepository } from '../../../modules/dynamic-api/repositories/dynamic.repository';
import { DynamicRepositoryFactory } from '../../../modules/dynamic-api/repositories/dynamic-repository.factory';
import { MetadataCacheService } from './metadata-cache.service';
import { TDynamicContext } from '../../../shared/types';
import { CACHE_EVENTS } from '../../../shared/utils/cache-events.constants';
import { EventEmitter2 } from 'eventemitter2';

export class RepoRegistryService {
  private readonly metadataCacheService: MetadataCacheService;
  private readonly factory: DynamicRepositoryFactory;
  private aliasToName = new Map<string, string>();
  private initialized = false;

  constructor(deps: {
    metadataCacheService: MetadataCacheService;
    dynamicRepositoryFactory: DynamicRepositoryFactory;
    eventEmitter: EventEmitter2;
  }) {
    this.metadataCacheService = deps.metadataCacheService;
    this.factory = deps.dynamicRepositoryFactory;
    deps.eventEmitter.on(
      CACHE_EVENTS.INVALIDATE,
      this.handleCacheInvalidation.bind(this),
    );
  }

  async rebuildFromMetadata(
    metadataCache?: MetadataCacheService,
  ): Promise<void> {
    const cache = metadataCache || this.metadataCacheService;
    const tables = await cache.getAllTablesMetadata();
    const newMap = new Map<string, string>();
    for (const table of tables) {
      newMap.set(table.name, table.name);
      if (table.alias && table.alias !== table.name) {
        newMap.set(table.alias, table.name);
      }
    }
    this.aliasToName = newMap;
    this.initialized = true;
  }

  private ensureInitialized() {
    if (this.initialized) return;
    if (!this.metadataCacheService.isLoaded()) return;
    const metadata = this.metadataCacheService.getDirectMetadata();
    if (!metadata?.tablesList) return;
    const newMap = new Map<string, string>();
    for (const table of metadata.tablesList) {
      newMap.set(table.name, table.name);
      if (table.alias && table.alias !== table.name) {
        newMap.set(table.alias, table.name);
      }
    }
    this.aliasToName = newMap;
    this.initialized = true;
  }

  private handleCacheInvalidation() {
    this.initialized = false;
  }

  resolveTableName(nameOrAlias: string): string | null {
    this.ensureInitialized();
    return this.aliasToName.get(nameOrAlias) ?? null;
  }

  createReposProxy(
    context: TDynamicContext,
    mainTableName?: string,
  ): Record<string, any> {
    const repoCache = new Map<string, DynamicRepository>();
    const self = this;

    const getOrCreateRepo = (
      tableName: string,
      enforceFieldPermission: boolean,
    ): DynamicRepository | undefined => {
      const cacheKey = `${tableName}|${enforceFieldPermission ? '1' : '0'}`;
      if (repoCache.has(cacheKey)) return repoCache.get(cacheKey);

      const resolvedName = self.resolveTableName(tableName);
      if (!resolvedName) return undefined;

      const resolvedKey = `${resolvedName}|${enforceFieldPermission ? '1' : '0'}`;
      if (repoCache.has(resolvedKey)) {
        const existing = repoCache.get(resolvedKey);
        repoCache.set(cacheKey, existing);
        return existing;
      }

      const repo = self.factory.create(
        resolvedName,
        context,
        enforceFieldPermission,
      );

      repoCache.set(resolvedKey, repo);
      repoCache.set(cacheKey, repo);
      return repo;
    };

    return new Proxy({} as Record<string, any>, {
      get(_target, prop: string) {
        if (prop === 'main' && mainTableName) {
          return getOrCreateRepo(mainTableName, true);
        }
        if (prop === 'secure') {
          return new Proxy({} as Record<string, any>, {
            get(_t, p: string) {
              if (typeof p === 'symbol') return undefined;
              return getOrCreateRepo(p, true);
            },
            has(_t, p: string) {
              if (typeof p === 'symbol') return false;
              return self.resolveTableName(p) !== null;
            },
          });
        }
        if (typeof prop === 'symbol') return undefined;
        return getOrCreateRepo(prop, false);
      },
      has(_target, prop: string) {
        if (prop === 'main' && mainTableName) return true;
        if (prop === 'secure') return true;
        if (typeof prop === 'symbol') return false;
        return self.resolveTableName(prop) !== null;
      },
    });
  }
}
