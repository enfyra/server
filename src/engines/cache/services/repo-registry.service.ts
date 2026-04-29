import {
  DynamicRepository,
  DynamicRepositoryFactory,
} from '../../../modules/dynamic-api';
import { MetadataCacheService } from './metadata-cache.service';
import { TDynamicContext } from '../../../shared/types';
import { CACHE_EVENTS } from '../../../shared/utils/cache-events.constants';
import { IRepoRegistry } from '../../../domain/shared/interfaces/repo-registry.interface';
import { EventEmitter2 } from 'eventemitter2';

export class RepoRegistryService implements IRepoRegistry {
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

  private ensureInitialized() {}

  private handleCacheInvalidation() {}

  resolveTableName(nameOrAlias: string): string | null {
    this.ensureInitialized();
    return this.aliasToName.get(nameOrAlias) ?? null;
  }

  createReposProxy(
    context: TDynamicContext,
    mainTableName?: string,
  ): Record<string, any> {
    const resolve = (name: string) => this.resolveTableName(name);
    const create = (resolved: string, enforce: boolean) =>
      this.factory.create(resolved, context, enforce);

    const buildProxy = (enforce: boolean) =>
      new Proxy({} as Record<string, DynamicRepository>, {
        get(target, prop) {
          if (typeof prop === 'symbol') return undefined;
          const resolved = resolve(prop as string);
          if (!resolved) return undefined;
          if (target[resolved]) return target[resolved];
          target[resolved] = create(resolved, enforce);
          return target[resolved];
        },
        has(_t, p) {
          if (typeof p === 'symbol') return false;
          return resolve(p as string) !== null;
        },
      });

    const secureProxy = buildProxy(true);
    const trustedProxy = buildProxy(false);

    return new Proxy({} as Record<string, any>, {
      get(_target, prop) {
        if (prop === 'main' && mainTableName) return secureProxy[mainTableName];
        if (prop === 'secure') return secureProxy;
        if (typeof prop === 'symbol') return undefined;
        return trustedProxy[prop as string];
      },
      has(_target, prop) {
        if (prop === 'main' && mainTableName) return true;
        if (prop === 'secure') return true;
        if (typeof prop === 'symbol') return false;
        return resolve(prop as string) !== null;
      },
    });
  }
}
