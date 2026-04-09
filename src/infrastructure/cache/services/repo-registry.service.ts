import { Injectable, Logger } from '@nestjs/common';
import { OnEvent, EventEmitter2 } from '@nestjs/event-emitter';
import { QueryBuilderService } from '../../query-builder/query-builder.service';
import { QueryEngine } from '../../query-engine/services/query-engine.service';
import { MetadataCacheService } from './metadata-cache.service';
import { TableHandlerService } from '../../../modules/table-management/services/table-handler.service';
import { PolicyService } from '../../../core/policy/policy.service';
import { TableValidationService } from '../../../modules/dynamic-api/services/table-validation.service';
import { DynamicRepository } from '../../../modules/dynamic-api/repositories/dynamic.repository';
import { SettingCacheService } from './setting-cache.service';
import { TDynamicContext } from '../../../shared/types';
import { CACHE_EVENTS } from '../../../shared/utils/cache-events.constants';

@Injectable()
export class RepoRegistryService {
  private readonly logger = new Logger(RepoRegistryService.name);
  private aliasToName = new Map<string, string>();
  private initialized = false;

  constructor(
    private readonly queryBuilder: QueryBuilderService,
    private readonly queryEngine: QueryEngine,
    private readonly metadataCacheService: MetadataCacheService,
    private readonly tableHandlerService: TableHandlerService,
    private readonly policyService: PolicyService,
    private readonly tableValidationService: TableValidationService,
    private readonly settingCacheService: SettingCacheService,
    private readonly eventEmitter: EventEmitter2,
  ) {}

  @OnEvent(CACHE_EVENTS.METADATA_LOADED)
  async onMetadataLoaded() {
    const tables = await this.metadataCacheService.getAllTablesMetadata();
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
    ): DynamicRepository | undefined => {
      if (repoCache.has(tableName)) return repoCache.get(tableName);

      const resolvedName = self.resolveTableName(tableName);
      if (!resolvedName) return undefined;

      if (repoCache.has(resolvedName)) {
        const existing = repoCache.get(resolvedName);
        repoCache.set(tableName, existing);
        return existing;
      }

      const repo = new DynamicRepository({
        context,
        tableName: resolvedName,
        tableHandlerService: self.tableHandlerService,
        queryBuilder: self.queryBuilder,
        queryEngine: self.queryEngine,
        metadataCacheService: self.metadataCacheService,
        policyService: self.policyService,
        tableValidationService: self.tableValidationService,
        settingCacheService: self.settingCacheService,
        eventEmitter: self.eventEmitter,
      });

      repoCache.set(resolvedName, repo);
      if (tableName !== resolvedName) {
        repoCache.set(tableName, repo);
      }
      return repo;
    };

    return new Proxy({} as Record<string, any>, {
      get(_target, prop: string) {
        if (prop === 'main' && mainTableName) {
          return getOrCreateRepo(mainTableName);
        }
        if (typeof prop === 'symbol') return undefined;
        return getOrCreateRepo(prop);
      },
      has(_target, prop: string) {
        if (prop === 'main' && mainTableName) return true;
        if (typeof prop === 'symbol') return false;
        return self.resolveTableName(prop) !== null;
      },
    });
  }
}
