import type { EventEmitter2 } from 'eventemitter2';
import type { QueryBuilderService } from '@enfyra/kernel';
import { BaseCacheService } from './base-cache.service';
import type { RedisRuntimeCacheStore } from './redis-runtime-cache-store.service';
import type { SharedHelperDefinition } from '../../../shared/types/shared-helper.types';
import { CACHE_IDENTIFIERS } from '../../../shared/utils/cache-events.constants';
import { resolveExecutableScript } from '../../../shared/utils/script-code.util';
import { assertSharedHelperName } from '../../../shared/utils/shared-helper-name.util';
import { DatabaseConfigService } from '../../../shared/services/database-config.service';

export class SharedHelperCacheService extends BaseCacheService<
  SharedHelperDefinition[]
> {
  private readonly queryBuilderService: QueryBuilderService;

  constructor(deps: {
    queryBuilderService: QueryBuilderService;
    eventEmitter: EventEmitter2;
    redisRuntimeCacheStore?: RedisRuntimeCacheStore;
  }) {
    super(
      {
        cacheIdentifier: CACHE_IDENTIFIERS.HELPER,
        colorCode: '\x1b[35m',
        cacheName: 'SharedHelperCache',
      },
      deps.eventEmitter,
      deps.redisRuntimeCacheStore,
    );
    this.queryBuilderService = deps.queryBuilderService;
  }

  protected async loadFromDb(): Promise<SharedHelperDefinition[]> {
    const records: SharedHelperDefinition[] = [];
    const pageSize = 1000;
    for (let page = 1; ; page++) {
      const result = await this.queryBuilderService.find({
        table: 'enfyra_helper',
        fields: [
          DatabaseConfigService.getPkField(),
          'name',
          'isEnabled',
          'sourceCode',
          'scriptLanguage',
          'compiledCode',
        ],
        sort: [DatabaseConfigService.getPkField()],
        limit: pageSize,
        page,
      });
      records.push(...result.data);
      if (result.data.length < pageSize) break;
    }
    const names = new Set<string>();
    return records.map((record: SharedHelperDefinition) => {
      assertSharedHelperName(record.name);
      if (names.has(record.name))
        throw new Error(`Duplicate helper name: ${record.name}`);
      names.add(record.name);
      const enabled = record.isEnabled === true;
      const code = enabled ? resolveExecutableScript(record).code : '';
      if (enabled && !code)
        throw new Error(
          `Enabled helper has no executable source: ${record.name}`,
        );
      const id = DatabaseConfigService.getRecordId(record);
      return {
        id: id == null ? undefined : String(id),
        name: record.name,
        isEnabled: enabled,
        code: code ?? '',
        sourceCode: record.sourceCode,
        scriptLanguage: record.scriptLanguage,
      };
    });
  }

  protected transformData(
    data: SharedHelperDefinition[],
  ): SharedHelperDefinition[] {
    return data;
  }

  protected getLogCount(): string {
    return `${this.cache.length} helpers`;
  }
}
