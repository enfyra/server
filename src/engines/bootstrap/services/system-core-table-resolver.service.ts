import { QueryBuilderService } from '@enfyra/kernel';
import {
  CORE_SYSTEM_TABLES,
  LEGACY_CORE_SYSTEM_TABLES,
} from '../../../shared/utils/system-tables.constants';
import type {
  CoreSystemTableKey,
  CoreSystemTableNames,
} from '../../../shared/types/system-tables.types';

export class SystemCoreTableResolver {
  private readonly queryBuilderService: QueryBuilderService;

  constructor(deps: { queryBuilderService: QueryBuilderService }) {
    this.queryBuilderService = deps.queryBuilderService;
  }

  async getNames(): Promise<CoreSystemTableNames> {
    return {
      table: await this.resolveOne('table'),
      column: await this.resolveOne('column'),
      relation: await this.resolveOne('relation'),
    };
  }

  async getTableName(key: CoreSystemTableKey): Promise<string> {
    return this.resolveOne(key);
  }

  private async resolveOne(key: CoreSystemTableKey): Promise<string> {
    const canonical = CORE_SYSTEM_TABLES[key];
    const legacy = LEGACY_CORE_SYSTEM_TABLES[key];

    if (await this.physicalTableExists(canonical)) return canonical;
    if (await this.physicalTableExists(legacy)) return legacy;
    return canonical;
  }

  async physicalTableExists(tableName: string): Promise<boolean> {
    if (this.queryBuilderService.isMongoDb()) {
      const db = this.queryBuilderService.getMongoDb();
      const matches = await db.listCollections({ name: tableName }).toArray();
      return matches.length > 0;
    }

    const knex = this.queryBuilderService.getKnex();
    return knex.schema.hasTable(tableName);
  }
}
