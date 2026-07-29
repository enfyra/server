import { QueryBuilderService } from '@enfyra/kernel';
import type { Db } from 'mongodb';
import type { MetadataTableReference } from '../../types/metadata-migration.types';
import { SystemCoreTableResolver } from '../system-core-table-resolver.service';

export class MetadataTableLocator {
  private readonly queryBuilderService: QueryBuilderService;
  private readonly systemCoreTableResolver: SystemCoreTableResolver;

  constructor(deps: {
    queryBuilderService: QueryBuilderService;
    systemCoreTableResolver: SystemCoreTableResolver;
  }) {
    this.queryBuilderService = deps.queryBuilderService;
    this.systemCoreTableResolver = deps.systemCoreTableResolver;
  }

  private getMongoDb(): Db {
    return this.queryBuilderService.getMongoDb();
  }

  async findTable(
    tableName: string,
    isMongoDB: boolean,
  ): Promise<MetadataTableReference | null> {
    const coreNames = await this.systemCoreTableResolver.getNames();
    if (isMongoDB) {
      const table = await this.getMongoDb()
        .collection(coreNames.table)
        .findOne({ name: tableName });
      if (!table) return null;
      return { tableId: table._id, tableIdField: 'table' };
    }

    const table = await this.queryBuilderService
      .getKnex()(coreNames.table)
      .where('name', tableName)
      .first();
    if (!table) return null;
    return { tableId: table.id, tableIdField: 'tableId' };
  }
}
