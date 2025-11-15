import { Logger } from '@nestjs/common';
import { MetadataCacheService } from '../../../../infrastructure/cache/services/metadata-cache.service';

const logger = new Logger('ListTablesExecutor');

export interface ListTablesExecutorDependencies {
  metadataCacheService: MetadataCacheService;
}

export async function executeListTables(deps: ListTablesExecutorDependencies): Promise<any> {
  logger.debug(`[list_tables] Called`);
  const { metadataCacheService } = deps;
  const metadata = await metadataCacheService.getMetadata();
  const tablesList = Array.from(metadata.tables.entries()).map(([name, table]) => ({
    name,
    description: table.description || '',
  }));

  logger.debug(`[list_tables] Returning ${tablesList.length} tables`);
  return {
    totalCount: tablesList.length,
    tables: tablesList,
  };
}

