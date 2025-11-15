import { MetadataCacheService } from '../../../../infrastructure/cache/services/metadata-cache.service';

export interface ListTablesExecutorDependencies {
  metadataCacheService: MetadataCacheService;
}

export async function executeListTables(deps: ListTablesExecutorDependencies): Promise<any> {
  const { metadataCacheService } = deps;
  const metadata = await metadataCacheService.getMetadata();
  const tablesList = Array.from(metadata.tables.entries()).map(([name, table]) => ({
    name,
    description: table.description || '',
  }));

  return {
    totalCount: tablesList.length,
    tables: tablesList,
  };
}

